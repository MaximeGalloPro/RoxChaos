from __future__ import annotations

import json
import os
import shutil
import tempfile
import hashlib
from pathlib import Path
from typing import Any

from .runner import Compose, parse_marked_json
from .settings import Settings


def capture_manifest(settings: Settings) -> dict[str, Any]:
    roxia_root = settings.workspace / "RoxIA"
    source_compose = Compose(
        roxia_root / "docker-compose.yml",
        "roxia",
        settings.command_environment(),
        project_directory=roxia_root,
    )
    script = settings.root / "scripts/export_manifest.rb"
    destination = "/tmp/roxchaos_export_manifest.rb"
    source_compose.copy_to(script, "web", destination)
    output = source_compose.exec(
        "web",
        "bin/rails",
        "runner",
        destination,
        env={
            "ROXCHAOS_ORGANISATION": settings.organisation,
            "ROXCHAOS_WORKFLOWS": json.dumps(settings.workflows),
        },
    )
    manifest = parse_marked_json(output)
    _capture_inputs(manifest, settings)

    _secure_write(
        settings.manifest_path,
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
    )
    return manifest


def load_manifest(settings: Settings) -> dict[str, Any]:
    if not settings.manifest_path.is_file():
        raise FileNotFoundError(
            f"Manifest not found: {settings.manifest_path}. Run 'roxchaos capture'."
        )
    return json.loads(settings.manifest_path.read_text(encoding="utf-8"))


def materialize_inputs(manifest: dict[str, Any], settings: Settings) -> list[Path]:
    settings.validate_destructive_scope()
    root = settings.runtime_documents.resolve()
    if root.exists():
        marker = root / ".roxchaos-owned"
        if not marker.is_file():
            raise ValueError(f"Refusing to remove unowned runtime directory: {root}")
        shutil.rmtree(root)
    root.mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    _secure_write(root / ".roxchaos-owned", "RoxChaos generated directory\n")

    written: list[Path] = []
    planned: dict[Path, str] = {}
    for workflow in manifest["workflows"]:
        captured_input = workflow.get("input")
        if not captured_input:
            raise ValueError(f"Workflow {workflow['key']} has no captured input")

        local_sources = _local_sources(workflow)
        paths = {source["attributes"]["local_path"] for source in local_sources}
        separators = {source["attributes"]["separator"] for source in local_sources}
        line_separators = {
            source["attributes"]["line_separator"] for source in local_sources
        }
        if len(paths) != 1 or len(separators) != 1 or len(line_separators) != 1:
            raise ValueError(
                f"Workflow {workflow['key']} must use one consistent local input"
            )

        destination = (root / next(iter(paths))).resolve()
        if root not in destination.parents:
            raise ValueError(f"Unsafe local source path: {destination}")
        separator = next(iter(separators))
        line_separator = next(iter(line_separators))
        records = [separator.join(captured_input["headers"])]
        records.extend(separator.join(row) for row in captured_input["rows"])
        content = line_separator.join(records)
        if destination in planned and planned[destination] != content:
            raise ValueError(
                f"Workflows provide conflicting inputs for {destination}"
            )
        planned[destination] = content

    for destination, content in planned.items():
        destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(destination.parent, 0o700)
        _secure_write(destination, content)
        written.append(destination)
    return written


def cleanup_inputs(settings: Settings) -> None:
    settings.validate_destructive_scope()
    root = settings.runtime_documents.resolve()
    if not root.exists():
        return
    marker = root / ".roxchaos-owned"
    if not marker.is_file():
        raise ValueError(f"Refusing to remove unowned runtime directory: {root}")
    shutil.rmtree(root)


def ensure_runtime_root(settings: Settings) -> None:
    settings.validate_destructive_scope()
    root = settings.runtime_documents.resolve()
    if root.exists():
        if not (root / ".roxchaos-owned").is_file():
            raise ValueError(f"Runtime directory is not owned by RoxChaos: {root}")
        return
    root.mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    _secure_write(root / ".roxchaos-owned", "RoxChaos generated directory\n")


def _capture_inputs(manifest: dict[str, Any], settings: Settings) -> None:
    workflows_by_name = {
        workflow["list"]["attributes"]["name"]: workflow
        for workflow in manifest["workflows"]
    }
    if set(workflows_by_name) != set(settings.workflows):
        raise ValueError("Exported workflows do not match the requested workflows")

    selected_identities: set[str] = set()
    for name in settings.workflows:
        workflow = workflows_by_name[name]
        local_sources = _local_sources(workflow)
        separators = {source["attributes"]["separator"] for source in local_sources}
        line_separators = {
            source["attributes"]["line_separator"] for source in local_sources
        }
        if not local_sources or len(separators) != 1 or len(line_separators) != 1:
            raise ValueError(f"Workflow {name} has inconsistent local sources")

        source_path = settings.source_inputs[name]
        if not source_path.is_file():
            raise FileNotFoundError(f"Input for {name} not found: {source_path}")
        separator = next(iter(separators))
        line_separator = next(iter(line_separators))
        records = [
            record
            for record in source_path.read_text(encoding="utf-8-sig").split(
                line_separator
            )
            if record
        ]
        if len(records) <= settings.input_rows:
            raise ValueError(
                f"Input for {name} has fewer than {settings.input_rows} data rows"
            )
        headers = records[0].split(separator)
        first_source = min(
            (
                task
                for task in workflow["tasks"]
                if task["specific_task"]["type"] == "RetrieverTask"
                and task["specific_task"].get("local_source", {})["attributes"].get(
                    "element_number"
                )
                is not None
            ),
            key=lambda task: task["attributes"]["step_order"],
        )["specific_task"]["local_source"]["attributes"]
        if first_source["element_number"] < settings.input_rows:
            raise ValueError(
                f"Workflow {name} reads fewer than {settings.input_rows} rows"
            )
        identity_columns = [
            column.strip() for column in first_source["element_selector"].split("|")
        ]
        try:
            identity_indexes = [headers.index(column) for column in identity_columns]
        except ValueError as error:
            raise ValueError(f"Input for {name} is missing an identity column") from error

        selected_rows: list[list[str]] = []
        identity_digests: list[str] = []
        for record in records[1:]:
            row = record.split(separator)
            if len(headers) != len(row):
                continue
            identity = " ".join(row[index] for index in identity_indexes)
            if not identity.strip() or identity in selected_identities:
                continue
            selected_identities.add(identity)
            selected_rows.append(row)
            identity_digests.append(hashlib.sha256(identity.encode()).hexdigest())
            if len(selected_rows) == settings.input_rows:
                break
        if len(selected_rows) != settings.input_rows:
            raise ValueError(
                f"Input for {name} has fewer than {settings.input_rows} globally unique identities"
            )
        workflow["input"] = {
            "captured_from": source_path.name,
            "headers": headers,
            "rows": selected_rows,
            "identity_sha256": identity_digests,
        }


def _local_sources(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        task["specific_task"]["local_source"]
        for task in workflow["tasks"]
        if task["specific_task"]["type"] == "RetrieverTask"
        and "local_source" in task["specific_task"]
    ]


def _secure_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary_name = tempfile.mkstemp(dir=path.parent, prefix=".roxchaos-")
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()

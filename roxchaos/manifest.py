from __future__ import annotations

import json
import os
import shutil
import tempfile
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
        content = separator.join(captured_input["headers"])
        content += line_separator
        content += separator.join(captured_input["row"])
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

    for name, workflow in workflows_by_name.items():
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
        records = source_path.read_text(encoding="utf-8-sig").split(line_separator)
        records = [record for record in records if record]
        if len(records) < 2:
            raise ValueError(f"Input for {name} has no data row")
        headers = records[0].split(separator)
        row = records[1].split(separator)
        if len(headers) != len(row):
            raise ValueError(
                f"Input for {name} has {len(headers)} headers but {len(row)} values"
            )
        workflow["input"] = {
            "captured_from": source_path.name,
            "headers": headers,
            "row": row,
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

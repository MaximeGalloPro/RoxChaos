from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from .manifest import (
    cleanup_inputs,
    ensure_runtime_root,
    load_manifest,
    materialize_inputs,
)
from .runner import CommandError, Compose, parse_marked_json, request_json, wait_until
from .settings import Settings


TERMINAL_RUN_STATUSES = {
    "completed",
    "completed_no_changes",
    "completed_with_errors",
    "failed",
    "stuck",
    "canceled",
}


class RailsBridge:
    def __init__(self, compose: Compose, scripts: Path) -> None:
        self.compose = compose
        self.scripts = scripts

    def run(
        self,
        script_name: str,
        payload: dict[str, Any] | None = None,
        env: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        source = self.scripts / script_name
        destination = f"/tmp/roxchaos_{script_name}"
        self.compose.copy_to(source, "web", destination)
        output = self.compose.exec(
            "web",
            "bin/rails",
            "runner",
            destination,
            input_text=None if payload is None else json.dumps(payload),
            env=env,
        )
        return parse_marked_json(output)


class ChaosHarness:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.compose = Compose(
            settings.compose_file,
            settings.project_name,
            settings.command_environment(),
        )
        self.rails = RailsBridge(self.compose, settings.root / "scripts")

    def setup(self) -> dict[str, Any]:
        self.settings.validate_destructive_scope()
        manifest = load_manifest(self.settings)
        materialize_inputs(manifest, self.settings)

        self.compose.command("down", "--volumes", "--remove-orphans")
        self.compose.command("build", "roxapi", "roxinfer-worker", "web", "jobs")
        self.compose.command("up", "-d", "roxapi-redis", "roxia-redis")
        self._wait_for_redis("roxapi-redis")
        self._wait_for_redis("roxia-redis")

        self.compose.command("up", "-d", "roxapi")
        self._wait_for_api()

        self.compose.command(
            "run",
            "--rm",
            "--no-deps",
            "web",
            "bin/rails",
            "db:drop",
            "db:create",
            "db:prepare:all",
        )
        self.compose.command("up", "-d", "web")
        self._wait_for_web()
        loaded = self.rails.run(
            "load_manifest.rb",
            manifest,
            env={"ROXCHAOS_DISABLE_SCHEDULES": "1"},
        )

        self.compose.command("up", "-d", "roxinfer-worker")
        self._wait_for_worker_group()
        self.compose.command("up", "-d", "jobs")
        self._wait_for_jobs()
        return loaded

    def teardown(self, *, strict: bool = True) -> None:
        errors: list[Exception] = []
        runtime_ready = True
        try:
            ensure_runtime_root(self.settings)
        except (OSError, ValueError) as error:
            errors.append(error)
            runtime_ready = False

        actions = [lambda: self.compose.command("stop", "jobs", "web")]
        if runtime_ready:
            actions.append(
                lambda: self.compose.command(
                    "run", "--rm", "--no-deps", "web", "bin/rails", "db:drop"
                )
            )
        actions.append(
            lambda: self.compose.command("down", "--volumes", "--remove-orphans")
        )
        if runtime_ready:
            actions.append(lambda: cleanup_inputs(self.settings))
        for action in actions:
            try:
                action()
            except (CommandError, OSError, ValueError) as error:
                errors.append(error)
        if strict and errors:
            raise RuntimeError(
                "RoxChaos cleanup failed:\n" + "\n".join(str(error) for error in errors)
            )

    def start_runs(self) -> list[dict[str, Any]]:
        result = self.rails.run(
            "start_runs.rb",
            {
                "organization": self.settings.organisation,
                "workflows": list(self.settings.workflows),
            },
        )
        return result["runs"]

    def snapshot(self, run_ids: list[int]) -> dict[str, Any]:
        return self.rails.run("snapshot_runs.rb", {"run_ids": run_ids})

    def wait_for_fault_window(self, run_ids: list[int]) -> dict[str, Any]:
        def ready() -> dict[str, Any] | None:
            snapshot = self.snapshot(run_ids)
            if any(not run["external_jobs"] for run in snapshot["runs"]):
                return None
            observed_jobs = [
                {
                    "run_database_id": run["id"],
                    "job": job,
                    "api_job": self.api_job(job["job_id"]),
                }
                for run in snapshot["runs"]
                for job in run["external_jobs"]
            ]
            processing = [
                observed
                for observed in observed_jobs
                if observed["api_job"].get("status") == "processing"
            ]
            if not processing:
                return None
            victim = processing[0]
            snapshot["api_jobs"] = [observed["api_job"] for observed in observed_jobs]
            snapshot["fault_victim"] = {
                "run_database_id": victim["run_database_id"],
                "job_id": victim["job"]["job_id"],
                "idempotency_key": victim["job"]["idempotency_key"],
            }
            return snapshot

        return wait_until(
            "two durable workflows with an inference in progress",
            ready,
            timeout=90,
            interval=0.5,
        )

    def crash_processing_services(self) -> None:
        self.compose.command(
            "kill", "-s", "SIGKILL", "roxinfer-worker", "roxapi", "jobs"
        )

    def restart_processing_services(self) -> None:
        self.compose.command("start", "roxapi")
        self._wait_for_api()
        time.sleep(3)
        self.compose.command("start", "roxinfer-worker")
        self._wait_for_worker_group()
        self.compose.command("start", "jobs")
        self._wait_for_jobs()

    def wait_for_terminal_runs(self, run_ids: list[int]) -> dict[str, Any]:
        def terminal() -> dict[str, Any] | None:
            snapshot = self.snapshot(run_ids)
            if all(run["status"] in TERMINAL_RUN_STATUSES for run in snapshot["runs"]):
                return snapshot
            return None

        return wait_until(
            "both workflow runs to become terminal",
            terminal,
            timeout=self.settings.timeout_seconds,
            interval=2,
        )

    def api_job(self, job_id: str) -> dict[str, Any]:
        return request_json(f"{self.settings.api_url}/jobs/{job_id}")

    def api_jobs(self) -> list[dict[str, Any]]:
        return request_json(f"{self.settings.api_url}/jobs")

    def pending_summary(self) -> Any:
        output = self.compose.exec(
            "roxapi-redis",
            "redis-cli",
            "--json",
            "XPENDING",
            "stream:roxinfer",
            "roxinfer-workers",
        )
        return json.loads(output)

    def stream_length(self) -> int:
        output = self.compose.exec(
            "roxapi-redis", "redis-cli", "--raw", "XLEN", "stream:roxinfer"
        )
        return int(output.strip())

    def idempotency_target(self, key: str) -> str | None:
        output = self.redis_value(f"idempotency:{key}")
        return output

    def redis_value(self, key: str) -> str | None:
        output = self.compose.exec(
            "roxapi-redis", "redis-cli", "--raw", "GET", key
        ).strip()
        return output or None

    def execution_expectations(self) -> dict[str, dict[str, int]]:
        manifest = load_manifest(self.settings)
        rules = {rule["key"]: rule for rule in manifest["rules"]}
        expectations: dict[str, dict[str, int]] = {}
        for workflow in manifest["workflows"]:
            tasks = workflow["tasks"]
            task_types = [task["specific_task"]["type"] for task in tasks]
            compliant_tags = sum(
                1
                for task in tasks
                if task["specific_task"]["type"] == "RuleTask"
                and rules[task["specific_task"]["rule_key"]]["attributes"][
                    "tag_on_conformity"
                ]
            )
            expectations[workflow["list"]["attributes"]["name"]] = {
                "workflow_run_items": sum(
                    task_type not in {"AsyncProcessTask", "DataSaverTask"}
                    for task_type in task_types
                ),
                "external_jobs": sum(
                    task_type in {"ReformatTextTask", "RuleTask"}
                    for task_type in task_types
                ),
                "successful_task_logs": sum(
                    task_type != "AsyncProcessTask" for task_type in task_types
                ),
                "analyses": 1,
                "reference_tags": compliant_tags,
                "workflow_document_logs": 1,
            }
        return expectations

    def provenance(self) -> dict[str, Any]:
        repositories: dict[str, Any] = {}
        for name in ("RoxIA", "RoxAPI", "RoxTune"):
            path = self.settings.workspace / name
            repositories[name] = {
                "commit": _git_output(path, "rev-parse", "HEAD"),
                "branch": _git_output(path, "branch", "--show-current"),
                "dirty": bool(_git_output(path, "status", "--porcelain")),
            }
        image_lines = self.compose.command("images", "--format", "json").splitlines()
        manifest_digest = hashlib.sha256(
            self.settings.manifest_path.read_bytes()
        ).hexdigest()
        return {
            "repositories": repositories,
            "images": [json.loads(line) for line in image_lines if line],
            "manifest_sha256": manifest_digest,
            "python": sys.version.split()[0],
            "docker": _command_output("docker", "version", "--format", "{{.Server.Version}}"),
        }

    def service_status(self) -> str:
        return self.compose.command("ps", "--all")

    def _wait_for_redis(self, service: str) -> None:
        wait_until(
            f"{service} Redis",
            lambda: self.compose.exec(service, "redis-cli", "ping").strip() == "PONG",
            timeout=60,
        )

    def _wait_for_api(self) -> None:
        wait_until(
            "RoxAPI",
            lambda: request_json(f"{self.settings.api_url}/jobs") is not None,
            timeout=90,
        )

    def _wait_for_web(self) -> None:
        wait_until(
            "RoxIA web",
            lambda: _request_text(f"{self.settings.roxia_url}/health") == "OK",
            timeout=180,
        )

    def _wait_for_jobs(self) -> None:
        wait_until(
            "RoxIA Solid Queue processes",
            lambda: self.compose.exec("jobs", "bin/jobs-healthcheck") is not None,
            timeout=180,
            interval=2,
        )

    def _wait_for_worker_group(self) -> None:
        wait_until(
            "RoxInference Redis Stream consumer group",
            lambda: self._service_running("roxinfer-worker")
            and bool(
                    json.loads(
                        self.compose.exec(
                            "roxapi-redis",
                            "redis-cli",
                            "--json",
                            "XINFO",
                            "GROUPS",
                            "stream:roxinfer",
                        )
                    )
                ),
            timeout=120,
        )

    def _service_running(self, service: str) -> bool:
        services = self.compose.command("ps", "--status", "running", "--services")
        return service in services.splitlines()


def _request_text(url: str) -> str:
    import urllib.request

    with urllib.request.urlopen(url, timeout=10) as response:
        return response.read().decode().strip()


def _git_output(path: Path, *arguments: str) -> str:
    return _command_output("git", *arguments, cwd=path)


def _command_output(*command: str, cwd: Path | None = None) -> str:
    result = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=True,
    )
    return result.stdout.strip()

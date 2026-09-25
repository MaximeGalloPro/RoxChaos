from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

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
        self.worker_instances: set[str] = set()
        self.initial_training_consumers: set[str] = set()
        self.inference_consumers_before_crash: set[str] = set()
        self.scheduler_startup: dict[str, Any] = {}

    def setup(self) -> dict[str, Any]:
        self.settings.validate_destructive_scope()
        manifest = load_manifest(self.settings)
        materialize_inputs(manifest, self.settings)

        self.compose.command("down", "--volumes", "--remove-orphans")
        if self.compose.environment.get("ROXCHAOS_SKIP_BUILD", "0").lower() not in {
            "1",
            "true",
            "yes",
        }:
            self.compose.command(
                "build",
                "roxapi",
                "roxinfer-worker",
                "roxtrain-worker",
                "web",
                "jobs",
            )
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

        self.assert_training_queue_empty()
        self.compose.command("up", "-d", "roxinfer-worker", "roxtrain-worker")
        self.scheduler_startup = self._exercise_scheduler_startup()
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

    def arm_scheduled_workflows(self) -> dict[str, Any]:
        return self.rails.run(
            "arm_scheduled_tasks.rb",
            {
                "organization": self.settings.organisation,
                "workflows": list(self.settings.workflows),
            },
        )

    def wait_for_scheduled_runs(self, arm_result: dict[str, Any]) -> dict[str, Any]:
        return wait_until(
            "ScheduledTasksJob recurring scan to create both workflow runs",
            lambda: self._discover_scheduled_runs(arm_result),
            timeout=180,
            interval=1,
        )

    def _discover_scheduled_runs(
        self, arm_result: dict[str, Any]
    ) -> dict[str, Any] | None:
        result = self.rails.run(
            "discover_scheduled_runs.rb", {"arm_result": arm_result}
        )
        return result if result["ready"] else None

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
        self.inference_consumers_before_crash = self._consumer_names("roxinfer")
        self.compose.command(
            "kill", "-s", "SIGKILL", "roxinfer-worker", "roxapi", "jobs"
        )

    def restart_processing_services(self) -> dict[str, Any]:
        self.compose.command("start", "roxapi")
        self._wait_for_api()
        wait_until(
            "stale RoxInference heartbeat to expire",
            lambda: not self._workers("roxinfer"),
            timeout=45,
            interval=1,
        )
        short_schedule = self._configure_worker_schedules(duration_minutes=9)
        self.compose.environment["ROXCHAOS_INFERENCE_SLEEP_SECONDS"] = "0"
        self.compose.command(
            "up", "-d", "--no-deps", "--force-recreate", "roxinfer-worker"
        )
        fresh_worker = self._wait_for_worker("roxinfer")
        fresh_consumers = wait_until(
            "a fresh scheduled RoxInference subprocess",
            lambda: self._new_inference_consumers(),
            timeout=90,
            interval=0.5,
        )
        next_turn_schedule = self._configure_worker_schedules(duration_minutes=30)
        self.compose.command("start", "jobs")
        self._wait_for_jobs()
        return {
            "worker": self._safe_worker(fresh_worker),
            "fresh_consumers": sorted(fresh_consumers),
            "cached_short_schedule": short_schedule,
            "next_turn_schedule": next_turn_schedule,
        }

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

    def pending_summary(self, queue_name: str = "roxinfer") -> Any:
        output = self.compose.exec(
            "roxapi-redis",
            "redis-cli",
            "--json",
            "XPENDING",
            f"stream:{queue_name}",
            f"{queue_name}-workers",
        )
        if "NOGROUP" in output:
            return [0, None, None, None]
        return json.loads(output)

    def stream_length(self, queue_name: str = "roxinfer") -> int:
        output = self.compose.exec(
            "roxapi-redis",
            "redis-cli",
            "--raw",
            "XLEN",
            f"stream:{queue_name}",
        )
        return int(output.strip())

    def assert_training_queue_empty(self) -> dict[str, Any]:
        summary = {
            "pending": self.pending_summary("roxtrain"),
            "stream_length": self.stream_length("roxtrain"),
            "api_jobs": [
                job["job_id"]
                for job in self.api_jobs()
                if job.get("name") == "roxtrain"
            ],
        }
        if summary["pending"][0] != 0 or summary["stream_length"] != 0:
            raise RuntimeError(f"RoxTrain queue is not empty: {summary}")
        if summary["api_jobs"]:
            raise RuntimeError(f"RoxTrain jobs were submitted: {summary['api_jobs']}")
        return summary

    def idempotency_target(self, key: str) -> str | None:
        output = self.redis_value(f"idempotency:{key}")
        return output

    def redis_value(self, key: str) -> str | None:
        output = self.compose.exec(
            "roxapi-redis", "redis-cli", "--raw", "GET", key
        ).strip()
        return output or None

    def execution_expectations(self) -> dict[str, dict[str, Any]]:
        manifest = load_manifest(self.settings)
        rules = {rule["key"]: rule for rule in manifest["rules"]}
        expectations: dict[str, dict[str, Any]] = {}
        for workflow in manifest["workflows"]:
            tasks = workflow["tasks"]
            task_types = [task["specific_task"]["type"] for task in tasks]
            documents = len(workflow["input"]["rows"])
            pre_workflow_tasks = sum(
                task["specific_task"]["type"] == "RetrieverTask"
                and task["specific_task"]["local_source"]["attributes"].get(
                    "element_number"
                )
                is not None
                for task in tasks
            )
            per_document_tasks = sum(
                task_type != "AsyncProcessTask" for task_type in task_types
            ) - pre_workflow_tasks
            compliant_tags = sum(
                1
                for task in tasks
                if task["specific_task"]["type"] == "RuleTask"
                and rules[task["specific_task"]["rule_key"]]["attributes"][
                    "tag_on_conformity"
                ]
            )
            expectations[workflow["list"]["attributes"]["name"]] = {
                "documents": documents,
                "workflow_run_items": per_document_tasks * documents,
                "items_per_document": per_document_tasks,
                "external_jobs": documents
                * sum(
                    task_type in {"ReformatTextTask", "RuleTask"}
                    for task_type in task_types
                ),
                "successful_task_logs": pre_workflow_tasks
                + (per_document_tasks * documents),
                "analyses": documents,
                "reference_tags": compliant_tags * documents,
                "workflow_document_logs": 1,
                "analysis_identity_sha256": workflow["input"]["identity_sha256"],
            }
        return expectations

    def wait_for_scheduler_cycle_after_inference(self) -> dict[str, Any]:
        observations = {
            "lock_absent": False,
            "training_heartbeat_gap": False,
        }

        def completed_cycle() -> dict[str, Any] | None:
            consumers = self._consumer_names("roxtrain")
            new_consumers = consumers - self.initial_training_consumers
            lock_exists = self._lock_exists()
            training_workers = self._workers("roxtrain")
            observations["lock_absent"] |= not lock_exists
            observations["training_heartbeat_gap"] |= not training_workers
            if (
                new_consumers
                and lock_exists
                and training_workers
                and observations["lock_absent"]
                and observations["training_heartbeat_gap"]
            ):
                return {
                    "new_training_consumers": sorted(new_consumers),
                    "lock_transition": "present -> absent -> present",
                    "lock_absence_observed": observations["lock_absent"],
                    "training_heartbeat_gap_observed": observations[
                        "training_heartbeat_gap"
                    ],
                    "worker": self._safe_worker(training_workers[0]),
                }
            return None

        return wait_until(
            "RoxInference to release the lock and RoxTrain to complete its next turn",
            completed_cycle,
            timeout=720,
            interval=0.5,
        )

    def _exercise_scheduler_startup(self) -> dict[str, Any]:
        inference = self._wait_for_worker("roxinfer")
        training = self._wait_for_worker("roxtrain")
        self.worker_instances = {inference["instance_id"], training["instance_id"]}
        if self._lock_exists():
            raise RuntimeError("Fresh scheduler lock volume unexpectedly contains a lock")

        first_samples = {
            "roxinfer": float(inference["last_heartbeat"]),
            "roxtrain": float(training["last_heartbeat"]),
        }
        time.sleep(11)
        refreshed_inference = self._wait_for_worker("roxinfer")
        refreshed_training = self._wait_for_worker("roxtrain")
        if float(refreshed_inference["last_heartbeat"]) <= first_samples["roxinfer"]:
            raise RuntimeError("RoxInference heartbeat did not refresh")
        if float(refreshed_training["last_heartbeat"]) <= first_samples["roxtrain"]:
            raise RuntimeError("RoxTrain heartbeat did not refresh")

        schedule = self._configure_worker_schedules(duration_minutes=30)
        wait_until(
            "RoxTrain heartbeat gap during its scheduled subprocess",
            lambda: not self._workers("roxtrain"),
            timeout=60,
            interval=0.25,
        )
        wait_until(
            "RoxTrain to create the inference lock",
            self._lock_exists,
            timeout=180,
            interval=0.25,
        )
        returned_training = self._wait_for_worker("roxtrain")
        self.initial_training_consumers = wait_until(
            "the first real RoxTrain stream consumer",
            lambda: self._consumer_names("roxtrain") or None,
            timeout=60,
            interval=0.5,
        )
        inference_consumers = wait_until(
            "the first real RoxInference stream consumer",
            lambda: self._consumer_names("roxinfer") or None,
            timeout=90,
            interval=0.5,
        )

        inference_before = float(self._wait_for_worker("roxinfer")["last_heartbeat"])
        time.sleep(11)
        active_inference = self._wait_for_worker("roxinfer")
        if float(active_inference["last_heartbeat"]) <= inference_before:
            raise RuntimeError("RoxInference heartbeat stopped during its subprocess")
        if not self._lock_exists():
            raise RuntimeError("RoxInference did not retain the shared lock during its turn")
        self.assert_training_queue_empty()

        return {
            "schedule": schedule,
            "initial_lock": "absent",
            "after_training_lock": "present",
            "heartbeat_refresh": {
                "roxinfer": True,
                "roxtrain": True,
                "roxtrain_gap_during_subprocess": True,
                "roxinfer_during_subprocess": True,
            },
            "workers": {
                "roxinfer": self._safe_worker(active_inference),
                "roxtrain": self._safe_worker(returned_training),
            },
            "consumers": {
                "roxinfer": sorted(inference_consumers),
                "roxtrain": sorted(self.initial_training_consumers),
            },
            "training_queue_empty": True,
        }

    def _configure_worker_schedules(self, *, duration_minutes: int) -> dict[str, Any]:
        now = datetime.now(ZoneInfo("Europe/Paris"))
        start = now.replace(second=0, microsecond=0)
        schedule = {
            "start_day": start.strftime("%A"),
            "start_time": start.strftime("%H:%M"),
            "duration": duration_minutes,
        }
        for instance_id in self.worker_instances:
            request_json(
                f"{self.settings.api_url}/schedules/{quote(instance_id, safe='')}",
                method="PUT",
                payload={"schedules": [schedule]},
            )
        return {
            "instances": sorted(self.worker_instances),
            "schedule": schedule,
            "configured_at": now.isoformat(),
            "ends_at": (start + timedelta(minutes=duration_minutes)).isoformat(),
        }

    def _workers(self, worker_type: str) -> list[dict[str, Any]]:
        payload = request_json(
            f"{self.settings.api_url}/workers/status?worker_type={worker_type}"
        )
        if not isinstance(payload, dict) or payload.get("error"):
            raise ValueError(f"Worker status probe failed: {payload}")
        workers = payload.get("workers")
        if not isinstance(workers, list):
            raise ValueError(f"Worker status probe returned no worker list: {payload}")
        return workers

    def _wait_for_worker(self, worker_type: str) -> dict[str, Any]:
        def one_worker() -> dict[str, Any] | None:
            workers = self._workers(worker_type)
            if len(workers) > 1:
                raise RuntimeError(
                    f"Expected one {worker_type} heartbeat, found {len(workers)}"
                )
            return workers[0] if workers else None

        return wait_until(
            f"one {worker_type} heartbeat",
            one_worker,
            timeout=120,
            interval=1,
        )

    def _consumer_names(self, queue_name: str) -> set[str]:
        output = self.compose.exec(
            "roxapi-redis",
            "redis-cli",
            "--json",
            "XINFO",
            "CONSUMERS",
            f"stream:{queue_name}",
            f"{queue_name}-workers",
        )
        consumers = json.loads(output)
        return {consumer["name"] for consumer in consumers}

    def _new_inference_consumers(self) -> set[str] | None:
        if not self._service_running("roxinfer-worker"):
            return None
        new_consumers = (
            self._consumer_names("roxinfer")
            - self.inference_consumers_before_crash
        )
        return new_consumers or None

    def _lock_exists(self) -> bool:
        state = self.compose.exec(
            "roxtrain-worker",
            "sh",
            "-c",
            "if [ -e /app/config/worker.lock ]; then printf present; else printf absent; fi",
        ).strip()
        if state not in {"present", "absent"}:
            raise ValueError(f"Lock probe returned an unexpected state: {state!r}")
        return state == "present"

    @staticmethod
    def _safe_worker(worker: dict[str, Any]) -> dict[str, Any]:
        return {
            key: worker.get(key)
            for key in (
                "worker_id",
                "instance_id",
                "worker_type",
                "status",
                "last_heartbeat",
            )
        }

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

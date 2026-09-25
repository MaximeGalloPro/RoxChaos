from __future__ import annotations

import json
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable


class CommandError(RuntimeError):
    pass


def run_command(
    command: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    input_text: str | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        input=input_text,
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise CommandError(
            f"Command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


class Compose:
    def __init__(
        self,
        compose_file: Path,
        project_name: str,
        environment: dict[str, str],
        project_directory: Path | None = None,
    ) -> None:
        self.compose_file = compose_file
        self.project_name = project_name
        self.environment = environment
        self.project_directory = project_directory or compose_file.parent

    def command(self, *arguments: str, input_text: str | None = None) -> str:
        command = [
            "docker",
            "compose",
            "--project-directory",
            str(self.project_directory),
            "-p",
            self.project_name,
            "-f",
            str(self.compose_file),
            *arguments,
        ]
        return run_command(
            command,
            cwd=self.project_directory,
            environment=self.environment,
            input_text=input_text,
        ).stdout

    def exec(
        self,
        service: str,
        *arguments: str,
        input_text: str | None = None,
        env: dict[str, str] | None = None,
    ) -> str:
        options = ["exec", "-T"]
        for name, value in (env or {}).items():
            options.extend(["-e", f"{name}={value}"])
        return self.command(*options, service, *arguments, input_text=input_text)

    def copy_to(self, source: Path, service: str, destination: str) -> None:
        self.command("cp", str(source), f"{service}:{destination}")


def parse_marked_json(output: str) -> Any:
    start_marker = "ROXCHAOS_JSON_BEGIN"
    end_marker = "ROXCHAOS_JSON_END"
    start = output.rfind(start_marker)
    end = output.find(end_marker, start + len(start_marker))
    if start < 0 or end < 0:
        raise ValueError(f"Rails runner returned no marked JSON:\n{output}")
    payload = output[start + len(start_marker) : end].strip()
    return json.loads(payload)


def request_json(
    url: str, method: str = "GET", payload: dict[str, Any] | None = None
) -> Any:
    body = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode())


def wait_until(
    description: str,
    predicate: Callable[[], Any],
    *,
    timeout: float,
    interval: float = 1.0,
) -> Any:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (CommandError, OSError, ValueError, urllib.error.URLError) as error:
            last_error = error
        time.sleep(interval)
    detail = f" Last error: {last_error}" if last_error else ""
    raise TimeoutError(f"Timed out waiting for {description}.{detail}")

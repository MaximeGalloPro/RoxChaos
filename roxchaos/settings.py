from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _enabled(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes"}


@dataclass(frozen=True)
class Settings:
    root: Path
    project_name: str
    manifest_path: Path
    report_dir: Path
    runtime_documents: Path
    organisation: str
    workflows: tuple[str, str]
    source_inputs: dict[str, Path]
    input_rows: int
    api_url: str
    roxia_url: str
    timeout_seconds: int
    keep_stack: bool
    database_name: str
    queue_database_name: str
    dotenv: dict[str, str]

    @classmethod
    def load(cls) -> "Settings":
        root = Path(__file__).resolve().parents[1]
        dotenv = _load_env_file(root / ".env")

        def value(name: str, default: str) -> str:
            return os.getenv(name, dotenv.get(name, default))

        def path_from_env(name: str, default: str) -> Path:
            path = Path(value(name, default))
            return path if path.is_absolute() else (root / path).resolve()

        api_port = value("ROXCHAOS_API_PORT", "8180")
        roxia_port = value("ROXCHAOS_ROXIA_PORT", "3180")
        workflows = ("Flash Emplois", "Flash Emplois - AUDIT")
        return cls(
            root=root,
            project_name=value("ROXCHAOS_PROJECT_NAME", "roxchaos"),
            manifest_path=path_from_env(
                "ROXCHAOS_MANIFEST", "manifests/current.local.json"
            ),
            report_dir=path_from_env("ROXCHAOS_REPORT_DIR", "reports"),
            runtime_documents=path_from_env(
                "ROXCHAOS_RUNTIME_DOCUMENTS", "runtime/documents"
            ),
            organisation=value("ROXCHAOS_ORGANISATION", "CD06"),
            workflows=workflows,
            source_inputs={
                workflows[0]: path_from_env(
                    "ROXCHAOS_NORMAL_INPUT",
                    "../RoxIA/documents/extract_4_TAB.output.csv",
                ),
                workflows[1]: path_from_env(
                    "ROXCHAOS_AUDIT_INPUT",
                    "../RoxIA/documents/AUDIT_extract_TEST_TAB.output.csv",
                ),
            },
            input_rows=int(value("ROXCHAOS_INPUT_ROWS", "3")),
            api_url=value("ROXCHAOS_API_URL", f"http://127.0.0.1:{api_port}"),
            roxia_url=value(
                "ROXCHAOS_ROXIA_URL", f"http://127.0.0.1:{roxia_port}"
            ),
            timeout_seconds=int(value("ROXCHAOS_TIMEOUT_SECONDS", "900")),
            keep_stack=value("ROXCHAOS_KEEP_STACK", "0").lower()
            in {"1", "true", "yes"},
            database_name=value("ROXCHAOS_DB_NAME", "roxchaos_development"),
            queue_database_name=value(
                "ROXCHAOS_QUEUE_DB_NAME", "roxchaos_development_queue"
            ),
            dotenv=dotenv,
        )

    @property
    def compose_file(self) -> Path:
        return self.root / "compose.yaml"

    @property
    def workspace(self) -> Path:
        return self.root.parent

    def command_environment(self) -> dict[str, str]:
        environment = self.dotenv | os.environ.copy()
        environment["ROXCHAOS_PROJECT_NAME"] = self.project_name
        environment["ROXCHAOS_DB_NAME"] = self.database_name
        environment["ROXCHAOS_QUEUE_DB_NAME"] = self.queue_database_name
        return environment

    def validate_destructive_scope(self) -> None:
        if not self.database_name.startswith("roxchaos_"):
            raise ValueError("ROXCHAOS_DB_NAME must start with 'roxchaos_'")
        if not self.queue_database_name.startswith("roxchaos_"):
            raise ValueError("ROXCHAOS_QUEUE_DB_NAME must start with 'roxchaos_'")
        if self.input_rows < 2:
            raise ValueError("ROXCHAOS_INPUT_ROWS must be at least 2")
        if not (
            self.project_name == "roxchaos"
            or self.project_name.startswith("roxchaos-")
        ):
            raise ValueError("ROXCHAOS_PROJECT_NAME must use the 'roxchaos' namespace")

        runtime_root = (self.root / "runtime").resolve()
        documents = self.runtime_documents.resolve()
        if documents == runtime_root or runtime_root not in documents.parents:
            raise ValueError("ROXCHAOS_RUNTIME_DOCUMENTS must be below RoxChaos/runtime")
        if self.runtime_documents.is_symlink():
            raise ValueError("ROXCHAOS_RUNTIME_DOCUMENTS must not be a symlink")


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, raw_value = line.split("=", 1)
        values[name.strip()] = raw_value.strip().strip("\"'")
    return values

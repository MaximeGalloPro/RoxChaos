from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class ChaosReport:
    report_dir: Path
    started_at: str = field(default_factory=_now)
    events: list[dict[str, Any]] = field(default_factory=list)
    outcome: str = "running"
    error: str | None = None
    final_state: dict[str, Any] | None = None

    def add(self, name: str, **details: Any) -> None:
        self.events.append({"at": _now(), "name": name, "details": details})

    def pass_test(self, final_state: dict[str, Any]) -> None:
        self.outcome = "passed"
        self.final_state = final_state

    def fail_test(self, error: BaseException) -> None:
        self.outcome = "failed"
        self.error = f"{type(error).__name__}: {error}"

    def write(self) -> None:
        self.report_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "started_at": self.started_at,
            "finished_at": _now(),
            "outcome": self.outcome,
            "error": self.error,
            "timeline": self.events,
            "final_state": self.final_state,
        }
        json_path = self.report_dir / "recovery.json"
        _secure_write(
            json_path,
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        )
        lines = [
            "# Roxia chaos recovery report",
            "",
            f"Outcome: **{self.outcome.upper()}**",
            f"Started: `{self.started_at}`",
            f"Finished: `{payload['finished_at']}`",
        ]
        if self.error:
            lines.extend(["", "## Error", "", f"```text\n{self.error}\n```"])
        lines.extend(["", "## Timeline", ""])
        for event in self.events:
            details = json.dumps(event["details"], ensure_ascii=False, sort_keys=True)
            lines.append(f"- `{event['at']}` **{event['name']}**: `{details}`")
        markdown_path = self.report_dir / "recovery.md"
        _secure_write(markdown_path, "\n".join(lines) + "\n")


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

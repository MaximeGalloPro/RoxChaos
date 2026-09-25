from __future__ import annotations

import argparse
import subprocess
import sys

from .manifest import capture_manifest
from .harness import ChaosHarness
from .settings import Settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Roxia Docker chaos harness")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("capture", help="Capture the two current workflow configs")
    run_parser = subparsers.add_parser("run", help="Run the end-to-end recovery test")
    run_parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    subparsers.add_parser("down", help="Remove only the dedicated chaos stack")
    args, extra_args = parser.parse_known_args()
    settings = Settings.load()

    if args.command == "capture":
        if extra_args:
            parser.error(f"unrecognized arguments: {' '.join(extra_args)}")
        manifest = capture_manifest(settings)
        print(
            f"Captured {len(manifest['workflows'])} workflows in "
            f"{settings.manifest_path}"
        )
        return 0

    if args.command == "down":
        if extra_args:
            parser.error(f"unrecognized arguments: {' '.join(extra_args)}")
        settings.validate_destructive_scope()
        ChaosHarness(settings).teardown()
        return 0

    if not settings.manifest_path.is_file():
        capture_manifest(settings)
    settings.report_dir.mkdir(parents=True, exist_ok=True)
    pytest_args = [*args.pytest_args, *extra_args]
    if pytest_args[:1] == ["--"]:
        pytest_args = pytest_args[1:]
    command = [sys.executable, "-m", "pytest", *pytest_args]
    return subprocess.call(command, cwd=settings.root)


if __name__ == "__main__":
    raise SystemExit(main())

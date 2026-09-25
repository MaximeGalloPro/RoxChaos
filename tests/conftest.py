from __future__ import annotations

import fcntl
import os

import pytest

from roxchaos.harness import ChaosHarness
from roxchaos.settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings.load()


@pytest.fixture(scope="session")
def chaos_lock(settings: Settings):
    runtime = settings.root / "runtime"
    runtime.mkdir(mode=0o700, exist_ok=True)
    lock_path = runtime / "roxchaos.lock"
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(descriptor, "w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("Another RoxChaos run is already active") from error
        yield
        fcntl.flock(lock, fcntl.LOCK_UN)


@pytest.fixture(scope="session")
def chaos_harness(settings: Settings, chaos_lock):
    harness = ChaosHarness(settings)
    setup_completed = False
    try:
        harness.setup()
        setup_completed = True
        yield harness
    finally:
        if not settings.keep_stack:
            harness.teardown(strict=setup_completed)

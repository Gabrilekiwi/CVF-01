"""Minimal process-entry smoke test."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from unittest.mock import patch

from cvf.config import load_settings
from cvf.main import main
from cvf.main import run as run_application


def test_one_shot_entry_returns_success() -> None:
    assert main(["--once"]) == 0


def test_collect_rejects_zero_duration_without_network() -> None:
    assert main(["collect", "--duration", "0"]) == 1


def test_long_running_entry_uses_clean_shutdown_path() -> None:
    settings = load_settings(environ={})
    restored: list[bool] = []

    def trigger_shutdown(
        stop_event: asyncio.Event,
        _logger: object,
    ) -> Callable[[], None]:
        asyncio.get_running_loop().call_soon(stop_event.set)
        return lambda: restored.append(True)

    with patch("cvf.main._install_shutdown_handlers", trigger_shutdown):
        assert asyncio.run(run_application(settings)) == 0

    assert restored == [True]

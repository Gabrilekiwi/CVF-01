"""Configuration loading and invariant tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from cvf import __version__
from cvf.config import load_settings


def test_default_configuration_loads_with_phase3_only_runtime_sections() -> None:
    settings = load_settings(environ={})

    assert __version__ == "0.3.1"
    assert settings.app.strategy_version == "0.3.1"
    assert settings.app.paper_trading_only is True
    assert settings.markets.canonical_symbols == [
        "BTC-USDT-PERP",
        "ETH-USDT-PERP",
    ]
    active_sections = set(settings.model_dump())
    assert active_sections.isdisjoint(
        {"scoring", "signal_rules", "execution", "risk", "exits"}
    )


def test_environment_override_uses_double_underscore_path() -> None:
    settings = load_settings(
        environ={
            "CVF__APP__STATUS_INTERVAL_SECONDS": "2.5",
            "CVF__LOGGING__LEVEL": "warning",
        }
    )

    assert settings.app.status_interval_seconds == 2.5
    assert settings.logging.level == "WARNING"


def test_safety_invariants_cannot_be_disabled_by_environment() -> None:
    with pytest.raises(ValidationError):
        load_settings(environ={"CVF__APP__PAPER_TRADING_ONLY": "false"})

    phase4_overrides = (
        "CVF__SCORING__LONG_ENTRY_THRESHOLD",
        "CVF__SIGNAL_RULES__SIGNAL_TTL_SECONDS",
        "CVF__EXECUTION__DEPTH_PENALTY_BPS",
        "CVF__RISK__ALLOW_MARTINGALE",
        "CVF__EXITS__REVERSE_SCORE_EXIT_THRESHOLD",
    )
    for name in phase4_overrides:
        with pytest.raises(ValidationError):
            load_settings(environ={name: "1"})

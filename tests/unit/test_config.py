"""Configuration loading and invariant tests."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from cvf import __version__
from cvf.config import load_settings


def test_default_configuration_loads_and_is_paper_only() -> None:
    settings = load_settings(environ={})

    assert __version__ == "0.2.1"
    assert settings.app.paper_trading_only is True
    assert settings.markets.canonical_symbols == [
        "BTC-USDT-PERP",
        "ETH-USDT-PERP",
    ]
    assert settings.risk.maximum_open_positions == 1


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

    with pytest.raises(ValidationError):
        load_settings(environ={"CVF__RISK__ALLOW_MARTINGALE": "true"})

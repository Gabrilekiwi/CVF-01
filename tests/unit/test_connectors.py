"""Phase-1 connector contract tests."""

from __future__ import annotations

import asyncio

import pytest

from cvf.config import load_settings
from cvf.exchanges import (
    BinanceMarketDataConnector,
    ConnectorNotImplementedError,
    OKXMarketDataConnector,
)
from cvf.models import Exchange, HealthStatus


def test_binance_subscription_plan_and_disconnected_health() -> None:
    settings = load_settings(environ={})
    connector = BinanceMarketDataConnector(
        settings.exchanges.binance,
        stale_after_ms=settings.health.stale_after_ms,
    )

    plans = connector.planned_subscriptions()
    health = asyncio.run(connector.health_check())

    assert {plan.venue_symbol for plan in plans} == {"BTCUSDT", "ETHUSDT"}
    assert any(plan.subscription_key == "btcusdt@aggTrade" for plan in plans)
    assert health.exchange is Exchange.BINANCE
    assert health.status is HealthStatus.DISCONNECTED
    assert health.rest_healthy is False
    assert health.details["network_attempted"] is False


def test_okx_index_subscription_uses_index_instrument() -> None:
    settings = load_settings(environ={})
    connector = OKXMarketDataConnector(
        settings.exchanges.okx,
        stale_after_ms=settings.health.stale_after_ms,
    )

    plans = connector.planned_subscriptions()

    assert any(
        plan.channel == "index-tickers" and plan.venue_symbol == "BTC-USDT"
        for plan in plans
    )


def test_phase_one_connectors_fail_closed_on_network_use() -> None:
    settings = load_settings(environ={})
    connector = BinanceMarketDataConnector(
        settings.exchanges.binance,
        stale_after_ms=settings.health.stale_after_ms,
    )

    with pytest.raises(ConnectorNotImplementedError, match="phase 2"):
        asyncio.run(connector.connect())

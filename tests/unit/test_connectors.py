"""Connector planning and offline-health contract tests."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from cvf.config import load_settings
from cvf.exchanges import (
    BinanceMarketDataConnector,
    OKXMarketDataConnector,
)
from cvf.models import Exchange, HealthStatus
from cvf.monitoring import StreamHealthRegistry


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


def test_binance_uses_current_routed_public_and_market_urls() -> None:
    settings = load_settings(environ={})
    connector = BinanceMarketDataConnector(
        settings.exchanges.binance,
        stale_after_ms=settings.health.stale_after_ms,
    )

    urls = connector.websocket_urls()

    assert urls["public"].startswith("wss://fstream.binance.com/public/stream?streams=")
    assert "btcusdt@depth@100ms" in urls["public"]
    assert "btcusdt@bookTicker" in urls["public"]
    assert urls["market"].startswith("wss://fstream.binance.com/market/stream?streams=")
    assert "btcusdt@aggTrade" in urls["market"]


@pytest.mark.asyncio
async def test_binance_open_interest_failure_does_not_poison_websocket_health(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(environ={})
    registry = StreamHealthRegistry(
        stale_after_ms=settings.health.stale_after_ms,
        maximum_core_latency_ms=settings.health.maximum_core_latency_ms,
        clock_skew_warning_ms=settings.health.clock_skew_warning_ms,
    )
    connector = BinanceMarketDataConnector(
        settings.exchanges.binance,
        stale_after_ms=settings.health.stale_after_ms,
        health_registry=registry,
    )
    snapshots = connector.health_snapshots()
    for snapshot in snapshots:
        registry.record_rest_result(snapshot.key, healthy=True)

    async def poll_time() -> None:
        return None

    async def fail_open_interest() -> None:
        connector._stop.set()
        raise httpx.ConnectTimeout("simulated OI timeout")

    monkeypatch.setattr(connector, "_poll_server_time", poll_time)
    monkeypatch.setattr(connector, "_poll_open_interest_once", fail_open_interest)

    await connector._open_interest_loop()

    rest_health = {
        snapshot.key.channel: snapshot.rest_healthy
        for snapshot in connector.health_snapshots()
    }
    assert rest_health["open_interest"] is False
    assert rest_health["aggTrade"] is True
    assert rest_health["bookTicker"] is True
    assert rest_health["depth@100ms"] is True
    assert rest_health["markPrice@1s"] is True
    assert rest_health["forceOrder"] is True

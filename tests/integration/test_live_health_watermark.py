"""Regression coverage for live health-event receive-time watermarking."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

import cvf.collector as collector_module
from cvf.collector import MarketDataCollector
from cvf.config import load_settings
from cvf.models import Exchange
from cvf.monitoring import StreamKey


@pytest.mark.asyncio
async def test_status_loop_stamps_each_health_event_when_it_is_published(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    """A slow health batch must not reuse one stale receive timestamp."""

    settings = load_settings(environ={})
    exchanges = settings.exchanges.model_copy(
        update={
            "binance": settings.exchanges.binance.model_copy(
                update={"enabled": False}
            ),
            "okx": settings.exchanges.okx.model_copy(update={"enabled": False}),
        }
    )
    collector = MarketDataCollector(
        settings.model_copy(update={"exchanges": exchanges}),
        output_path=tmp_path / "raw",
    )

    snapshots = [
        SimpleNamespace(
            key=StreamKey(Exchange.BINANCE, "BTC-USDT-PERP", "aggTrade")
        ),
        SimpleNamespace(
            key=StreamKey(Exchange.BINANCE, "BTC-USDT-PERP", "bookTicker")
        ),
    ]
    monkeypatch.setattr(
        collector,
        "health_snapshots",
        lambda *, now=None: snapshots,
    )
    monkeypatch.setattr(collector_module, "asdict", lambda value: {})

    observed_times = []

    def fake_exchange_health(key: StreamKey, *, now: Any) -> object:
        observed_times.append(now)
        return object()

    monkeypatch.setattr(collector._health, "exchange_health", fake_exchange_health)

    stop_event = asyncio.Event()

    async def slow_record_event(event: object) -> None:
        if len(observed_times) == 1:
            # Simulate journal/publish work long enough for a live receive-time
            # clock to advance between health events in the same status batch.
            await asyncio.sleep(0.01)
        elif len(observed_times) == 2:
            stop_event.set()

    monkeypatch.setattr(collector, "_record_event", slow_record_event)

    await collector._status_loop(
        stop_event,
        rss_samples=[],
        cpu_started=0.0,
    )

    assert len(observed_times) == 2
    assert observed_times[1] > observed_times[0]

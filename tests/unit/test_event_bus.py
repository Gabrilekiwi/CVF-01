"""Normalized event fan-out lifecycle and failure behavior."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from cvf.models import AggressorSide, Exchange, Trade
from cvf.pipeline import EventBusError, NormalizedEventBus

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def trade(sequence: int) -> Trade:
    return Trade(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        exchange_timestamp=NOW,
        local_receive_timestamp=NOW,
        normalization_timestamp=NOW,
        sequence_id=sequence,
        raw_payload_reference=f"raw://{sequence:032x}",
        trade_id=str(sequence),
        price=Decimal("100"),
        quantity=Decimal("1"),
        aggressor_side=AggressorSide.BUY,
    )


@pytest.mark.asyncio
async def test_fans_out_in_order_and_drains_every_consumer() -> None:
    first: list[int] = []
    second: list[int] = []
    bus = NormalizedEventBus(default_queue_capacity=2)

    async def consume_first(event: Trade) -> None:
        first.append(int(event.trade_id))

    async def consume_second(event: Trade) -> None:
        second.append(int(event.trade_id))

    bus.register("first", consume_first)  # type: ignore[arg-type]
    bus.register("second", consume_second)  # type: ignore[arg-type]
    await bus.start()
    for sequence in range(5):
        await bus.publish(trade(sequence))
    await bus.close()

    assert first == list(range(5))
    assert second == list(range(5))
    assert bus.stats["first"].published_events == 5
    assert bus.stats["first"].processed_events == 5
    assert bus.stats["second"].queue_depth == 0


@pytest.mark.asyncio
async def test_applies_backpressure_without_dropping() -> None:
    release = asyncio.Event()
    entered = asyncio.Event()
    received: list[int] = []
    bus = NormalizedEventBus(default_queue_capacity=1)

    async def slow_consumer(event: Trade) -> None:
        entered.set()
        await release.wait()
        received.append(int(event.trade_id))

    bus.register("slow", slow_consumer)  # type: ignore[arg-type]
    await bus.start()
    await bus.publish(trade(1))
    await entered.wait()
    await bus.publish(trade(2))
    blocked_publish = asyncio.create_task(bus.publish(trade(3)))
    await asyncio.sleep(0)

    assert not blocked_publish.done()
    release.set()
    await blocked_publish
    await bus.close()

    assert received == [1, 2, 3]
    assert bus.stats["slow"].backpressure_events >= 1


@pytest.mark.asyncio
async def test_consumer_failure_is_observable() -> None:
    bus = NormalizedEventBus(default_queue_capacity=1)

    async def fail(_event: Trade) -> None:
        raise RuntimeError("consumer broke")

    bus.register("broken", fail)  # type: ignore[arg-type]
    await bus.start()
    await bus.publish(trade(1))
    await asyncio.sleep(0)

    with pytest.raises(EventBusError, match="consumer broke"):
        await bus.close()

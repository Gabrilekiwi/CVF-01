"""Shared live/replay receive-time feature timeline boundaries."""

from __future__ import annotations

import asyncio
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from cvf.clock import DecisionTick, TickKind
from cvf.config import load_settings
from cvf.features.models import CrossVenueFeatureSnapshot, FeatureUnavailableCode
from cvf.features.runtime import FeatureRuntime, ReceiveTimeFeatureDriver
from cvf.models import (
    AggressorSide,
    Exchange,
    OrderBookLevel,
    OrderBookSnapshot,
    Trade,
)
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline import NormalizedEventBus
from cvf.storage import FeatureParquetReader

NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)
SYMBOL = "BTC-USDT-PERP"


@contextmanager
def scratch_directory() -> Iterator[Path]:
    path = Path("data/processed") / f"feature-runtime-{uuid4().hex[:8]}"
    path.mkdir()
    try:
        yield path
    finally:
        resolved = str(path.resolve())
        native_path = (
            f"\\\\?\\{resolved}"
            if os.name == "nt" and not resolved.startswith("\\\\?\\")
            else resolved
        )
        shutil.rmtree(native_path)


def trade(
    exchange: Exchange,
    at: datetime,
    sequence: int,
) -> Trade:
    return Trade(
        exchange=exchange,
        symbol=SYMBOL,
        exchange_timestamp=at,
        local_receive_timestamp=at,
        normalization_timestamp=at + timedelta(microseconds=1),
        sequence_id=sequence,
        raw_payload_reference=f"raw://{sequence:032x}",
        trade_id=str(sequence),
        price=Decimal("100"),
        quantity=Decimal("1"),
        aggressor_side=AggressorSide.BUY,
    )


def book(
    exchange: Exchange,
    at: datetime,
    sequence: int,
    midpoint: Decimal,
    *,
    generation: int = 0,
) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        exchange=exchange,
        symbol=SYMBOL,
        exchange_timestamp=at,
        local_receive_timestamp=at,
        normalization_timestamp=at + timedelta(microseconds=1),
        sequence_id=sequence,
        raw_payload_reference=(
            f"raw://{exchange.value.lower()}-{generation}-{sequence:020d}"
        ),
        bids=[
            OrderBookLevel(
                price=midpoint - Decimal("0.5"),
                quantity=Decimal("1"),
            )
        ],
        asks=[
            OrderBookLevel(
                price=midpoint + Decimal("0.5"),
                quantity=Decimal("1"),
            )
        ],
        depth=1,
        generation=generation,
    )


class RecordingRuntime:
    def __init__(self, observed: list[int]) -> None:
        self.observed = observed
        self.feature_ticks: list[tuple[datetime, tuple[int, ...]]] = []

    async def consume_tick(self, tick: DecisionTick) -> None:
        if tick.kind is TickKind.FEATURE:
            self.feature_ticks.append((tick.timestamp, tuple(self.observed)))


@pytest.mark.asyncio
async def test_boundary_events_precede_tick_and_future_event_does_not() -> None:
    settings = load_settings(environ={})
    observed: list[int] = []
    bus = NormalizedEventBus(default_queue_capacity=20)

    async def capture(event: NormalizedMarketEvent) -> None:
        assert event.sequence_id is not None
        observed.append(int(event.sequence_id))

    bus.register("capture", capture)
    runtime = RecordingRuntime(observed)
    driver = ReceiveTimeFeatureDriver(
        settings,
        event_bus=bus,
        runtime=cast(Any, runtime),
    )
    await bus.start()
    try:
        await driver.publish(
            trade(Exchange.BINANCE, NOW + timedelta(microseconds=100), 1)
        )
        await driver.publish(
            trade(
                Exchange.BINANCE,
                NOW + timedelta(seconds=1, microseconds=1),
                5,
            )
        )
        await driver.publish(
            trade(Exchange.OKX, NOW + timedelta(seconds=1), 4)
        )
        await driver.publish(
            trade(
                Exchange.BINANCE,
                NOW + timedelta(seconds=1, microseconds=-1),
                2,
            )
        )
        await driver.publish(
            trade(Exchange.BINANCE, NOW + timedelta(seconds=1), 3)
        )
        await driver.finish()
    finally:
        await bus.close()

    assert runtime.feature_ticks == [
        (NOW + timedelta(seconds=1), (1, 2, 3, 4))
    ]
    assert observed == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_capacity_one_drains_safe_event_before_full_check() -> None:
    base = load_settings(environ={})
    settings = base.model_copy(
        update={
            "pipeline": base.pipeline.model_copy(
                update={"consumer_queue_capacity": 1}
            )
        }
    )
    observed: list[int] = []
    bus = NormalizedEventBus(default_queue_capacity=10)

    async def capture(event: NormalizedMarketEvent) -> None:
        assert event.sequence_id is not None
        observed.append(int(event.sequence_id))

    bus.register("capture", capture)
    driver = ReceiveTimeFeatureDriver(
        settings,
        event_bus=bus,
        runtime=cast(Any, RecordingRuntime(observed)),
    )
    await bus.start()
    try:
        await driver.publish(trade(Exchange.BINANCE, NOW, 1))

        # Exactly one lateness interval later, event 1 is safe and must be
        # drained before the capacity check for event 2.
        await driver.publish(
            trade(
                Exchange.OKX,
                NOW + timedelta(
                    milliseconds=settings.features.receive_time_reorder_ms
                ),
                2,
            )
        )
        await driver.finish()
    finally:
        await bus.close()

    assert observed == [1, 2]


@pytest.mark.asyncio
async def test_capacity_one_still_rejects_a_true_within_lateness_overflow() -> None:
    base = load_settings(environ={})
    settings = base.model_copy(
        update={
            "pipeline": base.pipeline.model_copy(
                update={"consumer_queue_capacity": 1}
            )
        }
    )
    bus = NormalizedEventBus(default_queue_capacity=10)
    driver = ReceiveTimeFeatureDriver(
        settings,
        event_bus=bus,
        runtime=cast(Any, RecordingRuntime([])),
    )
    await bus.start()
    try:
        await driver.publish(trade(Exchange.BINANCE, NOW, 1))

        with pytest.raises(RuntimeError, match="reorder buffer is full"):
            await driver.publish(
                trade(
                    Exchange.OKX,
                    NOW
                    + timedelta(
                        milliseconds=(
                            settings.features.receive_time_reorder_ms - 1
                        )
                    ),
                    2,
                )
            )
    finally:
        await bus.close()


class BlockingTickRuntime:
    def __init__(self, blocked_timestamp: datetime) -> None:
        self.blocked_timestamp = blocked_timestamp
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.feature_ticks: list[datetime] = []

    async def consume_tick(self, tick: DecisionTick) -> None:
        if tick.kind is not TickKind.FEATURE:
            return
        self.feature_ticks.append(tick.timestamp)
        if tick.timestamp == self.blocked_timestamp:
            self.entered.set()
            await self.release.wait()


@pytest.mark.asyncio
async def test_cancelling_due_tick_batch_waits_for_the_inner_transition() -> None:
    settings = load_settings(environ={})
    bus = NormalizedEventBus(default_queue_capacity=10)
    runtime = BlockingTickRuntime(NOW + timedelta(seconds=2))
    driver = ReceiveTimeFeatureDriver(
        settings,
        event_bus=bus,
        runtime=cast(Any, runtime),
    )
    await bus.start()
    try:
        await driver.publish(trade(Exchange.BINANCE, NOW, 1))
        await driver.advance_to(NOW)

        advance_task = asyncio.create_task(
            driver.advance_to(NOW + timedelta(seconds=3))
        )
        await runtime.entered.wait()
        advance_task.cancel()
        await asyncio.sleep(0)
        advance_task.cancel()
        await asyncio.sleep(0)

        # Cancellation cannot release the driver's lock while its scheduler
        # transition is only partially consumed.
        assert not advance_task.done()

        runtime.release.set()
        with pytest.raises(asyncio.CancelledError):
            await advance_task

        await driver.advance_to(NOW + timedelta(seconds=4))
        await driver.finish()
    finally:
        runtime.release.set()
        await bus.close()

    assert runtime.feature_ticks == [
        NOW + timedelta(seconds=1),
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=3),
        NOW + timedelta(seconds=4),
    ]


@pytest.mark.asyncio
async def test_repeated_close_cancellation_cannot_cancel_runtime_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings(environ={})
    with scratch_directory() as temporary:
        runtime = FeatureRuntime(settings, output_path=temporary / "features")
        await runtime.start()
        await runtime.consume_event(trade(Exchange.BINANCE, NOW, 1))
        await runtime.consume_tick(
            DecisionTick(
                timestamp=NOW + timedelta(seconds=1),
                kind=TickKind.FEATURE,
            )
        )
        close_entered = asyncio.Event()
        release_close = asyncio.Event()
        original_close = runtime.writer.close

        async def blocking_writer_close() -> None:
            close_entered.set()
            await release_close.wait()
            await original_close()

        monkeypatch.setattr(runtime.writer, "close", blocking_writer_close)
        closing = asyncio.create_task(runtime.close())
        try:
            await close_entered.wait()
            closing.cancel()
            await asyncio.sleep(0)
            closing.cancel()
            await asyncio.sleep(0)

            assert not closing.done()
            assert runtime._close_task is not None
            assert not runtime._close_task.cancelled()

            release_close.set()
            with pytest.raises(asyncio.CancelledError):
                await closing
        finally:
            release_close.set()
            if not closing.done():
                await asyncio.gather(closing, return_exceptions=True)
            await runtime.close()

        assert runtime._closed
        assert runtime.writer._closed
        assert runtime.stats.writer.accepted_snapshots > 0
        assert (
            runtime.stats.writer.accepted_snapshots
            == runtime.stats.writer.written_snapshots
        )


@pytest.mark.asyncio
async def test_runtime_rejects_out_of_universe_and_non_health_wildcard_events() -> None:
    settings = load_settings(environ={})
    with scratch_directory() as temporary:
        runtime = FeatureRuntime(settings, output_path=temporary / "features")
        await runtime.start()
        try:
            configured_out = trade(Exchange.BINANCE, NOW, 1).model_copy(
                update={"symbol": "SOL-USDT-PERP"}
            )
            with pytest.raises(
                ValueError,
                match="outside the configured feature universe",
            ):
                await runtime.consume_event(configured_out)

            non_health_wildcard = trade(Exchange.BINANCE, NOW, 2).model_copy(
                update={"symbol": "*"}
            )
            with pytest.raises(
                ValueError,
                match="only exchange-health events may use wildcard symbols",
            ):
                await runtime.consume_event(non_health_wildcard)
        finally:
            await runtime.close()

    assert runtime.stats.normalized_events == 0


@pytest.mark.asyncio
async def test_book_generation_change_rewarms_cross_venue_spread_history() -> None:
    base = load_settings(environ={})
    settings = base.model_copy(
        update={
            "markets": base.markets.model_copy(
                update={"canonical_symbols": [SYMBOL]}
            ),
            "timing": base.timing.model_copy(
                update={"feature_windows_seconds": [5]}
            ),
            "features": base.features.model_copy(
                update={"cross_venue_zscore_minimum_samples": 2}
            ),
        }
    )
    with scratch_directory() as temporary:
        output = temporary / "features"
        runtime = FeatureRuntime(
            settings,
            output_path=output,
            writer_batch_rows=1,
            writer_flush_seconds=60,
        )
        await runtime.start()
        try:
            observations = (
                (1, Decimal("100"), Decimal("99")),
                (2, Decimal("102"), Decimal("99")),
                (3, Decimal("104"), Decimal("99")),
            )
            for second, binance_mid, okx_mid in observations:
                source_at = NOW + timedelta(seconds=second, milliseconds=-100)
                await runtime.consume_event(
                    book(Exchange.BINANCE, source_at, second, binance_mid)
                )
                await runtime.consume_event(
                    book(Exchange.OKX, source_at, second, okx_mid)
                )
                await runtime.consume_tick(
                    DecisionTick(
                        timestamp=NOW + timedelta(seconds=second),
                        kind=TickKind.FEATURE,
                    )
                )

            rebuild_source_at = NOW + timedelta(seconds=3, milliseconds=900)
            await runtime.consume_event(
                book(
                    Exchange.BINANCE,
                    rebuild_source_at,
                    4,
                    Decimal("106"),
                    generation=1,
                )
            )
            await runtime.consume_event(
                book(
                    Exchange.OKX,
                    rebuild_source_at,
                    4,
                    Decimal("99"),
                )
            )
            await runtime.consume_tick(
                DecisionTick(
                    timestamp=NOW + timedelta(seconds=4),
                    kind=TickKind.FEATURE,
                )
            )
        finally:
            await runtime.close()

        crosses = {
            snapshot.decision_timestamp: snapshot
            for record in FeatureParquetReader(output).iter_records()
            if isinstance(
                (snapshot := record.snapshot),
                CrossVenueFeatureSnapshot,
            )
            and snapshot.symbol == SYMBOL
            and snapshot.window_seconds == 5
        }
        before_rebuild = crosses[NOW + timedelta(seconds=3)]
        after_rebuild = crosses[NOW + timedelta(seconds=4)]

        assert before_rebuild.price.mid_price_spread_zscore is not None
        assert after_rebuild.price.mid_price_spread_zscore is None
        assert after_rebuild.binance_book_generation == 1
        assert after_rebuild.okx_book_generation == 0
        assert FeatureUnavailableCode.INSUFFICIENT_HISTORY in {
            reason.code for reason in after_rebuild.unavailable_reasons
        }
        assert runtime.stats.book_generation_rebuilds == 1


@pytest.mark.asyncio
async def test_rejected_stale_generation_does_not_clear_warm_cross_history() -> None:
    base = load_settings(environ={})
    settings = base.model_copy(
        update={
            "markets": base.markets.model_copy(
                update={"canonical_symbols": [SYMBOL]}
            ),
            "timing": base.timing.model_copy(
                update={"feature_windows_seconds": [5]}
            ),
            "features": base.features.model_copy(
                update={"cross_venue_zscore_minimum_samples": 2}
            ),
        }
    )
    with scratch_directory() as temporary:
        output = temporary / "features"
        runtime = FeatureRuntime(
            settings,
            output_path=output,
            writer_batch_rows=1,
            writer_flush_seconds=60,
        )
        await runtime.start()
        try:
            observations = (
                (1, Decimal("100"), Decimal("99")),
                (2, Decimal("102"), Decimal("99")),
                (3, Decimal("104"), Decimal("99")),
            )
            for second, binance_mid, okx_mid in observations:
                source_at = NOW + timedelta(seconds=second, milliseconds=-100)
                await runtime.consume_event(
                    book(
                        Exchange.BINANCE,
                        source_at,
                        second,
                        binance_mid,
                        generation=1,
                    )
                )
                await runtime.consume_event(
                    book(
                        Exchange.OKX,
                        source_at,
                        second,
                        okx_mid,
                        generation=1,
                    )
                )
                await runtime.consume_tick(
                    DecisionTick(
                        timestamp=NOW + timedelta(seconds=second),
                        kind=TickKind.FEATURE,
                    )
                )

            late_stale_generation = book(
                Exchange.BINANCE,
                NOW + timedelta(milliseconds=500),
                99,
                Decimal("500"),
                generation=0,
            ).model_copy(
                update={
                    "local_receive_timestamp": NOW
                    + timedelta(seconds=3, milliseconds=500),
                    "normalization_timestamp": NOW
                    + timedelta(seconds=3, milliseconds=501),
                }
            )
            await runtime.consume_event(late_stale_generation)
            await runtime.consume_tick(
                DecisionTick(
                    timestamp=NOW + timedelta(seconds=4),
                    kind=TickKind.FEATURE,
                )
            )
        finally:
            await runtime.close()

        crosses = {
            snapshot.decision_timestamp: snapshot
            for record in FeatureParquetReader(output).iter_records()
            if isinstance(
                (snapshot := record.snapshot),
                CrossVenueFeatureSnapshot,
            )
            and snapshot.symbol == SYMBOL
            and snapshot.window_seconds == 5
        }

        assert crosses[
            NOW + timedelta(seconds=3)
        ].price.mid_price_spread_zscore is not None
        assert crosses[
            NOW + timedelta(seconds=4)
        ].price.mid_price_spread_zscore is not None
        assert runtime.stats.book_generation_rebuilds == 0
        assert runtime.stats.feature_state.rejected_events == 1


@pytest.mark.asyncio
async def test_events_behind_watermark_fail_closed_and_finish_seals() -> None:
    settings = load_settings(environ={})
    bus = NormalizedEventBus(default_queue_capacity=10)
    runtime = RecordingRuntime([])
    driver = ReceiveTimeFeatureDriver(
        settings,
        event_bus=bus,
        runtime=cast(Any, runtime),
    )
    await bus.start()
    try:
        await driver.publish(
            trade(Exchange.BINANCE, NOW + timedelta(milliseconds=100), 1)
        )
        await driver.advance_to(NOW + timedelta(seconds=1))

        with pytest.raises(RuntimeError, match="behind"):
            await driver.publish(
                trade(Exchange.OKX, NOW + timedelta(milliseconds=500), 2)
            )

        await driver.finish()
        with pytest.raises(RuntimeError, match="after"):
            await driver.publish(
                trade(Exchange.BINANCE, NOW + timedelta(seconds=2), 3)
            )
    finally:
        await bus.close()

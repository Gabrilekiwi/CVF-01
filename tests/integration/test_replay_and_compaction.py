"""Raw scan, normalized replay, and lossless compaction integration."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from cvf.clock import DecisionScheduler, DecisionTick, TickKind
from cvf.config import load_settings
from cvf.features import FeatureStatePipeline, MarketStateStore
from cvf.models import Exchange, Trade
from cvf.pipeline import NormalizedEventBus
from cvf.replay import RawParquetReader, ReplayOrder, ReplayRunner
from cvf.storage import AsyncPartitionedParquetWriter, RawMarketRecord, compact_raw_tree

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


@contextmanager
def scratch_directory() -> Iterator[Path]:
    path = Path("data/processed") / f"phase25-{uuid4().hex[:8]}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def agg_trade(sequence: int, offset_ms: int) -> RawMarketRecord:
    received = NOW + timedelta(milliseconds=offset_ms)
    payload = {
        "e": "aggTrade",
        "E": int(received.timestamp() * 1000),
        "a": sequence,
        "s": "BTCUSDT",
        "p": "100.0",
        "q": "2.0",
        "f": sequence,
        "l": sequence,
        "T": int(received.timestamp() * 1000),
        "m": False,
    }
    return RawMarketRecord(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        channel="aggTrade",
        message_kind="market_data",
        transport="websocket",
        exchange_timestamp=received,
        local_receive_timestamp=received + timedelta(milliseconds=10),
        normalization_timestamp=received + timedelta(milliseconds=11),
        sequence_id=sequence,
        connection_generation=3,
        raw_payload=json.dumps(payload, separators=(",", ":")).encode(),
    )


@pytest.mark.asyncio
async def test_reader_replays_live_normalization_equivalently() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(agg_trade(2, 2))
        await writer.write(agg_trade(1, 1))
        await writer.close()

        replayed: list[Trade] = []
        bus = NormalizedEventBus(default_queue_capacity=2)
        feature_state = FeatureStatePipeline(
            MarketStateStore(load_settings(environ={}).features)
        )

        async def capture(event: Trade) -> None:
            replayed.append(event)

        bus.register("capture", capture)  # type: ignore[arg-type]
        bus.register("feature-state", feature_state.consume)
        reader = RawParquetReader(raw)
        runner = ReplayRunner(
            event_bus=bus,
            order=ReplayOrder.EVENT_TIME,
            speed=0,
        )
        summary = await runner.run(reader.iter_records())

        assert [event.trade_id for event in replayed] == ["1", "2"]
        assert all(event.quantity == 2 for event in replayed)
        assert all(event.raw_payload_reference is not None for event in replayed)
        assert summary.raw_records == 2
        assert summary.normalized_events == 2
        assert summary.connection_generations["BINANCE:aggTrade:BTC-USDT-PERP"] == 3
        state = feature_state.store.state(Exchange.BINANCE, "BTC-USDT-PERP")
        assert [item.value.trade_id for item in state.trades] == ["1", "2"]
        assert feature_state.stats.accepted_events == 2


@pytest.mark.asyncio
async def test_okx_replay_primes_contract_metadata_before_event_time() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=10,
        )
        metadata = RawMarketRecord(
            exchange=Exchange.OKX,
            symbol="BTC-USDT-PERP",
            channel="instrument_metadata",
            message_kind="market_data",
            transport="rest",
            local_receive_timestamp=NOW,
            connection_generation=0,
            raw_payload=Path("tests/fixtures/okx/instrument_btc_live.json").read_bytes(),
        )
        trade_record = RawMarketRecord(
            exchange=Exchange.OKX,
            symbol="BTC-USDT-PERP",
            channel="trades",
            message_kind="market_data",
            transport="websocket",
            exchange_timestamp=NOW - timedelta(seconds=1),
            local_receive_timestamp=NOW + timedelta(seconds=1),
            connection_generation=1,
            raw_payload=Path("tests/fixtures/okx/trade_live.json").read_bytes(),
        )
        await writer.start()
        await writer.write(trade_record)
        await writer.write(metadata)
        await writer.close()

        replayed: list[Trade] = []
        bus = NormalizedEventBus(default_queue_capacity=2)

        async def capture(event: Trade) -> None:
            replayed.append(event)

        bus.register("capture", capture)  # type: ignore[arg-type]
        summary = await ReplayRunner(event_bus=bus, speed=0).run(
            RawParquetReader(raw).iter_records()
        )

        assert len(replayed) == 1
        assert replayed[0].contract_quantity is not None
        assert replayed[0].quantity == replayed[0].contract_quantity * Decimal("0.01")
        assert summary.raw_records == 2
        assert summary.skipped_records == 1


@pytest.mark.asyncio
async def test_replay_ticks_include_all_events_at_the_decision_boundary() -> None:
    bus = NormalizedEventBus(default_queue_capacity=2)
    feature_state = FeatureStatePipeline(
        MarketStateStore(load_settings(environ={}).features)
    )
    bus.register("feature-state", feature_state.consume)
    observed_trade_counts: list[tuple[datetime, int]] = []

    async def capture_tick(tick: DecisionTick) -> None:
        if tick.kind is not TickKind.FEATURE:
            return
        state = feature_state.store.state(Exchange.BINANCE, "BTC-USDT-PERP")
        observed_trade_counts.append((tick.timestamp, len(state.trades)))

    scheduler = DecisionScheduler(
        start=NOW,
        feature_interval=timedelta(seconds=1),
        signal_interval=timedelta(seconds=60),
    )
    await ReplayRunner(
        event_bus=bus,
        scheduler=scheduler,
        tick_sink=capture_tick,
        speed=0,
    ).run([agg_trade(1, 1_000), agg_trade(2, 2_000)])

    assert observed_trade_counts == [
        (NOW + timedelta(seconds=1), 1),
        (NOW + timedelta(seconds=2), 2),
    ]


@pytest.mark.asyncio
async def test_compaction_preserves_every_row_and_reduces_files() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        compacted = temporary / "compacted"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        for sequence in range(5):
            await writer.write(agg_trade(sequence, sequence))
        await writer.close()

        report = compact_raw_tree(raw, compacted, target_rows=3)

        assert report.before.rows == report.after.rows == 5
        assert report.before.unique_record_ids == report.after.unique_record_ids == 5
        assert report.before.content_digest == report.after.content_digest
        assert report.after.files == 2
        assert report.after.files < report.before.files

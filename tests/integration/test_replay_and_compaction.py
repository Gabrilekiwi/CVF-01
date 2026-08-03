"""Raw scan, normalized replay, and lossless compaction integration."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from cvf import __version__
from cvf.clock import DecisionScheduler, DecisionTick, TickKind
from cvf.config import load_settings
from cvf.exchanges.binance import BinanceMarketDataConnector
from cvf.features import FeatureStatePipeline, MarketStateStore
from cvf.features.runtime import FeatureRuntime, ReceiveTimeFeatureDriver
from cvf.main import run_replay
from cvf.models import Exchange, LiquidationEvent, Trade
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline import NormalizedEventBus
from cvf.replay import (
    RawParquetReader,
    RawRecordNormalizer,
    ReplayOrder,
    ReplayRunner,
    ReplaySourceMode,
)
from cvf.storage import (
    AsyncPartitionedParquetWriter,
    RawMarketRecord,
    begin_collection_manifest,
    compact_raw_tree,
    complete_collection_manifest,
)
from cvf.storage.compact import audit_raw_tree
from cvf.storage.features import compare_feature_trees
from cvf.storage.raw import (
    feature_timeline_end_journal_record,
    feature_timeline_end_timestamp,
    normalized_event_journal_record,
)
from cvf.utils.fingerprint import settings_fingerprint

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


@contextmanager
def scratch_directory() -> Iterator[Path]:
    path = Path("data/processed") / f"phase25-{uuid4().hex[:8]}"
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


def test_okx_replay_filters_unconfigured_liquidation_instruments() -> None:
    normalizer = RawRecordNormalizer()
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
    assert normalizer.normalize(metadata) == []

    payload = json.loads(
        Path("tests/fixtures/okx/liquidation_official.json").read_text(encoding="utf-8")
    )
    unrelated = dict(payload["data"][0])
    unrelated["instId"] = "O-USDT-SWAP"
    payload["data"].append(unrelated)
    record = RawMarketRecord(
        exchange=Exchange.OKX,
        symbol="*",
        channel="liquidation-orders",
        message_kind="market_data",
        transport="websocket",
        exchange_timestamp=NOW,
        local_receive_timestamp=NOW,
        connection_generation=1,
        raw_payload=json.dumps(payload, separators=(",", ":")).encode(),
    )

    events = normalizer.normalize(record)

    assert len(events) == 1
    assert isinstance(events[0], LiquidationEvent)
    assert events[0].symbol == "BTC-USDT-PERP"


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
async def test_external_feature_timeline_uses_first_normalized_event() -> None:
    bus = NormalizedEventBus(default_queue_capacity=2)
    observed: list[NormalizedMarketEvent] = []

    async def capture(event: NormalizedMarketEvent) -> None:
        observed.append(event)

    control = RawMarketRecord(
        exchange=Exchange.BINANCE,
        symbol="*",
        channel="server_time",
        message_kind="control",
        transport="rest",
        local_receive_timestamp=NOW - timedelta(days=1),
        connection_generation=0,
        raw_payload=b"{}",
    )
    market = agg_trade(1, 1_000)
    summary = await ReplayRunner(
        event_bus=bus,
        event_sink=capture,
        order=ReplayOrder.RECEIVE_TIME,
        speed=0,
    ).run([control, market])

    assert len(observed) == 1
    assert summary.started_at == market.local_receive_timestamp
    assert summary.finished_at == market.local_receive_timestamp


@pytest.mark.asyncio
async def test_external_feature_timeline_end_marker_round_trips_to_finish_sink() -> None:
    bus = NormalizedEventBus(default_queue_capacity=2)
    observed: list[NormalizedMarketEvent] = []
    finished: list[datetime | None] = []
    terminal = NOW + timedelta(seconds=2)
    marker = feature_timeline_end_journal_record(terminal)

    async def capture(event: NormalizedMarketEvent) -> None:
        observed.append(event)

    async def finish(timestamp: datetime | None) -> None:
        finished.append(timestamp)

    summary = await ReplayRunner(
        event_bus=bus,
        event_sink=capture,
        finish_sink=finish,
        order=ReplayOrder.RECEIVE_TIME,
        speed=0,
    ).run([agg_trade(1, 1_000), marker])

    assert feature_timeline_end_timestamp(marker) == terminal
    assert len(observed) == 1
    assert finished == [terminal]
    assert summary.finished_at == terminal
    assert summary.feature_timeline_end_at == terminal
    assert summary.feature_timeline_end_records == 1


@pytest.mark.asyncio
async def test_external_feature_timeline_without_end_marker_reports_none() -> None:
    bus = NormalizedEventBus(default_queue_capacity=2)
    finished: list[datetime | None] = []
    market = agg_trade(1, 1_000)

    async def capture(_: NormalizedMarketEvent) -> None:
        return

    async def finish(timestamp: datetime | None) -> None:
        finished.append(timestamp)

    summary = await ReplayRunner(
        event_bus=bus,
        event_sink=capture,
        finish_sink=finish,
        order=ReplayOrder.RECEIVE_TIME,
        speed=0,
    ).run([market])

    assert finished == [None]
    assert summary.finished_at == market.local_receive_timestamp
    assert summary.feature_timeline_end_at is None
    assert summary.feature_timeline_end_records == 0


@pytest.mark.asyncio
async def test_external_feature_timeline_rejects_multiple_end_markers() -> None:
    bus = NormalizedEventBus(default_queue_capacity=2)
    terminal = NOW + timedelta(seconds=2)
    markers = [
        feature_timeline_end_journal_record(terminal),
        feature_timeline_end_journal_record(terminal),
    ]

    async def capture(_: NormalizedMarketEvent) -> None:
        return

    with pytest.raises(
        ValueError,
        match="multiple feature-timeline end markers",
    ):
        await ReplayRunner(
            event_bus=bus,
            event_sink=capture,
            order=ReplayOrder.RECEIVE_TIME,
            speed=0,
        ).run(markers)


@pytest.mark.asyncio
async def test_external_feature_timeline_rejects_event_beyond_end_marker() -> None:
    bus = NormalizedEventBus(default_queue_capacity=2)
    terminal = NOW + timedelta(seconds=1)
    market = agg_trade(1, 2_000)

    async def capture(_: NormalizedMarketEvent) -> None:
        return

    with pytest.raises(ValueError, match="exceeds its feature-timeline"):
        await ReplayRunner(
            event_bus=bus,
            event_sink=capture,
            order=ReplayOrder.RECEIVE_TIME,
            speed=0,
        ).run([market, feature_timeline_end_journal_record(terminal)])


@pytest.mark.asyncio
async def test_reader_orders_one_physically_unsorted_file_across_read_batches() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=5,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        for sequence in (5, 1, 4, 2, 3):
            await writer.write(agg_trade(sequence, sequence))
        await writer.close()

        records = list(RawParquetReader(raw, batch_size=2).iter_records())

        assert [record.sequence_id for record in records] == [
            "1",
            "2",
            "3",
            "4",
            "5",
        ]


@pytest.mark.asyncio
async def test_reader_removes_external_sort_index_after_early_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=2,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(agg_trade(2, 2))
        await writer.write(agg_trade(1, 1))
        await writer.close()
        index_path = temporary / "sort-index.sqlite3"
        monkeypatch.setattr(
            RawParquetReader,
            "_temporary_index_path",
            staticmethod(lambda: index_path),
        )

        records = RawParquetReader(raw, batch_size=1).iter_records()
        assert next(records).sequence_id == "1"
        assert index_path.is_file()
        records.close()

        assert not index_path.exists()
        assert not Path(f"{index_path}-journal").exists()
        assert not Path(f"{index_path}-wal").exists()
        assert not Path(f"{index_path}-shm").exists()


@pytest.mark.asyncio
async def test_replay_failure_closes_external_sort_iterator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=2,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(agg_trade(2, 2))
        await writer.write(agg_trade(1, 1))
        await writer.close()
        index_path = temporary / "failed-replay-sort.sqlite3"
        monkeypatch.setattr(
            RawParquetReader,
            "_temporary_index_path",
            staticmethod(lambda: index_path),
        )

        async def fail_event_sink(_: NormalizedMarketEvent) -> None:
            raise RuntimeError("injected replay consumer failure")

        with pytest.raises(RuntimeError, match="injected replay consumer failure"):
            await ReplayRunner(
                event_bus=NormalizedEventBus(default_queue_capacity=2),
                event_sink=fail_event_sink,
                order=ReplayOrder.RECEIVE_TIME,
                speed=0,
            ).run(RawParquetReader(raw, batch_size=1).iter_records())

        assert not index_path.exists()
        assert not Path(f"{index_path}-journal").exists()
        assert not Path(f"{index_path}-wal").exists()
        assert not Path(f"{index_path}-shm").exists()


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
        eth_metadata = RawMarketRecord(
            exchange=Exchange.OKX,
            symbol="ETH-USDT-PERP",
            channel="instrument_metadata",
            message_kind="market_data",
            transport="rest",
            local_receive_timestamp=NOW + timedelta(milliseconds=1),
            connection_generation=0,
            raw_payload=Path("tests/fixtures/okx/instrument_eth_live.json").read_bytes(),
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
        await writer.write(eth_metadata)
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
        assert summary.raw_records == 3
        assert summary.skipped_records == 2


@pytest.mark.asyncio
async def test_replay_ticks_include_all_events_at_the_decision_boundary() -> None:
    class CountingEventBus(NormalizedEventBus):
        def __init__(self) -> None:
            super().__init__(default_queue_capacity=2)
            self.drain_calls = 0

        async def drain(self) -> None:
            self.drain_calls += 1
            await super().drain()

    bus = CountingEventBus()
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
    ).run(
        [
            agg_trade(1, 1_000),
            agg_trade(2, 1_500),
            agg_trade(3, 2_000),
        ]
    )

    assert observed_trade_counts == [
        (NOW + timedelta(seconds=1), 1),
        (NOW + timedelta(seconds=2), 3),
    ]
    assert bus.drain_calls == 2


@pytest.mark.asyncio
async def test_live_driver_and_standard_replay_persist_identical_features() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        live_features = temporary / "live-features"
        replay_features = temporary / "replay-features"
        settings = load_settings(environ={})
        in_progress = begin_collection_manifest(
            raw,
            started_at=NOW - timedelta(seconds=1),
            code_version=__version__,
            strategy_version=settings.app.strategy_version,
            settings_sha256=settings_fingerprint(settings),
        )
        raw_writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=10,
            flush_seconds=60,
            queue_capacity=10,
        )
        live_bus = NormalizedEventBus(default_queue_capacity=10)
        live_runtime = FeatureRuntime(settings, output_path=live_features)
        live_bus.register("feature-runtime", live_runtime.consume_event)
        live_driver = ReceiveTimeFeatureDriver(
            settings,
            event_bus=live_bus,
            runtime=live_runtime,
        )

        async def persist_and_publish(event: NormalizedMarketEvent) -> None:
            await raw_writer.write(normalized_event_journal_record(event))
            await live_driver.publish(event)

        connector = BinanceMarketDataConnector(
            settings.exchanges.binance,
            stale_after_ms=settings.health.stale_after_ms,
            raw_writer=raw_writer,
            event_sink=persist_and_publish,
            duplicate_cache_size=settings.health.duplicate_cache_size,
            duplicate_ttl_seconds=settings.health.duplicate_ttl_seconds,
        )
        payload = json.loads(
            Path("tests/fixtures/binance/agg_trade_live.json").read_text(
                encoding="utf-8"
            )
        )
        first_at = NOW
        first_milliseconds = int(first_at.timestamp() * 1000)
        payload["data"].update(
            {
                "E": first_milliseconds,
                "T": first_milliseconds,
                "a": 1,
                "f": 1,
                "l": 1,
            }
        )
        first_message = json.dumps(payload, separators=(",", ":"))
        second_payload = json.loads(first_message)
        second_milliseconds = int(
            (first_at + timedelta(seconds=1)).timestamp() * 1000
        )
        second_payload["data"].update(
            {
                "E": second_milliseconds,
                "T": second_milliseconds,
                "a": 2,
                "f": 2,
                "l": 2,
            }
        )
        second_message = json.dumps(second_payload, separators=(",", ":"))

        await raw_writer.start()
        await live_bus.start()
        await live_runtime.start()
        try:
            await connector.process_websocket_message(
                first_message,
                local_receive_timestamp=first_at,
            )
            await connector.process_websocket_message(
                first_message,
                local_receive_timestamp=first_at + timedelta(milliseconds=1),
            )
            await connector.process_websocket_message(
                second_message,
                local_receive_timestamp=first_at + timedelta(seconds=1),
            )
            terminal = first_at + timedelta(seconds=2)
            await live_driver.finish(through_timestamp=terminal)
            await raw_writer.write(
                feature_timeline_end_journal_record(terminal)
            )
        finally:
            await live_bus.close()
            await live_runtime.close()
            await raw_writer.close()
        complete_collection_manifest(
            raw,
            expected_run_id=in_progress.run_id,
            terminal_at=terminal + timedelta(seconds=1),
            feature_timeline_end_at=terminal,
            normalized_event_count=2,
            raw_audit=audit_raw_tree(raw),
        )

        assert live_runtime.stats.normalized_events == 2
        assert audit_raw_tree(raw).rows == 6
        assert RawParquetReader(raw).has_channel("_normalized_event")

        result = await run_replay(
            settings,
            input_path=raw,
            start=None,
            end=None,
            exchanges=None,
            symbols=None,
            channels=None,
            order=ReplayOrder.RECEIVE_TIME,
            source_mode=ReplaySourceMode.JOURNAL,
            speed=0,
            feature_output_path=replay_features,
        )

        assert result == 0
        report = compare_feature_trees(live_features, replay_features)
        assert report.identical
        assert report.left.rows == report.right.rows == 36


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


@pytest.mark.asyncio
async def test_raw_audit_detects_duplicate_ids_across_physical_files() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=2,
        )
        duplicate = agg_trade(1, 1)
        await writer.start()
        await writer.write(duplicate)
        await writer.write(duplicate)
        await writer.close()

        with pytest.raises(ValueError, match="duplicate raw record_id"):
            audit_raw_tree(raw)

"""Collection orchestration without external network access."""

from __future__ import annotations

import asyncio
import logging
import shutil
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from cvf.collector import MarketDataCollector
from cvf.config import Settings, load_settings
from cvf.models import AggressorSide, Exchange, Trade
from cvf.replay import RawParquetReader
from cvf.storage import CollectionTerminal, read_collection_manifest

NOW = datetime(2026, 7, 31, 12, 0, tzinfo=UTC)


@contextmanager
def scratch_directory():
    path = Path("data/processed") / f"collector-{uuid4().hex[:8]}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def collection_settings() -> Settings:
    settings = load_settings(environ={})
    exchanges = settings.exchanges.model_copy(
        update={
            "binance": settings.exchanges.binance.model_copy(
                update={"enabled": False}
            ),
            "okx": settings.exchanges.okx.model_copy(update={"enabled": False}),
        }
    )
    return settings.model_copy(update={"exchanges": exchanges})


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
async def test_duration_stops_and_closes_empty_collection_cleanly() -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)
        stop_event = asyncio.Event()

        summary = await collector.run(
            stop_event=stop_event,
            duration_seconds=0.01,
        )

        assert stop_event.is_set()
        assert summary.output_path == output.resolve()
        assert summary.duration_seconds >= 0
        assert summary.normalized_event_counts == {}
        assert summary.parquet.written_records == 1
        assert summary.parquet.last_error is None
        assert summary.feature_output_path is None
        assert summary.feature_runtime is None
        assert summary.resources.initial_rss_bytes > 0
        assert (
            summary.resources.peak_rss_bytes
            >= summary.resources.initial_rss_bytes
        )
        manifest = read_collection_manifest(output)
        assert manifest == summary.collection_manifest
        assert manifest.terminal is CollectionTerminal.CLEAN_END
        assert manifest.normalized_event_count == 0
        assert manifest.raw_audit is not None
        assert manifest.raw_audit.rows == 1
        assert len(list(RawParquetReader(output).iter_records())) == 1


@pytest.mark.asyncio
async def test_status_log_uses_non_reserved_resource_metrics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)

        with caplog.at_level(logging.INFO, logger="cvf.collector"):
            await collector.run(
                stop_event=asyncio.Event(),
                duration_seconds=0.01,
            )

        status_records = [
            record
            for record in caplog.records
            if getattr(record, "event", None) == "collection_status"
        ]
        assert status_records
        metrics = status_records[0].resource_metrics
        assert metrics["rss_bytes"] > 0
        assert metrics["process_cpu_seconds"] >= 0


@pytest.mark.asyncio
async def test_feature_runtime_starts_and_closes_with_collection() -> None:
    with scratch_directory() as output:
        feature_output = output / "f"
        collector = MarketDataCollector(
            collection_settings(),
            output_path=output / "r",
            feature_output_path=feature_output,
        )

        summary = await collector.run(
            stop_event=asyncio.Event(),
            duration_seconds=0.01,
        )

        assert summary.feature_output_path == feature_output.resolve()
        assert summary.feature_runtime is not None
        assert summary.feature_runtime.normalized_events == 0
        assert summary.feature_runtime.writer.last_error is None
        assert (feature_output / "feature_schema=v1").is_dir()


def test_rejects_overlapping_raw_and_feature_outputs() -> None:
    with scratch_directory() as output:
        raw = output / "raw"
        with pytest.raises(ValueError, match="disjoint"):
            MarketDataCollector(
                collection_settings(),
                output_path=raw,
                feature_output_path=raw / "features",
            )


@pytest.mark.asyncio
async def test_rejects_nonpositive_duration_before_starting() -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)

        with pytest.raises(ValueError, match="positive"):
            await collector.run(
                stop_event=asyncio.Event(),
                duration_seconds=0,
            )


@pytest.mark.asyncio
async def test_startup_failure_closes_every_started_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)

        async def fail_writer_start() -> None:
            raise RuntimeError("injected raw writer startup failure")

        monkeypatch.setattr(collector._writer, "start", fail_writer_start)

        with pytest.raises(RuntimeError, match="injected raw writer startup failure"):
            await collector.run(
                stop_event=asyncio.Event(),
                duration_seconds=0.01,
            )

        assert collector._event_bus._closed
        assert collector._writer._closed
        assert (
            read_collection_manifest(output).terminal
            is CollectionTerminal.IN_PROGRESS
        )


@pytest.mark.asyncio
async def test_repeated_cancellation_cannot_interrupt_collection_shutdown() -> None:
    class BlockingDisconnectConnector:
        exchange = Exchange.BINANCE

        def __init__(self) -> None:
            self.connected = asyncio.Event()
            self.disconnect_entered = asyncio.Event()
            self.release_disconnect = asyncio.Event()
            self.stopped = asyncio.Event()

        async def connect(self) -> None:
            self.connected.set()

        async def wait(self) -> None:
            await self.stopped.wait()

        async def disconnect(self) -> None:
            self.disconnect_entered.set()
            await self.release_disconnect.wait()
            self.stopped.set()

        def health_snapshots(self, *, now: object) -> list[object]:
            return []

    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)
        connector = BlockingDisconnectConnector()
        collector._connectors = [connector]  # type: ignore[list-item]
        run_task = asyncio.create_task(
            collector.run(stop_event=asyncio.Event()),
        )
        await asyncio.wait_for(connector.connected.wait(), timeout=1)

        run_task.cancel()
        await asyncio.wait_for(connector.disconnect_entered.wait(), timeout=1)
        run_task.cancel()
        connector.release_disconnect.set()

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(run_task), timeout=2)

        assert collector._writer._closed
        assert collector._event_bus._closed
        assert all(
            runtime.task is not None and runtime.task.done()
            for runtime in collector._event_bus._consumers.values()
        )
        assert connector.stopped.is_set()
        assert (
            read_collection_manifest(output).terminal
            is CollectionTerminal.IN_PROGRESS
        )


@pytest.mark.asyncio
async def test_repeated_cancellation_cannot_split_event_journal_and_publish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)
        publish_entered = asyncio.Event()
        release_publish = asyncio.Event()
        published: list[Trade] = []
        event = trade(1)

        async def blocking_publish(pending_event: Trade) -> None:
            publish_entered.set()
            await release_publish.wait()
            published.append(pending_event)

        monkeypatch.setattr(collector._event_bus, "publish", blocking_publish)
        await collector._writer.start()
        recording = asyncio.create_task(collector._record_event(event))
        await asyncio.wait_for(publish_entered.wait(), timeout=1)

        recording.cancel()
        await asyncio.sleep(0)
        recording.cancel()
        await asyncio.sleep(0)

        assert not recording.done()

        release_publish.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(asyncio.shield(recording), timeout=1)
        await collector._writer.close()

        assert published == [event]
        assert collector.writer_stats.accepted_records == 1
        assert collector.writer_stats.written_records == 1
        assert collector._event_counts == {"TRADE": 1}

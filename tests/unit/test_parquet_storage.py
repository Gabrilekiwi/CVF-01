"""Lossless, partitioned, asynchronous raw Parquet persistence tests."""

from __future__ import annotations

import asyncio
import shutil
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

import cvf.storage.parquet as parquet_storage
from cvf.models import Exchange
from cvf.storage import (
    AsyncPartitionedParquetWriter,
    ParquetWriterError,
    RawMarketRecord,
)

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


@contextmanager
def scratch_directory() -> Iterator[Path]:
    path = Path("data/processed") / f"pq-{uuid4().hex[:8]}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def raw_record(
    *,
    exchange: Exchange = Exchange.BINANCE,
    symbol: str = "BTC-USDT-PERP",
    channel: str = "trades",
    payload: bytes = b'{"price":"1"}',
    offset_ms: int = 0,
) -> RawMarketRecord:
    received_at = NOW + timedelta(milliseconds=offset_ms)
    return RawMarketRecord(
        exchange=exchange,
        symbol=symbol,
        channel=channel,
        message_kind="market_data",
        transport="websocket",
        exchange_timestamp=received_at - timedelta(milliseconds=10),
        local_receive_timestamp=received_at,
        normalization_timestamp=received_at + timedelta(milliseconds=1),
        sequence_id=f"seq-{offset_ms}",
        connection_generation=2,
        raw_payload=payload,
    )


@pytest.mark.asyncio
async def test_batches_lossless_bytes_into_hive_style_partitions() -> None:
    with scratch_directory() as temporary:
        root = temporary / "raw"
        backpressure: list[str] = []
        writer = AsyncPartitionedParquetWriter(
            root_path=root,
            batch_rows=2,
            flush_seconds=60,
            queue_capacity=1,
            on_backpressure=lambda record: backpressure.append(record.channel),
        )
        first = raw_record(payload=b"\xff\x00exact")
        second = raw_record(offset_ms=1)
        third = raw_record(
            exchange=Exchange.OKX,
            symbol="ETH-USDT-PERP",
            channel="books5",
            offset_ms=2,
        )

        await writer.start()
        await writer.write(first)
        await writer.write(second)
        await writer.write(third)
        await writer.close()

        files = sorted(root.rglob("*.parquet"))
        assert len(files) == 2
        assert any(
            "date=2026-07-24"
            in str(path)
            and "exchange=BINANCE" in str(path)
            and "symbol=BTC-USDT-PERP" in str(path)
            and "channel=trades" in str(path)
            for path in files
        )
        rows: list[dict[str, object]] = []
        for path in files:
            with path.open("rb") as source:
                rows.extend(pq.read_table(source).to_pylist())

        by_id = {row["record_id"]: row for row in rows}
        first_row = by_id[str(first.record_id)]
        assert first_row["raw_payload"] == b"\xff\x00exact"
        assert first_row["raw_payload_reference"] == first.raw_payload_reference
        assert first_row["connection_generation"] == 2
        assert len(backpressure) >= 1
        assert writer.stats.accepted_records == 3
        assert writer.stats.written_records == 3
        assert writer.stats.written_files == 2
        assert writer.stats.backpressure_events == len(backpressure)
        assert writer.stats.queue_depth == 0
        assert writer.stats.last_error is None


@pytest.mark.asyncio
async def test_flushes_partial_batch_on_interval() -> None:
    with scratch_directory() as temporary:
        writer = AsyncPartitionedParquetWriter(
            root_path=temporary / "raw",
            batch_rows=100,
            flush_seconds=0.01,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(raw_record())

        async def wait_until_written() -> None:
            while writer.stats.written_records == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_until_written(), timeout=1.0)
        assert writer.stats.written_records == 1
        assert writer.stats.flush_count == 1
        await writer.close()


@pytest.mark.asyncio
async def test_close_is_idempotent_and_rejects_late_writes() -> None:
    with scratch_directory() as temporary:
        writer = AsyncPartitionedParquetWriter(
            root_path=temporary / "raw",
            batch_rows=10,
            flush_seconds=10,
            queue_capacity=10,
        )

        with pytest.raises(ParquetWriterError, match="start"):
            await writer.write(raw_record())

        await writer.start()
        await writer.close()
        await writer.close()

        with pytest.raises(ParquetWriterError, match="after close"):
            await writer.write(raw_record())
        with pytest.raises(ParquetWriterError, match="restart"):
            await writer.start()


@pytest.mark.asyncio
async def test_cancelled_full_queue_write_has_no_orphan_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        write_started = threading.Event()
        release_write = threading.Event()
        original_write = parquet_storage._write_partition_file

        def block_first_write(
            root: Path,
            partition: tuple[str, str, str, str],
            records: list[RawMarketRecord],
        ) -> Path:
            if not write_started.is_set():
                write_started.set()
                if not release_write.wait(timeout=5):
                    raise TimeoutError("test did not release raw write")
            return original_write(root, partition, records)

        monkeypatch.setattr(
            parquet_storage,
            "_write_partition_file",
            block_first_write,
        )
        root = temporary / "raw"
        writer = AsyncPartitionedParquetWriter(
            root_path=root,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=1,
        )
        first = raw_record(offset_ms=101)
        second = raw_record(offset_ms=102)
        cancelled = raw_record(offset_ms=103)

        await writer.start()
        await writer.write(first)
        assert await asyncio.to_thread(write_started.wait, 2)
        await writer.write(second)
        pending = asyncio.create_task(writer.write(cancelled))
        while writer.stats.backpressure_events == 0:
            await asyncio.sleep(0)

        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert writer.stats.accepted_records == 2
        assert writer.stats.queue_depth == 1

        release_write.set()
        await writer.close()

        rows = [
            row
            for path in root.rglob("*.parquet")
            for row in pq.ParquetFile(path).read().to_pylist()
        ]
        assert {row["record_id"] for row in rows} == {
            str(first.record_id),
            str(second.record_id),
        }
        assert writer.stats.accepted_records == writer.stats.written_records == 2
        assert writer.stats.queue_depth == 0


@pytest.mark.asyncio
async def test_cancelled_close_waits_for_shared_raw_close_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        write_started = threading.Event()
        release_write = threading.Event()
        original_write = parquet_storage._write_partition_file

        def block_write(
            root: Path,
            partition: tuple[str, str, str, str],
            records: list[RawMarketRecord],
        ) -> Path:
            write_started.set()
            if not release_write.wait(timeout=5):
                raise TimeoutError("test did not release raw write")
            return original_write(root, partition, records)

        monkeypatch.setattr(parquet_storage, "_write_partition_file", block_write)
        writer = AsyncPartitionedParquetWriter(
            root_path=temporary / "raw",
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=2,
        )
        await writer.start()
        await writer.write(raw_record(offset_ms=111))
        assert await asyncio.to_thread(write_started.wait, 2)

        cancelled_close = asyncio.create_task(writer.close())
        while writer._close_task is None:
            await asyncio.sleep(0)
        saved_close_task = writer._close_task
        concurrent_close = asyncio.create_task(writer.close())
        cancelled_close.cancel()
        await asyncio.sleep(0)

        assert not cancelled_close.done()
        assert writer._close_task is saved_close_task
        assert writer._closed is False
        assert writer._task is not None and not writer._task.done()

        release_write.set()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_close
        await concurrent_close

        assert writer._closed is True
        assert saved_close_task.done()
        assert writer._task.done()
        assert writer.stats.accepted_records == writer.stats.written_records == 1


@pytest.mark.asyncio
async def test_cancelled_raw_worker_is_reported_as_writer_failure() -> None:
    with scratch_directory() as temporary:
        writer = AsyncPartitionedParquetWriter(
            root_path=temporary / "raw",
            batch_rows=10,
            flush_seconds=60,
            queue_capacity=2,
        )
        await writer.start()
        assert writer._task is not None
        writer._task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await writer._task

        with pytest.raises(ParquetWriterError, match="worker task was cancelled"):
            await writer.close()


def test_raw_record_requires_aware_timestamps_and_has_stable_reference() -> None:
    record = raw_record()
    assert record.raw_payload_reference == f"raw://{record.record_id}"
    assert record.raw_payload_reference == record.raw_payload_reference

    with pytest.raises(ValueError, match="timezone-aware"):
        RawMarketRecord(
            exchange=Exchange.OKX,
            symbol="BTC-USDT-PERP",
            channel="trades",
            message_kind="market_data",
            transport="websocket",
            local_receive_timestamp=datetime(2026, 7, 24, 12, 0),
            raw_payload=b"{}",
        )

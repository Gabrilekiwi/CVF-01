"""Lossless, partitioned, asynchronous raw Parquet persistence tests."""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pyarrow.parquet as pq
import pytest

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

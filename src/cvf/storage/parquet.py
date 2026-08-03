"""Asynchronous, bounded, partitioned Parquet persistence for raw payloads."""

from __future__ import annotations

import asyncio
import os
from collections import defaultdict
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final
from urllib.parse import quote
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from cvf.storage.raw import RawMarketRecord
from cvf.utils.async_lifecycle import await_task_completion

_STOP: Final = object()

RAW_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("schema_version", pa.int16(), nullable=False),
        pa.field("record_id", pa.string(), nullable=False),
        pa.field("raw_payload_reference", pa.string(), nullable=False),
        pa.field("exchange", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("channel", pa.string(), nullable=False),
        pa.field("message_kind", pa.string(), nullable=False),
        pa.field("transport", pa.string(), nullable=False),
        pa.field("exchange_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field(
            "local_receive_timestamp",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
        ),
        pa.field("normalization_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("sequence_id", pa.string()),
        pa.field("connection_generation", pa.int64(), nullable=False),
        pa.field("raw_payload", pa.binary(), nullable=False),
    ]
)


class ParquetWriterError(RuntimeError):
    """Raised for invalid lifecycle operations or a failed worker."""


@dataclass(frozen=True, slots=True)
class ParquetWriterStats:
    accepted_records: int
    written_records: int
    written_files: int
    flush_count: int
    backpressure_events: int
    queue_depth: int
    last_file: Path | None
    last_error: str | None


@dataclass(slots=True)
class _RawSubmission:
    record: RawMarketRecord
    enqueued: bool = False


def _partition_value(value: str) -> str:
    return quote(value, safe="-_.")


def _utc(value: datetime | None) -> datetime | None:
    return None if value is None else value.astimezone(UTC)


def _native_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name == "nt" and not resolved.startswith("\\\\?\\"):
        return f"\\\\?\\{resolved}"
    return resolved


def _row(record: RawMarketRecord) -> dict[str, object]:
    return {
        "schema_version": 1,
        "record_id": str(record.record_id),
        "raw_payload_reference": record.raw_payload_reference,
        "exchange": record.exchange.value,
        "symbol": record.symbol,
        "channel": record.channel,
        "message_kind": record.message_kind,
        "transport": record.transport,
        "exchange_timestamp": _utc(record.exchange_timestamp),
        "local_receive_timestamp": _utc(record.local_receive_timestamp),
        "normalization_timestamp": _utc(record.normalization_timestamp),
        "sequence_id": None if record.sequence_id is None else str(record.sequence_id),
        "connection_generation": record.connection_generation,
        "raw_payload": record.raw_payload,
    }


def _write_partition_file(
    root: Path,
    partition: tuple[str, str, str, str],
    records: Sequence[RawMarketRecord],
) -> Path:
    date, exchange, symbol, channel = partition
    directory = (
        root
        / f"date={_partition_value(date)}"
        / f"exchange={_partition_value(exchange)}"
        / f"symbol={_partition_value(symbol)}"
        / f"channel={_partition_value(channel)}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    first_at = records[0].local_receive_timestamp.astimezone(UTC)
    filename = f"part-{first_at:%Y%m%dT%H%M%S.%fZ}-{uuid4().hex[:16]}.parquet"
    destination = directory / filename
    temporary = directory / f".{filename}.tmp"
    table = pa.Table.from_pylist([_row(record) for record in records], schema=RAW_PARQUET_SCHEMA)
    try:
        pq.write_table(
            table,
            _native_path(temporary),
            compression="zstd",
            use_dictionary=["exchange", "symbol", "channel", "message_kind", "transport"],
            write_statistics=True,
        )
        os.replace(_native_path(temporary), _native_path(destination))
    finally:
        if os.path.exists(_native_path(temporary)):
            os.unlink(_native_path(temporary))
    return destination


class AsyncPartitionedParquetWriter:
    """Batch exact payloads off-loop into atomic partition files."""

    def __init__(
        self,
        *,
        root_path: Path,
        batch_rows: int,
        flush_seconds: float,
        queue_capacity: int,
        on_backpressure: Callable[[RawMarketRecord], None] | None = None,
    ) -> None:
        if batch_rows < 1:
            raise ValueError("batch_rows must be positive")
        if flush_seconds <= 0:
            raise ValueError("flush_seconds must be positive")
        if queue_capacity < 1:
            raise ValueError("queue_capacity must be positive")
        self._root_path = root_path
        self._batch_rows = batch_rows
        self._flush_seconds = flush_seconds
        self._queue: asyncio.Queue[_RawSubmission | object] = asyncio.Queue(
            maxsize=queue_capacity
        )
        self._on_backpressure = on_backpressure
        self._task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._closed = False
        self._accepted_records = 0
        self._written_records = 0
        self._written_files = 0
        self._flush_count = 0
        self._backpressure_events = 0
        self._last_file: Path | None = None
        self._last_error: str | None = None

    @property
    def stats(self) -> ParquetWriterStats:
        return ParquetWriterStats(
            accepted_records=self._accepted_records,
            written_records=self._written_records,
            written_files=self._written_files,
            flush_count=self._flush_count,
            backpressure_events=self._backpressure_events,
            queue_depth=self._queue.qsize(),
            last_file=self._last_file,
            last_error=self._last_error,
        )

    async def __aenter__(self) -> AsyncPartitionedParquetWriter:
        await self.start()
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: object,
    ) -> None:
        await self.close()

    async def start(self) -> None:
        if self._closed:
            raise ParquetWriterError("cannot restart a closed Parquet writer")
        if self._close_task is not None:
            raise ParquetWriterError("cannot start a closing Parquet writer")
        if self._task is None:
            self._root_path.mkdir(parents=True, exist_ok=True)
            self._task = asyncio.create_task(self._run(), name="raw-parquet-writer")

    def _raise_worker_failure(self) -> None:
        if self._task is None or not self._task.done():
            return
        if self._task.cancelled():
            raise ParquetWriterError("Parquet worker task was cancelled")
        error = self._task.exception()
        if error is not None:
            raise ParquetWriterError(f"Parquet worker failed: {error}") from error

    async def _put_or_worker_failure(self, item: _RawSubmission | object) -> None:
        worker = self._task
        if worker is None:
            raise ParquetWriterError("Parquet writer is not running")
        if not self._queue.full():
            self._queue.put_nowait(item)
            if isinstance(item, _RawSubmission):
                item.enqueued = True
            return
        putter = asyncio.create_task(self._queue.put(item))
        try:
            done, _ = await asyncio.wait(
                {putter, worker},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            if not putter.done():
                putter.cancel()
                with suppress(asyncio.CancelledError):
                    await putter
            elif not putter.cancelled() and putter.exception() is None:
                if isinstance(item, _RawSubmission):
                    item.enqueued = True
            raise
        if worker in done:
            if not putter.done():
                putter.cancel()
                with suppress(asyncio.CancelledError):
                    await putter
            elif not putter.cancelled() and putter.exception() is None:
                if isinstance(item, _RawSubmission):
                    item.enqueued = True
            self._raise_worker_failure()
            if item is _STOP and not putter.cancelled() and putter.exception() is None:
                return
            raise ParquetWriterError("Parquet worker stopped unexpectedly")
        await putter
        if isinstance(item, _RawSubmission):
            item.enqueued = True

    async def write(self, record: RawMarketRecord) -> None:
        if self._closed:
            raise ParquetWriterError("cannot write after close")
        if self._close_task is not None:
            raise ParquetWriterError("cannot write after close begins")
        if self._task is None:
            raise ParquetWriterError("start the Parquet writer before writing")
        self._raise_worker_failure()
        if self._queue.full():
            self._backpressure_events += 1
            if self._on_backpressure is not None:
                self._on_backpressure(record)
        submission = _RawSubmission(record)
        try:
            await self._put_or_worker_failure(submission)
        except BaseException:
            if submission.enqueued:
                self._accepted_records += 1
            raise
        self._accepted_records += 1

    async def close(self) -> None:
        close_task = self._close_task
        if close_task is None:
            close_task = asyncio.create_task(
                self._close_lifecycle(),
                name="raw-parquet-writer-close",
            )
            self._close_task = close_task
        await await_task_completion(close_task)

    async def _close_lifecycle(self) -> None:
        if self._task is None:
            self._closed = True
            return
        try:
            self._raise_worker_failure()
            await self._put_or_worker_failure(_STOP)
            await self._task
        except ParquetWriterError:
            raise
        except Exception as exc:
            raise ParquetWriterError(f"Parquet worker failed: {exc}") from exc
        finally:
            self._closed = True

    async def _flush(self, records: list[RawMarketRecord]) -> None:
        if not records:
            return
        grouped: dict[tuple[str, str, str, str], list[RawMarketRecord]] = defaultdict(list)
        for record in records:
            received_at = record.local_receive_timestamp.astimezone(UTC)
            partition = (
                received_at.date().isoformat(),
                record.exchange.value,
                record.symbol,
                record.channel,
            )
            grouped[partition].append(record)
        for partition, partition_records in grouped.items():
            path = await asyncio.to_thread(
                _write_partition_file,
                self._root_path,
                partition,
                partition_records,
            )
            self._last_file = path
            self._written_files += 1
            self._written_records += len(partition_records)
        self._flush_count += 1

    async def _run(self) -> None:
        batch: list[RawMarketRecord] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._flush_seconds
        try:
            while True:
                timeout = max(0.0, deadline - loop.time())
                try:
                    item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
                except TimeoutError:
                    await self._flush(batch)
                    batch.clear()
                    deadline = loop.time() + self._flush_seconds
                    continue
                if item is _STOP:
                    await self._flush(batch)
                    return
                assert isinstance(item, _RawSubmission)
                batch.append(item.record)
                if len(batch) >= self._batch_rows:
                    await self._flush(batch)
                    batch.clear()
                    deadline = loop.time() + self._flush_seconds
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            raise

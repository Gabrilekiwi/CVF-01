"""Filtered, bounded reads over the raw hive-style Parquet tree."""

from __future__ import annotations

import os
import sqlite3
import tempfile
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import cast
from urllib.parse import quote
from uuid import UUID, uuid4

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from cvf.models.enums import Exchange
from cvf.replay.ordering import ReplayOrder, stable_record_key
from cvf.storage.parquet import RAW_PARQUET_SCHEMA
from cvf.storage.raw import RawMarketRecord


@dataclass(frozen=True, slots=True)
class RawScanFilter:
    start: datetime | None = None
    end: datetime | None = None
    exchanges: frozenset[Exchange] | None = None
    symbols: frozenset[str] | None = None
    channels: frozenset[str] | None = None
    excluded_channels: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        for value in (self.start, self.end):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError("raw scan timestamps must be timezone-aware")
        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError("raw scan end cannot precede start")
        if self.channels is not None and self.channels & self.excluded_channels:
            raise ValueError("raw scan channels and excluded_channels overlap")


class ReplaySourceMode(StrEnum):
    """Select legacy raw normalization or the post-dedup normalized journal."""

    AUTO = "auto"
    RAW = "raw"
    JOURNAL = "journal"


def _record_from_row(row: dict[str, object]) -> RawMarketRecord:
    return RawMarketRecord(
        record_id=UUID(str(row["record_id"])),
        exchange=Exchange(str(row["exchange"])),
        symbol=str(row["symbol"]),
        channel=str(row["channel"]),
        message_kind=str(row["message_kind"]),
        transport=str(row["transport"]),  # type: ignore[arg-type]
        exchange_timestamp=row["exchange_timestamp"],  # type: ignore[arg-type]
        local_receive_timestamp=row["local_receive_timestamp"],  # type: ignore[arg-type]
        normalization_timestamp=row["normalization_timestamp"],  # type: ignore[arg-type]
        sequence_id=None if row["sequence_id"] is None else str(row["sequence_id"]),
        connection_generation=int(str(row["connection_generation"])),
        raw_payload=bytes(cast(bytes, row["raw_payload"])),
    )


class RawParquetReader:
    """Scan and externally sort raw records without materializing every file."""

    def __init__(self, root_path: Path, *, batch_size: int = 4_096) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.root_path = root_path.resolve()
        self.batch_size = batch_size

    def has_channel(self, channel: str) -> bool:
        """Return whether the hive tree contains at least one channel partition."""

        partition_name = f"channel={quote(channel, safe='-_.')}"
        return any(
            candidate.is_dir()
            for candidate in self.root_path.rglob(partition_name)
        )

    def _matches(self, record: RawMarketRecord, filters: RawScanFilter) -> bool:
        timestamp = record.local_receive_timestamp.astimezone(UTC)
        if filters.start is not None and timestamp < filters.start.astimezone(UTC):
            return False
        if filters.end is not None and timestamp > filters.end.astimezone(UTC):
            return False
        if filters.exchanges is not None and record.exchange not in filters.exchanges:
            return False
        if filters.symbols is not None and record.symbol not in filters.symbols:
            return False
        if record.channel in filters.excluded_channels:
            return False
        return filters.channels is None or record.channel in filters.channels

    @staticmethod
    def _microseconds(value: datetime) -> int:
        utc_value = value.astimezone(UTC)
        delta = utc_value - datetime(1970, 1, 1, tzinfo=UTC)
        return (
            delta.days * 86_400_000_000
            + delta.seconds * 1_000_000
            + delta.microseconds
        )

    @staticmethod
    def _datetime(value: int | None) -> datetime | None:
        if value is None:
            return None
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
            microseconds=value
        )

    @staticmethod
    def _temporary_index_path() -> Path:
        parent = Path.cwd() / ".tmp"
        try:
            parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            parent = Path(tempfile.gettempdir())
        return parent / f"cvf-replay-sort-{uuid4().hex}.sqlite3"

    @staticmethod
    def _native_path(path: Path) -> str:
        resolved = str(path.resolve())
        if os.name == "nt" and not resolved.startswith("\\\\?\\"):
            return f"\\\\?\\{resolved}"
        return resolved

    @staticmethod
    def _create_sort_table(index: sqlite3.Connection) -> None:
        index.execute("PRAGMA journal_mode=OFF")
        index.execute("PRAGMA synchronous=OFF")
        index.execute("PRAGMA temp_store=FILE")
        index.execute("PRAGMA cache_size=-32768")
        index.execute(
            """
            CREATE TABLE replay_records (
                record_priority INTEGER NOT NULL,
                replay_timestamp_us INTEGER NOT NULL,
                local_receive_timestamp_us INTEGER NOT NULL,
                connection_generation INTEGER NOT NULL,
                exchange_key TEXT NOT NULL,
                symbol_key TEXT NOT NULL,
                channel_key TEXT NOT NULL,
                sequence_key TEXT NOT NULL,
                record_id TEXT NOT NULL,
                source_ordinal INTEGER NOT NULL,
                message_kind TEXT NOT NULL,
                transport TEXT NOT NULL,
                exchange_timestamp_us INTEGER,
                normalization_timestamp_us INTEGER,
                sequence_id TEXT,
                raw_payload BLOB NOT NULL,
                PRIMARY KEY (
                    record_priority,
                    replay_timestamp_us,
                    local_receive_timestamp_us,
                    connection_generation,
                    exchange_key,
                    symbol_key,
                    channel_key,
                    sequence_key,
                    record_id,
                    source_ordinal
                )
            ) WITHOUT ROWID
            """
        )

    def _sort_row(
        self,
        record: RawMarketRecord,
        order: ReplayOrder,
        ordinal: int,
    ) -> tuple[object, ...]:
        key = stable_record_key(record, order)
        return (
            key[0],
            self._microseconds(key[1]),
            self._microseconds(key[2]),
            key[3],
            key[4],
            key[5],
            key[6],
            key[7],
            key[8],
            ordinal,
            record.message_kind,
            record.transport,
            (
                None
                if record.exchange_timestamp is None
                else self._microseconds(record.exchange_timestamp)
            ),
            (
                None
                if record.normalization_timestamp is None
                else self._microseconds(record.normalization_timestamp)
            ),
            None if record.sequence_id is None else str(record.sequence_id),
            record.raw_payload,
        )

    def _externally_sorted_records(
        self,
        files: list[Path],
        filters: RawScanFilter,
        order: ReplayOrder,
    ) -> Generator[RawMarketRecord, None, None]:
        index_path = self._temporary_index_path()
        index: sqlite3.Connection | None = None
        cursor: sqlite3.Cursor | None = None
        try:
            index = sqlite3.connect(self._native_path(index_path))
            self._create_sort_table(index)
            ordinal = 0
            for path in files:
                parquet_file = pq.ParquetFile(path)
                try:
                    if parquet_file.schema_arrow != RAW_PARQUET_SCHEMA:
                        raise ValueError(f"raw Parquet schema mismatch: {path}")
                    for batch in parquet_file.iter_batches(batch_size=self.batch_size):
                        rows: list[tuple[object, ...]] = []
                        for row in batch.to_pylist():
                            schema_version = int(str(row["schema_version"]))
                            if schema_version != 1:
                                raise ValueError(
                                    "unsupported raw Parquet schema version "
                                    f"{schema_version}: {path}"
                                )
                            record = _record_from_row(row)
                            if not self._matches(record, filters):
                                continue
                            ordinal += 1
                            rows.append(self._sort_row(record, order, ordinal))
                        index.executemany(
                            """
                            INSERT INTO replay_records VALUES (
                                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                            )
                            """,
                            rows,
                        )
                        index.commit()
                finally:
                    parquet_file.close()
            cursor = index.execute(
                """
                SELECT
                    record_id,
                    exchange_key,
                    symbol_key,
                    channel_key,
                    message_kind,
                    transport,
                    exchange_timestamp_us,
                    local_receive_timestamp_us,
                    normalization_timestamp_us,
                    sequence_id,
                    connection_generation,
                    raw_payload
                FROM replay_records
                ORDER BY
                    record_priority,
                    replay_timestamp_us,
                    local_receive_timestamp_us,
                    connection_generation,
                    exchange_key,
                    symbol_key,
                    channel_key,
                    sequence_key,
                    record_id,
                    source_ordinal
                """
            )
            for row in cursor:
                yield RawMarketRecord(
                    record_id=UUID(str(row[0])),
                    exchange=Exchange(str(row[1])),
                    symbol=str(row[2]),
                    channel=str(row[3]),
                    message_kind=str(row[4]),
                    transport=str(row[5]),  # type: ignore[arg-type]
                    exchange_timestamp=self._datetime(row[6]),
                    local_receive_timestamp=cast(
                        datetime,
                        self._datetime(int(row[7])),
                    ),
                    normalization_timestamp=self._datetime(row[8]),
                    sequence_id=None if row[9] is None else str(row[9]),
                    connection_generation=int(row[10]),
                    raw_payload=bytes(cast(bytes, row[11])),
                )
        finally:
            if cursor is not None:
                cursor.close()
            if index is not None:
                index.close()
            for suffix in ("", "-journal", "-wal", "-shm"):
                Path(f"{index_path}{suffix}").unlink(missing_ok=True)

    def iter_records(
        self,
        *,
        filters: RawScanFilter | None = None,
        order: ReplayOrder = ReplayOrder.EVENT_TIME,
    ) -> Generator[RawMarketRecord, None, None]:
        selected = filters or RawScanFilter()
        files = sorted(self.root_path.rglob("*.parquet"))
        return self._externally_sorted_records(files, selected, order)

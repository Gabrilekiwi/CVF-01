"""Filtered, bounded reads over the raw hive-style Parquet tree."""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from uuid import UUID

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

    def __post_init__(self) -> None:
        for value in (self.start, self.end):
            if value is not None and (value.tzinfo is None or value.utcoffset() is None):
                raise ValueError("raw scan timestamps must be timezone-aware")
        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError("raw scan end cannot precede start")


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
    """Scan selected raw records and merge files into one deterministic stream."""

    def __init__(self, root_path: Path, *, batch_size: int = 65_536) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.root_path = root_path.resolve()
        self.batch_size = batch_size

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
        return filters.channels is None or record.channel in filters.channels

    def _file_records(
        self,
        path: Path,
        filters: RawScanFilter,
        order: ReplayOrder,
    ) -> Iterator[RawMarketRecord]:
        parquet_file = pq.ParquetFile(path)
        if parquet_file.schema_arrow != RAW_PARQUET_SCHEMA:
            raise ValueError(f"raw Parquet schema mismatch: {path}")

        def row_group_records(index: int) -> Iterator[RawMarketRecord]:
            records: list[RawMarketRecord] = []
            for batch in parquet_file.iter_batches(
                batch_size=self.batch_size,
                row_groups=[index],
            ):
                records.extend(
                    _record_from_row(row)
                    for row in batch.to_pylist()
                    if int(str(row["schema_version"])) == 1
                )
            records = [
                record for record in records if self._matches(record, filters)
            ]
            records.sort(key=lambda record: stable_record_key(record, order))
            yield from records

        row_groups = (
            row_group_records(index)
            for index in range(parquet_file.num_row_groups)
        )
        yield from heapq.merge(
            *row_groups,
            key=lambda record: stable_record_key(record, order),
        )

    def iter_records(
        self,
        *,
        filters: RawScanFilter | None = None,
        order: ReplayOrder = ReplayOrder.EVENT_TIME,
    ) -> Iterator[RawMarketRecord]:
        selected = filters or RawScanFilter()
        files = sorted(self.root_path.rglob("*.parquet"))
        iterators = [self._file_records(path, selected, order) for path in files]
        return heapq.merge(
            *iterators,
            key=lambda record: stable_record_key(record, order),
        )

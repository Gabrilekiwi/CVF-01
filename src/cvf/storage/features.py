"""Versioned, audited, deterministic Parquet persistence for feature snapshots."""

from __future__ import annotations

import asyncio
import heapq
import os
from collections import OrderedDict, defaultdict
from collections.abc import Callable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Final, cast
from urllib.parse import quote
from uuid import UUID, uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from cvf import __version__
from cvf.config import Settings
from cvf.features.models import CrossVenueFeatureSnapshot, FeatureSnapshot
from cvf.models.enums import EventType, Exchange
from cvf.utils.fingerprint import (
    canonical_json,
    model_payload_json,
    settings_fingerprint,
    sha256_text,
)
from cvf.utils.validation import validate_canonical_symbol

type PersistableFeatureSnapshot = FeatureSnapshot | CrossVenueFeatureSnapshot

_STOP: Final = object()
_DIGEST_MODULUS = 1 << 256
_SCHEMA_DIRECTORY = "feature_schema=v1"

FEATURE_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("feature_schema_version", pa.int16(), nullable=False),
        pa.field("feature_snapshot_id", pa.string(), nullable=False),
        pa.field("scope", pa.string(), nullable=False),
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("window_seconds", pa.int32(), nullable=False),
        pa.field("decision_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("calculation_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("exchange_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("local_receive_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("normalization_timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("strategy_version", pa.string(), nullable=False),
        pa.field("code_version", pa.string(), nullable=False),
        pa.field("config_hash", pa.string(), nullable=False),
        pa.field("sequence_id", pa.string()),
        pa.field("raw_payload_reference", pa.string()),
        pa.field("source_snapshot_ids", pa.list_(pa.string()), nullable=False),
        pa.field("source_sequence_id", pa.string()),
        pa.field("source_event_count", pa.int64(), nullable=False),
        pa.field("oldest_source_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("newest_source_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("data_age_ms", pa.float64()),
        pa.field("is_warm", pa.bool_(), nullable=False),
        pa.field("is_healthy", pa.bool_(), nullable=False),
        pa.field("unavailable_reason_codes", pa.list_(pa.string()), nullable=False),
        pa.field("book_generation", pa.int64()),
        pa.field("binance_book_generation", pa.int64()),
        pa.field("okx_book_generation", pa.int64()),
        pa.field("payload_sha256", pa.string(), nullable=False),
        pa.field("payload_json", pa.large_string(), nullable=False),
    ]
)


class FeatureParquetError(RuntimeError):
    """Raised for lifecycle, lineage, schema, or persistence failures."""


class FeatureWriteStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    DEDUPLICATED = "DEDUPLICATED"


@dataclass(frozen=True, slots=True)
class FeatureWriterStats:
    accepted_snapshots: int
    deduplicated_snapshots: int
    written_snapshots: int
    written_files: int
    flush_count: int
    backpressure_events: int
    queue_depth: int
    deduplication_cache_size: int
    last_file: Path | None
    last_error: str | None


@dataclass(frozen=True, slots=True)
class PersistedFeatureRecord:
    snapshot: PersistableFeatureSnapshot
    code_version: str
    config_hash: str
    payload_sha256: str
    source_file: Path

    @property
    def feature_snapshot_id(self) -> UUID:
        return self.snapshot.feature_snapshot_id

    @property
    def scope(self) -> Exchange:
        return self.snapshot.exchange


@dataclass(frozen=True, slots=True)
class FeatureScanFilter:
    start: datetime | None = None
    end: datetime | None = None
    scopes: frozenset[Exchange] | None = None
    symbols: frozenset[str] | None = None
    windows: frozenset[int] | None = None
    is_warm: bool | None = None
    is_healthy: bool | None = None

    def __post_init__(self) -> None:
        for value in (self.start, self.end):
            if value is not None and (
                value.tzinfo is None or value.utcoffset() is None
            ):
                raise ValueError("feature scan timestamps must be timezone-aware")
        if self.start is not None and self.end is not None and self.end < self.start:
            raise ValueError("feature scan end cannot precede start")
        if self.scopes is not None and not self.scopes <= {
            Exchange.BINANCE,
            Exchange.OKX,
            Exchange.CROSS_VENUE,
        }:
            raise ValueError("feature scan scopes must be feature-producing scopes")
        if self.symbols is not None:
            for symbol in self.symbols:
                validate_canonical_symbol(symbol)
        if self.windows is not None and any(window < 1 for window in self.windows):
            raise ValueError("feature scan windows must be positive")


@dataclass(frozen=True, slots=True)
class FeatureAudit:
    rows: int
    files: int
    unique_snapshot_ids: int
    partitions: int
    content_digest: str
    scopes: tuple[str, ...]
    code_versions: tuple[str, ...]
    config_hashes: tuple[str, ...]
    earliest_decision_timestamp: datetime | None
    latest_decision_timestamp: datetime | None


@dataclass(frozen=True, slots=True)
class FeatureConsistencyReport:
    left_path: Path
    right_path: Path
    left: FeatureAudit
    right: FeatureAudit
    identical: bool


@dataclass(frozen=True, slots=True)
class _FeatureEnvelope:
    snapshot: PersistableFeatureSnapshot
    code_version: str
    config_hash: str
    payload_json: str
    payload_sha256: str


def _utc(value: datetime | None) -> datetime | None:
    return None if value is None else value.astimezone(UTC)


def _partition_value(value: str) -> str:
    return quote(value, safe="-_.")


def _native_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name == "nt" and not resolved.startswith("\\\\?\\"):
        return f"\\\\?\\{resolved}"
    return resolved


def _schema_root(root: Path) -> Path:
    resolved = root.resolve()
    if resolved.name == _SCHEMA_DIRECTORY:
        return resolved
    return resolved / _SCHEMA_DIRECTORY


def _partition(
    snapshot: PersistableFeatureSnapshot,
) -> tuple[str, str, str]:
    return (
        snapshot.decision_timestamp.astimezone(UTC).date().isoformat(),
        snapshot.symbol,
        snapshot.exchange.value,
    )


def _partition_directory(
    root: Path,
    partition: tuple[str, str, str],
) -> Path:
    date, symbol, scope = partition
    return (
        _schema_root(root)
        / f"date={_partition_value(date)}"
        / f"symbol={_partition_value(symbol)}"
        / f"scope={_partition_value(scope)}"
    )


def _source_snapshot_ids(
    snapshot: PersistableFeatureSnapshot,
) -> list[str]:
    if isinstance(snapshot, CrossVenueFeatureSnapshot):
        return [str(value) for value in snapshot.source_snapshot_ids]
    return []


def _reason_codes(snapshot: PersistableFeatureSnapshot) -> list[str]:
    return [reason.code.value for reason in snapshot.unavailable_reasons]


def _row(envelope: _FeatureEnvelope) -> dict[str, object]:
    snapshot = envelope.snapshot
    return {
        "feature_schema_version": 1,
        "feature_snapshot_id": str(snapshot.feature_snapshot_id),
        "scope": snapshot.exchange.value,
        "symbol": snapshot.symbol,
        "window_seconds": snapshot.window_seconds,
        "decision_timestamp": _utc(snapshot.decision_timestamp),
        "calculation_timestamp": _utc(snapshot.calculation_timestamp),
        "exchange_timestamp": _utc(snapshot.exchange_timestamp),
        "local_receive_timestamp": _utc(snapshot.local_receive_timestamp),
        "normalization_timestamp": _utc(snapshot.normalization_timestamp),
        "strategy_version": snapshot.strategy_version,
        "code_version": envelope.code_version,
        "config_hash": envelope.config_hash,
        "sequence_id": (
            None if snapshot.sequence_id is None else str(snapshot.sequence_id)
        ),
        "raw_payload_reference": snapshot.raw_payload_reference,
        "source_snapshot_ids": _source_snapshot_ids(snapshot),
        "source_sequence_id": (
            None
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            or snapshot.source_sequence_id is None
            else str(snapshot.source_sequence_id)
        ),
        "source_event_count": snapshot.source_event_count,
        "oldest_source_timestamp": _utc(snapshot.oldest_source_timestamp),
        "newest_source_timestamp": _utc(snapshot.newest_source_timestamp),
        "data_age_ms": snapshot.data_age_ms,
        "is_warm": snapshot.is_warm,
        "is_healthy": snapshot.is_healthy,
        "unavailable_reason_codes": _reason_codes(snapshot),
        "book_generation": (
            None
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            else snapshot.book_generation
        ),
        "binance_book_generation": (
            snapshot.binance_book_generation
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            else None
        ),
        "okx_book_generation": (
            snapshot.okx_book_generation
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            else None
        ),
        "payload_sha256": envelope.payload_sha256,
        "payload_json": envelope.payload_json,
    }


def _stable_key(record: PersistedFeatureRecord) -> tuple[object, ...]:
    snapshot = record.snapshot
    return (
        snapshot.decision_timestamp,
        snapshot.symbol,
        snapshot.window_seconds,
        snapshot.exchange.value,
        snapshot.feature_snapshot_id.hex,
    )


def _write_partition_file(
    root: Path,
    partition: tuple[str, str, str],
    envelopes: Sequence[_FeatureEnvelope],
) -> Path:
    ordered = sorted(
        envelopes,
        key=lambda envelope: (
            envelope.snapshot.decision_timestamp,
            envelope.snapshot.window_seconds,
            envelope.snapshot.feature_snapshot_id.hex,
        ),
    )
    directory = _partition_directory(root, partition)
    directory.mkdir(parents=True, exist_ok=True)
    first = ordered[0].snapshot.decision_timestamp.astimezone(UTC)
    last = ordered[-1].snapshot.decision_timestamp.astimezone(UTC)
    content_key = sha256_text(
        canonical_json(
            [
                {
                    "id": envelope.snapshot.feature_snapshot_id.hex,
                    "payload_sha256": envelope.payload_sha256,
                    "config_hash": envelope.config_hash,
                    "code_version": envelope.code_version,
                }
                for envelope in ordered
            ]
        )
    )
    filename = (
        f"part-{first:%Y%m%dT%H%M%S.%fZ}-"
        f"{last:%Y%m%dT%H%M%S.%fZ}-{content_key[:16]}.parquet"
    )
    destination = directory / filename
    temporary = directory / f".{filename}.{uuid4().hex}.tmp"
    table = pa.Table.from_pylist(
        [_row(envelope) for envelope in ordered],
        schema=FEATURE_PARQUET_SCHEMA,
    )
    try:
        pq.write_table(
            table,
            _native_path(temporary),
            compression="zstd",
            use_dictionary=[
                "scope",
                "symbol",
                "strategy_version",
                "code_version",
                "config_hash",
            ],
            write_statistics=True,
        )
        os.replace(_native_path(temporary), _native_path(destination))
    finally:
        if os.path.exists(_native_path(temporary)):
            os.unlink(_native_path(temporary))
    return destination


class AsyncFeatureParquetWriter:
    """Bounded writer with atomic files and bounded snapshot-ID deduplication."""

    def __init__(
        self,
        *,
        root_path: Path,
        settings: Settings,
        batch_rows: int | None = None,
        flush_seconds: float | None = None,
        queue_capacity: int | None = None,
        deduplication_capacity: int | None = None,
        on_backpressure: Callable[[PersistableFeatureSnapshot], None] | None = None,
    ) -> None:
        self._root_path = root_path
        self._settings = settings
        self._batch_rows = (
            settings.storage.feature_parquet_batch_rows
            if batch_rows is None
            else batch_rows
        )
        self._flush_seconds = (
            settings.storage.feature_parquet_flush_seconds
            if flush_seconds is None
            else flush_seconds
        )
        capacity = (
            settings.storage.feature_parquet_queue_capacity
            if queue_capacity is None
            else queue_capacity
        )
        self._deduplication_capacity = (
            settings.storage.feature_deduplication_capacity
            if deduplication_capacity is None
            else deduplication_capacity
        )
        if self._batch_rows < 1:
            raise ValueError("feature batch_rows must be positive")
        if self._flush_seconds <= 0:
            raise ValueError("feature flush_seconds must be positive")
        if capacity < 1:
            raise ValueError("feature queue_capacity must be positive")
        if self._deduplication_capacity < 1:
            raise ValueError("feature deduplication_capacity must be positive")
        self._queue: asyncio.Queue[_FeatureEnvelope | object] = asyncio.Queue(
            maxsize=capacity
        )
        self._on_backpressure = on_backpressure
        self._task: asyncio.Task[None] | None = None
        self._closed = False
        self._accepted = 0
        self._deduplicated = 0
        self._written = 0
        self._written_files = 0
        self._flush_count = 0
        self._backpressure_events = 0
        self._last_file: Path | None = None
        self._last_error: str | None = None
        self._code_version = __version__
        self._config_hash = settings_fingerprint(settings)
        self._deduplication: OrderedDict[UUID, str] = OrderedDict()

    @property
    def stats(self) -> FeatureWriterStats:
        return FeatureWriterStats(
            accepted_snapshots=self._accepted,
            deduplicated_snapshots=self._deduplicated,
            written_snapshots=self._written,
            written_files=self._written_files,
            flush_count=self._flush_count,
            backpressure_events=self._backpressure_events,
            queue_depth=self._queue.qsize(),
            deduplication_cache_size=len(self._deduplication),
            last_file=self._last_file,
            last_error=self._last_error,
        )

    @property
    def config_hash(self) -> str:
        return self._config_hash

    async def __aenter__(self) -> AsyncFeatureParquetWriter:
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
            raise FeatureParquetError("cannot restart a closed feature writer")
        if self._task is None:
            _schema_root(self._root_path).mkdir(parents=True, exist_ok=True)
            self._task = asyncio.create_task(
                self._run(),
                name="feature-parquet-writer",
            )

    def _raise_worker_failure(self) -> None:
        if self._task is None or not self._task.done():
            return
        error = self._task.exception()
        if error is not None:
            raise FeatureParquetError(f"feature writer failed: {error}") from error

    async def _put_or_worker_failure(
        self,
        item: _FeatureEnvelope | object,
    ) -> None:
        worker = self._task
        if worker is None:
            raise FeatureParquetError("feature writer is not running")
        if not self._queue.full():
            self._queue.put_nowait(item)
            return
        putter = asyncio.create_task(self._queue.put(item))
        done, _ = await asyncio.wait(
            {putter, worker},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if worker in done:
            if not putter.done():
                putter.cancel()
                with suppress(asyncio.CancelledError):
                    await putter
            self._raise_worker_failure()
            if item is _STOP and not putter.cancelled() and putter.exception() is None:
                return
            raise FeatureParquetError("feature writer stopped unexpectedly")
        await putter

    def _envelope(
        self,
        snapshot: PersistableFeatureSnapshot,
    ) -> _FeatureEnvelope:
        if snapshot.event_type is not EventType.MARKET_FEATURE:
            raise FeatureParquetError("only market feature snapshots can be persisted")
        if isinstance(snapshot, CrossVenueFeatureSnapshot):
            if snapshot.exchange is not Exchange.CROSS_VENUE:
                raise FeatureParquetError("cross-venue snapshot has an invalid scope")
            if snapshot.code_version != self._code_version:
                raise FeatureParquetError("cross-venue code version differs from the writer")
            if snapshot.config_hash != self._config_hash:
                raise FeatureParquetError("cross-venue config hash differs from the writer")
        elif snapshot.exchange not in (Exchange.BINANCE, Exchange.OKX):
            raise FeatureParquetError("single-venue feature scope must be Binance or OKX")
        if snapshot.strategy_version != self._settings.app.strategy_version:
            raise FeatureParquetError("snapshot strategy version differs from the writer")
        payload = model_payload_json(snapshot)
        return _FeatureEnvelope(
            snapshot=snapshot,
            code_version=self._code_version,
            config_hash=self._config_hash,
            payload_json=payload,
            payload_sha256=sha256_text(payload),
        )

    async def write(
        self,
        snapshot: PersistableFeatureSnapshot,
    ) -> FeatureWriteStatus:
        if self._closed:
            raise FeatureParquetError("cannot write after close")
        if self._task is None:
            raise FeatureParquetError("start the feature writer before writing")
        self._raise_worker_failure()
        envelope = self._envelope(snapshot)
        snapshot_id = snapshot.feature_snapshot_id
        existing = self._deduplication.get(snapshot_id)
        if existing is not None:
            if existing != envelope.payload_sha256:
                raise FeatureParquetError(
                    "duplicate feature_snapshot_id has different content"
                )
            self._deduplication.move_to_end(snapshot_id)
            self._deduplicated += 1
            return FeatureWriteStatus.DEDUPLICATED
        self._deduplication[snapshot_id] = envelope.payload_sha256
        while len(self._deduplication) > self._deduplication_capacity:
            self._deduplication.popitem(last=False)
        if self._queue.full():
            self._backpressure_events += 1
            if self._on_backpressure is not None:
                self._on_backpressure(snapshot)
        try:
            await self._put_or_worker_failure(envelope)
        except Exception:
            if self._deduplication.get(snapshot_id) == envelope.payload_sha256:
                del self._deduplication[snapshot_id]
            raise
        self._accepted += 1
        return FeatureWriteStatus.ACCEPTED

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._task is None:
            return
        self._raise_worker_failure()
        await self._put_or_worker_failure(_STOP)
        try:
            await self._task
        except Exception as exc:
            raise FeatureParquetError(f"feature writer failed: {exc}") from exc

    async def _flush(self, envelopes: list[_FeatureEnvelope]) -> None:
        if not envelopes:
            return
        grouped: dict[
            tuple[str, str, str],
            list[_FeatureEnvelope],
        ] = defaultdict(list)
        for envelope in envelopes:
            grouped[_partition(envelope.snapshot)].append(envelope)
        for partition in sorted(grouped):
            partition_envelopes = grouped[partition]
            path = await asyncio.to_thread(
                _write_partition_file,
                self._root_path,
                partition,
                partition_envelopes,
            )
            self._last_file = path
            self._written_files += 1
            self._written += len(partition_envelopes)
        self._flush_count += 1

    async def _run(self) -> None:
        batch: list[_FeatureEnvelope] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._flush_seconds
        try:
            while True:
                timeout = max(0.0, deadline - loop.time())
                try:
                    item = await asyncio.wait_for(
                        self._queue.get(),
                        timeout=timeout,
                    )
                except TimeoutError:
                    await self._flush(batch)
                    batch.clear()
                    deadline = loop.time() + self._flush_seconds
                    continue
                if item is _STOP:
                    await self._flush(batch)
                    return
                assert isinstance(item, _FeatureEnvelope)
                batch.append(item)
                if len(batch) >= self._batch_rows:
                    await self._flush(batch)
                    batch.clear()
                    deadline = loop.time() + self._flush_seconds
        except Exception as exc:
            self._last_error = f"{type(exc).__name__}: {exc}"
            raise


def _model_from_payload(
    payload_json: str,
    scope: Exchange,
) -> PersistableFeatureSnapshot:
    if scope is Exchange.CROSS_VENUE:
        return CrossVenueFeatureSnapshot.model_validate_json(payload_json)
    if scope in (Exchange.BINANCE, Exchange.OKX):
        return FeatureSnapshot.model_validate_json(payload_json)
    raise ValueError(f"unsupported persisted feature scope: {scope.value}")


def _timestamp_equal(left: object, right: datetime) -> bool:
    return isinstance(left, datetime) and left.astimezone(UTC) == right.astimezone(UTC)


def _optional_timestamp_equal(
    left: object,
    right: datetime | None,
) -> bool:
    if right is None:
        return left is None
    return _timestamp_equal(left, right)


def _record_from_row(
    row: dict[str, object],
    path: Path,
    schema_root: Path,
) -> PersistedFeatureRecord:
    if int(str(row["feature_schema_version"])) != 1:
        raise ValueError(f"unsupported feature schema version in {path}")
    payload_json = str(row["payload_json"])
    payload_sha256 = str(row["payload_sha256"])
    if sha256_text(payload_json) != payload_sha256:
        raise ValueError(f"feature payload digest mismatch: {path}")
    scope = Exchange(str(row["scope"]))
    snapshot = _model_from_payload(payload_json, scope)
    if model_payload_json(snapshot) != payload_json:
        raise ValueError(f"feature payload is not canonical: {path}")
    code_version = str(row["code_version"])
    config_hash = str(row["config_hash"])
    if not code_version:
        raise ValueError(f"empty feature code version: {path}")
    if len(config_hash) != 64 or any(
        character not in "0123456789abcdef" for character in config_hash
    ):
        raise ValueError(f"invalid feature config hash: {path}")
    if isinstance(snapshot, CrossVenueFeatureSnapshot):
        if snapshot.code_version != code_version:
            raise ValueError(f"cross-venue code-version lineage mismatch: {path}")
        if snapshot.config_hash != config_hash:
            raise ValueError(f"cross-venue config lineage mismatch: {path}")
    expected_partition = _partition_directory(schema_root, _partition(snapshot))
    if path.parent != expected_partition:
        raise ValueError(f"feature row/partition mismatch: {path}")

    expected_source_ids = _source_snapshot_ids(snapshot)
    actual_source_ids = [
        str(value)
        for value in cast(list[object], row["source_snapshot_ids"])
    ]
    expected_source_sequence = (
        None
        if isinstance(snapshot, CrossVenueFeatureSnapshot)
        or snapshot.source_sequence_id is None
        else str(snapshot.source_sequence_id)
    )
    checks = (
        str(row["feature_snapshot_id"]) == str(snapshot.feature_snapshot_id),
        scope is snapshot.exchange,
        str(row["symbol"]) == snapshot.symbol,
        int(str(row["window_seconds"])) == snapshot.window_seconds,
        _timestamp_equal(row["decision_timestamp"], snapshot.decision_timestamp),
        _timestamp_equal(
            row["calculation_timestamp"],
            snapshot.calculation_timestamp,
        ),
        _timestamp_equal(row["exchange_timestamp"], snapshot.exchange_timestamp),
        _timestamp_equal(
            row["local_receive_timestamp"],
            snapshot.local_receive_timestamp,
        ),
        _timestamp_equal(
            row["normalization_timestamp"],
            snapshot.normalization_timestamp,
        ),
        str(row["strategy_version"]) == snapshot.strategy_version,
        (
            None if row["sequence_id"] is None else str(row["sequence_id"])
        )
        == (None if snapshot.sequence_id is None else str(snapshot.sequence_id)),
        row["raw_payload_reference"] == snapshot.raw_payload_reference,
        actual_source_ids == expected_source_ids,
        row["source_sequence_id"] == expected_source_sequence,
        int(str(row["source_event_count"])) == snapshot.source_event_count,
        _optional_timestamp_equal(
            row["oldest_source_timestamp"],
            snapshot.oldest_source_timestamp,
        ),
        _optional_timestamp_equal(
            row["newest_source_timestamp"],
            snapshot.newest_source_timestamp,
        ),
        row["data_age_ms"] == snapshot.data_age_ms,
        row["is_warm"] is snapshot.is_warm,
        row["is_healthy"] is snapshot.is_healthy,
        [
            str(value)
            for value in cast(list[object], row["unavailable_reason_codes"])
        ]
        == _reason_codes(snapshot),
        row["book_generation"]
        == (
            None
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            else snapshot.book_generation
        ),
        row["binance_book_generation"]
        == (
            snapshot.binance_book_generation
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            else None
        ),
        row["okx_book_generation"]
        == (
            snapshot.okx_book_generation
            if isinstance(snapshot, CrossVenueFeatureSnapshot)
            else None
        ),
    )
    if not all(checks):
        raise ValueError(f"feature metadata/payload lineage mismatch: {path}")
    return PersistedFeatureRecord(
        snapshot=snapshot,
        code_version=code_version,
        config_hash=config_hash,
        payload_sha256=payload_sha256,
        source_file=path,
    )


class FeatureParquetReader:
    """Validate and merge partition files into deterministic decision-time order."""

    def __init__(self, root_path: Path, *, batch_size: int = 65_536) -> None:
        if batch_size < 1:
            raise ValueError("feature reader batch_size must be positive")
        self.root_path = root_path.resolve()
        self.schema_root = _schema_root(root_path)
        self.batch_size = batch_size

    @staticmethod
    def _matches(
        record: PersistedFeatureRecord,
        filters: FeatureScanFilter,
    ) -> bool:
        snapshot = record.snapshot
        decision = snapshot.decision_timestamp.astimezone(UTC)
        if filters.start is not None and decision < filters.start.astimezone(UTC):
            return False
        if filters.end is not None and decision > filters.end.astimezone(UTC):
            return False
        if filters.scopes is not None and snapshot.exchange not in filters.scopes:
            return False
        if filters.symbols is not None and snapshot.symbol not in filters.symbols:
            return False
        if filters.windows is not None and snapshot.window_seconds not in filters.windows:
            return False
        if filters.is_warm is not None and snapshot.is_warm is not filters.is_warm:
            return False
        return (
            filters.is_healthy is None
            or snapshot.is_healthy is filters.is_healthy
        )

    def _file_records(
        self,
        path: Path,
        filters: FeatureScanFilter,
    ) -> Iterator[PersistedFeatureRecord]:
        parquet_file = pq.ParquetFile(_native_path(path))
        if parquet_file.schema_arrow != FEATURE_PARQUET_SCHEMA:
            raise ValueError(f"feature Parquet schema mismatch: {path}")
        previous_key: tuple[object, ...] | None = None
        for batch in parquet_file.iter_batches(batch_size=self.batch_size):
            records = [
                _record_from_row(row, path, self.schema_root)
                for row in batch.to_pylist()
            ]
            for record in records:
                key = _stable_key(record)
                if previous_key is not None and key < previous_key:
                    raise ValueError(
                        f"feature Parquet file is not monotonically ordered: {path}"
                    )
                previous_key = key
                if self._matches(record, filters):
                    yield record

    def iter_records(
        self,
        *,
        filters: FeatureScanFilter | None = None,
    ) -> Iterator[PersistedFeatureRecord]:
        selected = filters or FeatureScanFilter()
        files = sorted(self.schema_root.rglob("*.parquet"))
        iterators = [
            self._file_records(path, selected)
            for path in files
        ]
        merged = heapq.merge(*iterators, key=_stable_key)

        def checked() -> Iterator[PersistedFeatureRecord]:
            seen: set[UUID] = set()
            for record in merged:
                snapshot_id = record.feature_snapshot_id
                if snapshot_id in seen:
                    raise ValueError(
                        f"duplicate persisted feature_snapshot_id: {snapshot_id}"
                    )
                seen.add(snapshot_id)
                yield record

        return checked()


def audit_feature_tree(
    root: Path,
    *,
    filters: FeatureScanFilter | None = None,
) -> FeatureAudit:
    """Audit schema, lineage, identity, partitions, and order-independent content."""

    records = FeatureParquetReader(root).iter_records(filters=filters)
    digest_sum = 0
    snapshot_ids: set[UUID] = set()
    files: set[Path] = set()
    partitions: set[tuple[str, str, str]] = set()
    scopes: set[str] = set()
    code_versions: set[str] = set()
    config_hashes: set[str] = set()
    decisions: list[datetime] = []
    rows = 0
    for record in records:
        snapshot = record.snapshot
        snapshot_ids.add(snapshot.feature_snapshot_id)
        files.add(record.source_file)
        partitions.add(_partition(snapshot))
        scopes.add(snapshot.exchange.value)
        code_versions.add(record.code_version)
        config_hashes.add(record.config_hash)
        decisions.append(snapshot.decision_timestamp)
        rows += 1
        digest_input = canonical_json(
            {
                "payload_sha256": record.payload_sha256,
                "code_version": record.code_version,
                "config_hash": record.config_hash,
            }
        )
        digest_sum = (
            digest_sum + int.from_bytes(bytes.fromhex(sha256_text(digest_input)), "big")
        ) % _DIGEST_MODULUS
    return FeatureAudit(
        rows=rows,
        files=len(files),
        unique_snapshot_ids=len(snapshot_ids),
        partitions=len(partitions),
        content_digest=f"{digest_sum:064x}",
        scopes=tuple(sorted(scopes)),
        code_versions=tuple(sorted(code_versions)),
        config_hashes=tuple(sorted(config_hashes)),
        earliest_decision_timestamp=min(decisions, default=None),
        latest_decision_timestamp=max(decisions, default=None),
    )


def compare_feature_trees(
    left_path: Path,
    right_path: Path,
    *,
    filters: FeatureScanFilter | None = None,
) -> FeatureConsistencyReport:
    """Compare logical feature content while allowing different file boundaries."""

    left = audit_feature_tree(left_path, filters=filters)
    right = audit_feature_tree(right_path, filters=filters)
    identical = (
        left.rows == right.rows
        and left.unique_snapshot_ids == right.unique_snapshot_ids
        and left.partitions == right.partitions
        and left.content_digest == right.content_digest
        and left.scopes == right.scopes
        and left.code_versions == right.code_versions
        and left.config_hashes == right.config_hashes
        and left.earliest_decision_timestamp == right.earliest_decision_timestamp
        and left.latest_decision_timestamp == right.latest_decision_timestamp
    )
    return FeatureConsistencyReport(
        left_path=left_path.resolve(),
        right_path=right_path.resolve(),
        left=left,
        right=right,
        identical=identical,
    )

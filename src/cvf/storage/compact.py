"""Read-only raw Parquet compaction with before/after lineage audit."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

from cvf.storage.collection_manifest import (
    compaction_in_progress_path,
    inspect_collection_evidence,
    preserve_clean_collection_manifest,
    validate_clean_collection,
)
from cvf.storage.parquet import RAW_PARQUET_SCHEMA

_MODULUS = 1 << 256


@dataclass(frozen=True, slots=True)
class RawAudit:
    rows: int
    files: int
    unique_record_ids: int
    content_digest: str
    payload_bytes: int
    partitions: int


@dataclass(frozen=True, slots=True)
class CompactionReport:
    input_path: Path
    output_path: Path
    before: RawAudit
    after: RawAudit


def _partition_value(value: str) -> str:
    return quote(value, safe="-_.")


def _row_partition(row: dict[str, object]) -> tuple[str, str, str, str]:
    received = row["local_receive_timestamp"]
    if not isinstance(received, datetime):
        raise ValueError("local_receive_timestamp must be a datetime")
    return (
        received.astimezone(UTC).date().isoformat(),
        str(row["exchange"]),
        str(row["symbol"]),
        str(row["channel"]),
    )


def _partition_directory(root: Path, partition: tuple[str, str, str, str]) -> Path:
    date, exchange, symbol, channel = partition
    return (
        root
        / f"date={_partition_value(date)}"
        / f"exchange={_partition_value(exchange)}"
        / f"symbol={_partition_value(symbol)}"
        / f"channel={_partition_value(channel)}"
    )


def _canonical(value: object) -> object:
    if isinstance(value, bytes):
        return {"bytes_sha256": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    return value


def _row_hash(row: dict[str, object]) -> int:
    encoded = json.dumps(
        {name: _canonical(row[name]) for name in RAW_PARQUET_SCHEMA.names},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return int.from_bytes(hashlib.sha256(encoded).digest(), "big")


def _temporary_index_path() -> Path:
    """Return a unique disk-backed audit index outside the read-only input tree."""

    candidate = Path.cwd() / ".tmp"
    try:
        candidate.mkdir(parents=True, exist_ok=True)
    except OSError:
        candidate = Path(tempfile.gettempdir())
    return candidate / f"cvf-raw-audit-{uuid4().hex}.sqlite3"


def _native_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name == "nt" and not resolved.startswith("\\\\?\\"):
        return f"\\\\?\\{resolved}"
    return resolved


def audit_raw_tree(root: Path) -> RawAudit:
    """Audit exact row identity/content without depending on file boundaries or row order."""

    resolved = root.resolve()
    files = sorted(resolved.rglob("*.parquet"))
    digest_sum = 0
    rows = 0
    payload_bytes = 0
    partitions: set[tuple[str, str, str, str]] = set()
    index_path = _temporary_index_path()
    index: sqlite3.Connection | None = None
    try:
        index = sqlite3.connect(_native_path(index_path))
        try:
            index.execute("PRAGMA journal_mode=OFF")
            index.execute("PRAGMA synchronous=OFF")
            index.execute("PRAGMA temp_store=FILE")
            index.execute(
                "CREATE TABLE record_ids (record_id TEXT PRIMARY KEY) WITHOUT ROWID"
            )
            for path in files:
                parquet_file = pq.ParquetFile(path)
                try:
                    if parquet_file.schema_arrow != RAW_PARQUET_SCHEMA:
                        raise ValueError(f"raw Parquet schema mismatch: {path}")
                    for batch in parquet_file.iter_batches(batch_size=65_536):
                        batch_record_ids: list[str] = []
                        for row in batch.to_pylist():
                            if int(str(row["schema_version"])) != 1:
                                raise ValueError(
                                    f"unsupported raw schema version in {path}"
                                )
                            record_id = str(row["record_id"])
                            if row["raw_payload_reference"] != f"raw://{record_id}":
                                raise ValueError(
                                    f"raw payload lineage mismatch: {record_id}"
                                )
                            payload = bytes(row["raw_payload"])
                            if not payload:
                                raise ValueError(f"empty raw payload: {record_id}")
                            partition = _row_partition(row)
                            expected = _partition_directory(resolved, partition)
                            if path.parent != expected:
                                raise ValueError(f"row/partition mismatch: {path}")
                            partitions.add(partition)
                            batch_record_ids.append(record_id)
                            payload_bytes += len(payload)
                            rows += 1
                            digest_sum = (digest_sum + _row_hash(row)) % _MODULUS
                        inserted_before = index.total_changes
                        index.executemany(
                            "INSERT OR IGNORE INTO record_ids (record_id) VALUES (?)",
                            ((record_id,) for record_id in batch_record_ids),
                        )
                        inserted = index.total_changes - inserted_before
                        if inserted != len(batch_record_ids):
                            raise ValueError("duplicate raw record_id")
                        index.commit()
                finally:
                    parquet_file.close()
        finally:
            index.close()
            index = None
    finally:
        if index is not None:
            index.close()
        for suffix in ("", "-journal", "-wal", "-shm"):
            Path(f"{index_path}{suffix}").unlink(missing_ok=True)
    return RawAudit(
        rows=rows,
        files=len(files),
        unique_record_ids=rows,
        content_digest=f"{digest_sum:064x}",
        payload_bytes=payload_bytes,
        partitions=len(partitions),
    )


def _write_rows(
    output: Path,
    partition: tuple[str, str, str, str],
    rows: list[dict[str, object]],
) -> None:
    directory = _partition_directory(output, partition)
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / f"part-compact-{uuid4().hex}.parquet"
    temporary = destination.with_name(f".{destination.name}.tmp")
    table = pa.Table.from_pylist(rows, schema=RAW_PARQUET_SCHEMA)
    try:
        pq.write_table(
            table,
            temporary,
            compression="zstd",
            use_dictionary=["exchange", "symbol", "channel", "message_kind", "transport"],
            write_statistics=True,
        )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def compact_raw_tree(
    input_path: Path,
    output_path: Path,
    *,
    target_rows: int = 100_000,
) -> CompactionReport:
    """Compact into a new tree and fail unless the full row audit remains identical."""

    if target_rows < 1:
        raise ValueError("target_rows must be positive")
    source = input_path.resolve()
    output = output_path.resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError("input and output trees must be disjoint")
    if not source.is_dir():
        raise ValueError(f"raw input directory does not exist: {source}")
    if output.exists() and any(output.iterdir()):
        raise ValueError("compaction output directory must be empty")
    evidence = inspect_collection_evidence(source)
    clean_collection = (
        validate_clean_collection(source)
        if evidence.any
        else None
    )
    before = (
        audit_raw_tree(source)
        if clean_collection is None
        else clean_collection.raw_audit
    )
    output.mkdir(parents=True, exist_ok=True)
    sentinel = compaction_in_progress_path(output)
    with sentinel.open("xb") as stream:
        stream.write(f"source={source}\n".encode())
        stream.flush()
        os.fsync(stream.fileno())

    current_partition: tuple[str, str, str, str] | None = None
    buffer: list[dict[str, object]] = []
    for path in sorted(source.rglob("*.parquet")):
        parquet_file = pq.ParquetFile(path)
        try:
            for batch in parquet_file.iter_batches(batch_size=target_rows):
                for row in batch.to_pylist():
                    partition = _row_partition(row)
                    if (
                        current_partition is not None
                        and partition != current_partition
                        and buffer
                    ):
                        _write_rows(output, current_partition, buffer)
                        buffer.clear()
                    current_partition = partition
                    buffer.append(row)
                    if len(buffer) >= target_rows:
                        _write_rows(output, partition, buffer)
                        buffer.clear()
        finally:
            parquet_file.close()
    if current_partition is not None and buffer:
        _write_rows(output, current_partition, buffer)

    after = audit_raw_tree(output)
    if (
        before.rows != after.rows
        or before.unique_record_ids != after.unique_record_ids
        or before.content_digest != after.content_digest
        or before.payload_bytes != after.payload_bytes
        or before.partitions != after.partitions
    ):
        raise RuntimeError("raw compaction audit mismatch")
    if clean_collection is not None:
        preserve_clean_collection_manifest(
            output,
            manifest=clean_collection.manifest,
            raw_audit=after,
        )
        sentinel.unlink()
        validate_clean_collection(output, raw_audit=after)
    else:
        sentinel.unlink()
    return CompactionReport(
        input_path=source,
        output_path=output,
        before=before,
        after=after,
    )

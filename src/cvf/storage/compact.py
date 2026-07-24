"""Read-only raw Parquet compaction with before/after lineage audit."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

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


def audit_raw_tree(root: Path) -> RawAudit:
    """Audit exact row identity/content without depending on file boundaries or row order."""

    resolved = root.resolve()
    files = sorted(resolved.rglob("*.parquet"))
    record_ids: set[str] = set()
    digest_sum = 0
    rows = 0
    payload_bytes = 0
    partitions: set[tuple[str, str, str, str]] = set()
    for path in files:
        parquet_file = pq.ParquetFile(path)
        if parquet_file.schema_arrow != RAW_PARQUET_SCHEMA:
            raise ValueError(f"raw Parquet schema mismatch: {path}")
        for batch in parquet_file.iter_batches(batch_size=65_536):
            for row in batch.to_pylist():
                if int(str(row["schema_version"])) != 1:
                    raise ValueError(f"unsupported raw schema version in {path}")
                record_id = str(row["record_id"])
                if record_id in record_ids:
                    raise ValueError(f"duplicate raw record_id: {record_id}")
                if row["raw_payload_reference"] != f"raw://{record_id}":
                    raise ValueError(f"raw payload lineage mismatch: {record_id}")
                payload = bytes(row["raw_payload"])
                if not payload:
                    raise ValueError(f"empty raw payload: {record_id}")
                partition = _row_partition(row)
                expected = _partition_directory(resolved, partition)
                if path.parent != expected:
                    raise ValueError(f"row/partition mismatch: {path}")
                partitions.add(partition)
                record_ids.add(record_id)
                payload_bytes += len(payload)
                rows += 1
                digest_sum = (digest_sum + _row_hash(row)) % _MODULUS
    return RawAudit(
        rows=rows,
        files=len(files),
        unique_record_ids=len(record_ids),
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
    before = audit_raw_tree(source)
    output.mkdir(parents=True, exist_ok=True)

    current_partition: tuple[str, str, str, str] | None = None
    buffer: list[dict[str, object]] = []
    for path in sorted(source.rglob("*.parquet")):
        parquet_file = pq.ParquetFile(path)
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
    return CompactionReport(
        input_path=source,
        output_path=output,
        before=before,
        after=after,
    )

"""Validate a Phase 2 raw Parquet collection without loading it all into memory."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import unquote
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

REQUIRED_COLUMNS = {
    "schema_version",
    "record_id",
    "raw_payload_reference",
    "exchange",
    "symbol",
    "channel",
    "message_kind",
    "transport",
    "exchange_timestamp",
    "local_receive_timestamp",
    "normalization_timestamp",
    "sequence_id",
    "connection_generation",
    "raw_payload",
}
VALIDATION_COLUMNS = [
    "record_id",
    "raw_payload_reference",
    "exchange",
    "symbol",
    "channel",
    "local_receive_timestamp",
    "raw_payload",
]


def _partition_values(root: Path, file_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for part in file_path.relative_to(root).parts[:-1]:
        if "=" in part:
            key, value = part.split("=", 1)
            values[key] = unquote(value)
    return values


def validate(root: Path, expected_rows: int | None) -> tuple[dict[str, object], list[str]]:
    parquet_files = sorted(root.rglob("*.parquet"))
    temporary_files = sorted(root.rglob("*.tmp"))
    errors: list[str] = []
    rows = 0
    payload_bytes = 0
    seen_record_ids: set[bytes] = set()
    exchanges: Counter[str] = Counter()
    channels: Counter[str] = Counter()

    if not root.is_dir():
        errors.append(f"collection root is not a directory: {root}")
    if not parquet_files:
        errors.append("no Parquet files found")
    if temporary_files:
        errors.append(f"{len(temporary_files)} temporary files remain")

    for file_path in parquet_files:
        partition = _partition_values(root, file_path)
        try:
            parquet_file = pq.ParquetFile(file_path)
            missing = REQUIRED_COLUMNS - set(parquet_file.schema_arrow.names)
            if missing:
                errors.append(f"{file_path}: missing columns {sorted(missing)}")
                continue

            for batch in parquet_file.iter_batches(columns=VALIDATION_COLUMNS, batch_size=65_536):
                records = batch.to_pydict()
                for index, record_id in enumerate(records["record_id"]):
                    reference = records["raw_payload_reference"][index]
                    payload = records["raw_payload"][index]
                    exchange = records["exchange"][index]
                    symbol = records["symbol"][index]
                    channel = records["channel"][index]
                    receive_time = records["local_receive_timestamp"][index]

                    try:
                        record_id_bytes = UUID(str(record_id)).bytes
                    except (TypeError, ValueError):
                        errors.append(f"{file_path}: invalid record UUID {record_id!r}")
                    else:
                        if record_id_bytes in seen_record_ids:
                            errors.append(f"{file_path}: duplicate record UUID {record_id}")
                        seen_record_ids.add(record_id_bytes)
                    if reference != f"raw://{record_id}":
                        errors.append(f"{file_path}: invalid raw reference for {record_id}")
                    if not isinstance(payload, bytes) or not payload:
                        errors.append(f"{file_path}: empty/non-binary payload for {record_id}")
                    if receive_time is None:
                        errors.append(f"{file_path}: missing receive timestamp for {record_id}")
                    for key, value in (
                        ("exchange", exchange),
                        ("symbol", symbol),
                        ("channel", channel),
                    ):
                        if partition.get(key) != value:
                            errors.append(
                                f"{file_path}: partition {key}={partition.get(key)!r} "
                                f"does not match row value {value!r}"
                            )

                    rows += 1
                    payload_bytes += len(payload) if isinstance(payload, bytes) else 0
                    exchanges[str(exchange)] += 1
                    channels[f"{exchange}:{channel}"] += 1
        except (OSError, pa.ArrowException) as exc:
            errors.append(f"{file_path}: {type(exc).__name__}: {exc}")

        if len(errors) >= 100:
            errors.append("validation stopped after 100 errors")
            break

    if expected_rows is not None and rows != expected_rows:
        errors.append(f"expected {expected_rows} rows, found {rows}")

    result: dict[str, object] = {
        "root": str(root.resolve()),
        "files": len(parquet_files),
        "rows": rows,
        "payload_bytes": payload_bytes,
        "temporary_files": len(temporary_files),
        "exchanges": dict(sorted(exchanges.items())),
        "channels": dict(sorted(channels.items())),
        "valid": not errors,
    }
    return result, errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--expected-rows", type=int)
    args = parser.parse_args()

    result, errors = validate(args.root, args.expected_rows)
    if errors:
        result["errors"] = errors
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())

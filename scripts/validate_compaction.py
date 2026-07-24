"""Audit a compacted raw tree against its source."""

from __future__ import annotations

import argparse
from pathlib import Path

from cvf.storage.compact import audit_raw_tree


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("compacted", type=Path)
    args = parser.parse_args()
    before = audit_raw_tree(args.source)
    after = audit_raw_tree(args.compacted)
    print({"source": before, "compacted": after})
    equivalent = (
        before.rows == after.rows
        and before.unique_record_ids == after.unique_record_ids
        and before.content_digest == after.content_digest
        and before.payload_bytes == after.payload_bytes
        and before.partitions == after.partitions
    )
    return 0 if equivalent else 1


if __name__ == "__main__":
    raise SystemExit(main())

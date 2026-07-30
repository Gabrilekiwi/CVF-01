# Phase 3D Feature Persistence and Consistency

## Storage contract

`AsyncFeatureParquetWriter` persists `FeatureSnapshot` and
`CrossVenueFeatureSnapshot` records beneath:

```text
feature_schema=v1/date=YYYY-MM-DD/symbol=<canonical>/scope=<exchange>
```

The date is the UTC decision date. Scope is `BINANCE`, `OKX`, or `CROSS_VENUE`. Feature files
never share the raw payload tree.

Every row preserves the full typed snapshot as canonical JSON and exposes query/audit columns
for:

- feature ID, schema, scope, symbol, and window;
- decision, calculation, event, receipt, and normalization timestamps;
- strategy/code versions and the full settings SHA-256;
- source snapshot IDs, sequence, count, timestamp bounds, raw reference, and book generations;
- data age, warmup, health, and structured unavailability reason codes;
- canonical payload SHA-256.

The writer rejects non-feature events, invalid scopes, or strategy/code/config mismatches.

## Bounded and atomic behavior

Queue capacity, batch rows, flush interval, and snapshot-ID deduplication capacity are validated
settings. A full queue applies backpressure and increments a counter. Duplicate ID plus identical
payload is acknowledged as `DEDUPLICATED`; the same ID with different content fails closed.

Each partition batch is sorted by decision time/window/UUID. PyArrow writes Zstandard data to a
unique temporary sibling, then `os.replace` atomically publishes the final file. Normal
completion drains the queue and flushes the final partial batch. Worker failure is retained in
stats and raised to producers/close.

## Read and audit behavior

`FeatureParquetReader` validates before yielding:

1. exact Arrow schema and schema version;
2. payload SHA-256 and canonical JSON form;
3. typed Pydantic snapshot validation;
4. every duplicated query column against the typed payload;
5. physical date/symbol/scope partition agreement;
6. monotonic order inside each file and deterministic merged order;
7. uniqueness of feature UUIDs across the selected tree.

Filters cover inclusive decision time, scope, symbol, window, schema version, snapshot ID,
structured unavailable reason, warmup, and health.

`audit_feature_tree` returns row/file/partition/unique-ID counts, an order-independent content
digest, scope/code/config sets, decision bounds, and structured unavailable-reason counts.
`compare_feature_trees` requires logical equality while allowing different file and batch
boundaries.

Float comparison is exact at the canonical payload layer. No hidden tolerance can convert a
small difference into equality.

## Offline commands

```powershell
python -m cvf audit-features --input data/processed
python -m cvf compare-features --left data/live_features --right data/replay_features
```

Both commands are network-free. They use no API keys, accounts, private endpoints, signals, or
orders.

The Phase 3 acceptance runner also records accepted/deduplicated snapshots, queue
backpressure, flush/file counts, last/average/maximum write latency, and retained worker error.
Atomic checkpoints separate replay completion from successful audit; `--resume` verifies
package/settings/input identity and re-audits any retained tree before comparison.

## Verification evidence

The Phase 3D-focused suite covers layout, round trips, source lineage, deduplication, bounded
caches/queues, flush/close, backpressure, atomic files, stable order, all filters,
schema/hash/metadata/partition tampering, duplicate IDs, audit summaries, exact float handling,
independent live/replay engine generation, logical-tree comparison, version/config rejection,
CLI behavior, and the no-signal/order boundary. Integration coverage additionally exercises
deterministic two-run acceptance, checkpoint reuse/rejection, stability summary generation,
strict no-lookahead, and zero signal/order/private-API counters.

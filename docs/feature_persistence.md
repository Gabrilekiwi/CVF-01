# Phase 3D Feature Persistence and Consistency

This document describes the v0.3.1 corrective release. The published v0.3.0 release remains
historical; v0.3.1 corrects restart idempotence, fail-closed audit behavior, and production
live/replay runtime integration. The authoritative fixed acceptance is
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2`. Each independent tree contains
32,490 unique rows (21,660 single-venue plus 10,830 cross-venue) with logical digest
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`; both runs report
package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`.

## Production runtime contract

`collect`, standard feature `replay`, and `accept-phase3` all persist through the same
`FeatureRuntime`. It owns feature state, single-venue/cross-venue engines, bounded history, and
the writer. All three paths feed it through `ReceiveTimeFeatureDriver`.

The driver reorders by local receive time inside the configurable
`features.receive_time_reorder_ms` bound (250 ms by default) and has a hard capacity. It publishes
eligible events, drains the event bus, and only then emits due feature ticks. An event at or
behind the advanced watermark and a full reorder buffer both fail closed. During live silence,
a wall-clock task advances through `now - reorder bound`, so feature ticks do not depend on a
new market message. Standard feature replay accepts receive-time ordering only.

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
- for single-venue rows, the versioned
  `feature-sources://sha256-sum-xor-v1/<fingerprint>` commitment over exact semantic source
  leaves;
- for cross-venue rows, the prior paired-spread history count and SHA-256 used by the Z-score
  and deterministic ID;
- data age, warmup, health, and structured unavailability reason codes;
- canonical payload SHA-256.

The writer rejects non-feature events, invalid scopes, or strategy/code/config mismatches.
`sha256-sum-xor-v1` binds source count, modular sum and XOR of domain-separated SHA-256 leaf
digests, and oldest/newest timestamps. It is an incremental, order-independent probabilistic
multiset commitment rather than a canonical-list SHA; the URI carries the algorithm version.

## Bounded, restart-safe, and atomic behavior

Queue capacity, batch rows, flush interval, and snapshot-ID deduplication capacity are validated
settings. A full queue applies backpressure and increments a counter. A bounded in-memory LRU
accelerates hot IDs; it is not the durability boundary. The writer maintains
`.feature-deduplication-v1.sqlite3` beside the schema tree and rebuilds this disposable SQLite
index from committed Parquet on every start. Duplicate ID plus identical payload is acknowledged
as `DEDUPLICATED`; the same ID with different content fails closed after cache eviction and
across writer restarts.

SQLite rows distinguish pending reservations from committed Parquet identities. A callback
failure or cancellation before enqueue rolls back its pending reservation. Atomic Parquet is the
source of truth, so an interrupted sidecar can be discarded and reconstructed. Worker failure is
retained in stats and raised to blocked/current producers and on close.

A lifecycle lock serializes start and close. A global write critical section covers
reservation, enqueue, and rollback, so a duplicate waiter cannot falsely deduplicate against an
owner cancelled before enqueue; after rollback it can take ownership and persist exactly one
row.

The feature root itself is claimed by `.feature-writer-v1.lock`. A process-local registry rejects
a second writer in the same process, while an OS-level exclusive lock uses `msvcrt` on Windows
and `flock` on Linux/Unix to reject a writer in another process. The root is normalized so a
caller using `root` and one using `root/feature_schema=v1` contend for the same claim. The writer
acquires the claim before removing any stale SQLite journal/WAL/SHM sidecars or rebuilding the
index from Parquet truth, and holds it through queue drain, rollback, index close, and complete
writer close. Contention fails closed immediately; startup failure, cancellation, worker/close
failure, and normal close release the claim, while process exit releases the OS lock. The lock
file may remain on disk as coordination metadata and is never a source of truth.

Rebuild and close use one shared lifecycle task. Repeated cancellation of callers is absorbed
until that task completes, queues drain, and worker/index resources close; an inner lifecycle
failure takes precedence over cancellation. The event bus, feature runtime, raw writer, and
collector shutdown follow the same completion-before-cancellation rule.

Each partition batch is sorted by decision time/window/UUID. PyArrow writes Zstandard data to a
unique temporary sibling, then `os.replace` atomically publishes the final file. Normal
completion drains the queue and flushes the final partial batch.

## Read and audit behavior

`FeatureParquetReader` validates before yielding:

1. the root exists and contains exactly the recognized `feature_schema=v1` layout;
2. no unknown feature-schema directory or Parquet file outside that schema tree exists;
3. at least one Parquet file and at least one physical feature row exist;
4. exact Arrow schema and schema version;
5. payload SHA-256 and canonical JSON form;
6. typed Pydantic snapshot validation;
7. every duplicated query column against the typed payload;
8. physical date/symbol/scope partition agreement;
9. monotonic order inside each file and deterministic merged order;
10. uniqueness of feature UUIDs across the selected tree.

Filters cover inclusive decision time, scope, symbol, window, schema version, snapshot ID,
structured unavailable reason, warmup, and health. A valid nonempty physical tree may yield zero
rows after filtering; that is distinct from accepting an absent or empty tree.

`audit_feature_tree` returns row/file/partition/unique-ID counts, an order-independent content
digest, scope/code/config sets, decision bounds, and structured unavailable-reason counts.
`compare_feature_trees` requires logical equality while allowing different file and batch
boundaries.

Float comparison is exact at the canonical payload layer. No hidden tolerance can convert a
small difference into equality.

## Offline commands

```powershell
python -m cvf audit-features --input data/processed
python -m cvf compare-features `
  --left data/processed/feature-live `
  --right data/processed/feature-replay
```

Both commands are network-free. They use no API keys, accounts, private endpoints, signals, or
orders.

The Phase 3 acceptance runner also records accepted/deduplicated snapshots, queue
backpressure, flush/file counts, last/average/maximum write latency, and retained worker error.
Atomic checkpoints separate replay completion from successful audit; `--resume` verifies
package/settings/input identity and re-audits any retained tree before comparison.

Safety evidence consists of an allowlisted runtime component inventory and observed output event
types. The offline acceptance graph excludes connectors, accounts, signal producers, and order
writers, but does not claim instrumented private-request counts.

`stability-phase3` repeats fixed-data replay and records its actual process wall time. It does
not set the continuous-live-soak flag. The distinct six-hour public-feed observation remains
pending and must be run through `scripts/run_phase3_live_soak.py`; neither the historical
v0.3.0 fixed replay nor a v0.3.1 harness check proves that live criterion.

The authoritative two runs achieved 1.237678x/1.229822x event-time throughput
(1,598.962/1,588.812 raw records per second). Their combined fixed-replay process observation
was 2,925.389 seconds and `live_stability_duration_completed=false`. The older
`fixed-30m-v0.3.1-final` evidence predates the final feature-root locking, journal-lineage, and
cancellation-race corrections and is superseded.

## Verification evidence

The Phase 3D-focused suite covers layout, round trips, source lineage, deduplication, bounded
caches/queues, flush/close, backpressure, atomic files, stable order, all filters,
schema/hash/metadata/partition tampering, duplicate IDs, audit summaries, exact float handling,
the production collect/replay/acceptance feature runtime, logical-tree comparison,
version/config rejection,
CLI behavior, and the feature-only execution boundary. Integration coverage additionally
exercises deterministic two-run acceptance, checkpoint reuse/rejection, fixed-replay stress
summary generation, strict no-lookahead, runtime component inventory, and observed output
event types. Private network requests are excluded by the offline execution graph; they are
not presented as instrumented request counters.

The current complete source-tree gate passes 275 tests, Ruff, strict mypy across 72 source
files, and `pip check`. Earlier candidate artifacts remain retired. The final-6 sdist/wheel build
and isolated clean-wheel offline CLI smoke test pass; current artifact hashes are recorded in
`docs/phase3_acceptance.md`.

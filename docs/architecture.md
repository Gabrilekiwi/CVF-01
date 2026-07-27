# CVF-01 Architecture

## Scope

CVF-01 remains a modular monolith and paper-trading research system. Version 0.3.0 persists the
deterministic single-venue and cross-venue observations shared by live collection and replay in
an audited, versioned feature tree, then verifies them with a resumable Phase 3 acceptance
harness. It contains no credentials, private APIs, market scores, signals, or order execution.

| Area | Status |
|---|---|
| Configuration, models, logging | Implemented and tested |
| Binance/OKX public collection | Implemented |
| Typed normalization and unit conversion | Implemented with fixtures |
| Exact venue-specific order books | Implemented with gap/checksum tests |
| Lifecycle, heartbeat, recovery, dedupe | Implemented with deterministic transports |
| Per-stream health and clock-skew accounting | Implemented |
| Bounded raw Parquet storage | Implemented |
| Ordered event bus and deterministic clock/scheduler | Implemented |
| Raw scan, normalized replay, compaction audit, CI | Implemented |
| Bounded venue/symbol state and feature availability | Implemented |
| Deterministic single-venue feature calculations | Implemented |
| Deterministic cross-venue alignment and research features | Implemented |
| Versioned feature persistence and live/replay consistency audit | Implemented |
| Fixed-data acceptance, checkpoints, and stability evidence | Implemented |
| Signals, trading, backtests, UI | Not active |

## Implemented data flow

```mermaid
flowchart LR
    BW["Binance /public WS"] --> BC["Binance connector"]
    BM["Binance /market WS"] --> BC
    BR["Binance public REST"] --> BC
    OW["OKX v5 public WS"] --> OC["OKX connector"]
    OR["OKX public REST"] --> OC
    BC --> RAW["Exact raw record"]
    OC --> RAW
    RAW --> Q["Bounded async queue"]
    Q --> PQ["Atomic partitioned Parquet"]
    BC --> BN["Typed normalization"]
    OC --> ON["Typed normalization + contract units"]
    BN --> BB["Binance local book"]
    ON --> OB["OKX local book"]
    BN --> H["Per-stream health"]
    ON --> H
    BB --> H
    OB --> H
    BN --> BUS["Bounded normalized event bus"]
    ON --> BUS
    BUS --> STATE["Bounded feature state"]
    STATE --> VF["Single-venue features"]
    VF --> XV["Phase-3C cross-venue alignment"]
    XV --> FP["Phase-3D feature Parquet"]
    FP --> FA["Schema + lineage + consistency audit"]
    FA --> AC["Phase-3 acceptance report"]
    PQ --> RR["Stable raw reader"]
    RR --> RN["Live normalizers"]
    RN --> BUS
```

Raw bytes are queued before normalization. A parse failure therefore does not erase the
offending frame. Storage failure is fatal and observable; it is not converted into silent
data loss.

Each normalized consumer has its own bounded FIFO queue and sequential worker. A full queue
applies producer backpressure. A failed consumer is recorded and surfaced as a fatal pipeline
error during publish or shutdown; it cannot silently disappear while collection continues.

Replay selects raw rows by time, venue, symbol, and channel, then merges them with an explicit
stable tie-break rule. `ReplayClock` and `DecisionScheduler` advance from event time without
depending on asyncio task ordering. Live and replay both publish into `NormalizedEventBus`.

Feature state is keyed by `(exchange, canonical symbol)`. Each channel family has an independent
event-time window with a configured duration and hard item cap. Default late events are dropped;
an insertion policy can admit events within a configured lateness bound. Window queries use
`(start, end]`, so decision-boundary behavior is deterministic.

The feature-side book applies absolute normalized levels. Updates arriving before a snapshot are
bounded and replayed after that snapshot. Any generation change immediately invalidates the old
book window and restarts warmup. Sequence gaps, crossed books, missing OI, stale OI, blocked
stream health, and pipeline backlog remain structured unavailability rather than numeric zeros.

`SingleVenueFeatureEngine` closes every trailing window at an explicit decision timestamp. It
calculates trade flow, sequence-valid book/OFI observations, price and volatility, OI context,
funding/premium crowding, and public-sample liquidation activity. Feature UUIDs are derived from
strategy, venue, symbol, window, decision time, and book source boundary. A current book that
already includes a post-decision update is rejected instead of being silently rewound.

`CrossVenueFeatureEngine` takes an unordered collection of typed single-venue snapshots and,
for each venue, selects the deterministic latest snapshot whose decision time is not after the
cross-venue boundary. It reports per-venue source time and age, their age/time differences,
alignment quality, and typed `ALIGNED`, `DEGRADED`, stale, or unavailable status. Missing,
future, stale, unhealthy, and non-warm sources remain structured reasons.

Cross-venue comparisons use only venue-local normalized ratios or states. In particular,
absolute Binance and OKX open interest is never compared. Spread Z-scores use prior paired
decision timestamps from the supplied snapshot set; exact inputs therefore produce the same
result regardless of iterable order or live/replay engine instance. Deterministic IDs include
strategy, code version, config hash, symbol/window/decision boundary, both source snapshot IDs,
and alignment status.

Confirmation/divergence values are typed research observations, not market scores or trading
signals. Lead/lag fields deliberately remain unavailable until independently validated
event-time history exists; local receive order is never accepted as evidence of venue
leadership.

`AsyncFeatureParquetWriter` accepts only typed market-feature snapshots whose strategy,
code version, and configuration hash match the writer. Its queue, batch, flush interval, and
deduplication cache are bounded and configurable. A full queue applies observable producer
backpressure. Files are grouped by schema/date/symbol/scope, sorted by a stable decision-time
key, written to a temporary sibling, and atomically replaced. File names include an input
content digest rather than process time.

The stored row contains directly queryable audit columns plus a canonical validation-compatible
JSON payload and SHA-256. `FeatureParquetReader` checks the exact Arrow schema, payload hash,
canonical encoding, typed model validation, metadata/payload agreement, and physical partition
before returning a record. Files are merged in stable decision-time order and duplicate feature
IDs fail closed.

`audit_feature_tree` produces an order-independent logical content digest with source version,
scope, partition, ID, and time-bound summaries. `compare_feature_trees` requires all logical
content to match while permitting different physical batch/file boundaries. There is no implicit
floating tolerance: the canonical persisted values must agree exactly.

`Phase3AcceptanceRunner` first audits the fixed raw input, then drives two separate
`ReplayRunner`/feature/writer instances with different writer batch sizes. Each run records
normalization counts, feature-state outcomes, decision boundaries, writer statistics, CPU,
RSS, throughput, and latency. A source timestamp after its decision boundary aborts
immediately. The two audited logical trees must match exactly even when their physical files
differ.

Per-run checkpoints are atomically published at replay-complete and audit-complete boundaries.
They are reusable only when package source, all settings, input/output paths, ordering, batch
size, and flush interval match; reuse still performs a fresh tree audit. The stability harness
repeats the same fixed-data acceptance until a requested wall-time target and records actual
observation time separately from the target. It explicitly does not equate repeated fixed data
with continuous live-feed recovery evidence.

## Connector lifecycle

`PublicWebSocketSession` is venue-neutral and owns:

- connect timeout;
- subscription/resubscription;
- receive deadline;
- protocol or text heartbeat;
- exponential reconnect delay with bounded jitter;
- stable-connection backoff reset;
- unsubscribe/close on shutdown;
- observable lifecycle events.

It catches only known transport/session failures. Unexpected application/storage exceptions
end the connector monitor and fail collection.

Binance uses two connections because current USDⓈ-M routing separates public depth/BBO from
market trades/mark/liquidation. OKX uses one v5 public connection and an alphanumeric request
ID.

## Ordering and local books

### Binance

Each configured symbol has an isolated bounded diff buffer. Every WebSocket generation
invalidates the old book and requests `/fapi/v1/depth`. Activation follows the documented
snapshot overlap rule. Once active, each diff must satisfy `pu == previous u`. Absolute
quantity replaces a level and zero removes it. Gaps, retry conditions, and buffer overflow
increment the generation and force a new snapshot.

### OKX

`books` snapshot/update state is isolated per symbol. `prevSeqId` must equal the active
`seqId`; the documented same-sequence keepalive and maintenance reset are accepted by the
state machine. A gap invalidates the book and reconnects for a fresh snapshot.

Current production checksum values are zero after the June 2026 deprecation, so continuity is
validated by sequence IDs. Nonzero historical fixtures still use the signed CRC32 top-25
algorithm. `books5`, when configured, remains a separate full-snapshot state.

## Time model

Every normalized event records:

- exchange-declared UTC timestamp;
- first local UTC receipt timestamp;
- normalization timestamp;
- optional venue sequence;
- stable raw payload reference.

Public REST time endpoints estimate `local - exchange` clock offset at the request/response
midpoint. Health uses the adjusted receive latency while preserving raw timestamps. Arrival
order alone is never evidence that one venue led another.

## Health model

Health state is keyed by `(exchange, canonical symbol, channel)` and tracks current state plus
lifetime counters:

- connection, last event/receive, stale deadline;
- last/average/maximum adjusted latency and normalization latency;
- clock skew;
- messages and duplicates;
- sequence gaps, checksum failures, book generation, resync;
- reconnects and resubscriptions;
- REST status and OI-specific freshness;
- parse errors, drops, and backpressure.

Status precedence is:

```text
DISCONNECTED > RESYNCING > STALE > DEGRADED > CONNECTED
```

Future trading phases must block new entries for `STALE`, `RESYNCING`, or `DISCONNECTED`.

## Backpressure and persistence

The raw writer uses a bounded `asyncio.Queue`. A full queue applies producer backpressure and
increments a health counter; core events are not intentionally dropped. Batches flush by row
count or elapsed time. PyArrow work runs off the event loop.

Files are grouped by UTC receipt date, exchange, canonical symbol, and channel. Each file is
written to a temporary sibling and atomically replaced. Worker failure races against blocked
producers so a dead writer cannot leave a producer waiting forever.

## Shutdown

The collector stops producers, unsubscribes where the protocol supports it, wakes receive and
reconnect waits, cancels REST pollers, waits for connector tasks, drains the raw queue, writes
the final partial batch, and closes the writer. Signals and finite durations use the same
path.

## Security boundary

- Public endpoints only.
- No key/secret/passphrase configuration.
- No live order endpoint or execution object.
- Offline commands do not connect.
- Additive venue fields are tolerated, but missing/invalid required fields fail loudly.

# CVF-01 Data Dictionary

This dictionary reflects the v0.3.1 corrective release. The published v0.3.0 release remains
historical; v0.3.1 corrects feature identity, missing-value semantics, normalized-event
journaling, restart idempotence, and acceptance evidence. The authoritative v0.3.1 fixed
30-minute acceptance is retained under
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2`; the continuous six-hour live
observation has not completed.

## 1. Conventions

- Canonical symbols are uppercase `BASE-QUOTE-PERP`.
- All timestamps are timezone-aware and normalized to UTC in memory.
- Prices, quantities, fees, funding, and PnL use `Decimal`; binary floats are reserved for
  scores, ratios, latency, and statistical features.
- Quantity units must be stated by the normalized field; venue contract counts are not silently
  treated as base-asset quantity.
- `sequence_id` is nullable because some public channels have no usable sequence. Null never
  means that ordering was validated.
- For normalized market/health events, `raw_payload_reference` points to retained raw lineage.
  A single-venue feature instead uses a canonical
  `feature-sources://sha256-sum-xor-v1/<source-fingerprint>` over all eligible semantic source
  content; cross-venue lineage includes both source snapshot IDs plus the count and SHA-256 of
  the exact prior paired-spread history.
- `sha256-sum-xor-v1` hashes each semantic source with a domain-separated SHA-256 leaf after
  excluding nondeterministic normalization time. Its final fingerprint binds count, modular
  digest sum, digest XOR, and oldest/newest source timestamps. It is a mergeable,
  order-independent probabilistic multiset commitment, not a canonical-list digest.

## 2. Common event fields

Implemented normalized market, health, and feature records inherit these fields. Inactive
Phase 4/5 data-contract classes use the same base shape, but no current producer emits signal or
paper-trading events:

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `exchange` | enum | no | `BINANCE`, `OKX`, `CROSS_VENUE`, or `SIMULATED` |
| `symbol` | string | no | Canonical symbol; `*` is allowed only for exchange-wide health |
| `exchange_timestamp` | UTC datetime | no | Venue-declared event time, or decision/event clock for derived records |
| `local_receive_timestamp` | UTC datetime | no | First local observation time |
| `normalization_timestamp` | UTC datetime | no | Time normalized construction began/completed |
| `event_type` | enum | no | Stable normalized record discriminator |
| `sequence_id` | integer/string | yes | Venue sequence/trade/update identifier when available |
| `raw_payload_reference` | string | yes | URI/path plus optional row locator in raw storage |
| `receive_latency_ms` | computed float | no | Local receive time minus exchange time; may expose clock skew |

## 3. Market-data models

### `Trade`

| Field | Type | Meaning |
|---|---|---|
| `trade_id` | string | Venue trade/aggregate-trade identifier |
| `price` | positive decimal | Execution price in quote per base |
| `quantity` | positive decimal | Normalized base quantity; conversion must be documented |
| `contract_quantity` | positive decimal | Venue contract quantity when the venue reports contracts |
| `aggressor_side` | `BUY`/`SELL` | Side that crossed the spread |
| `notional` | computed decimal | `price * quantity` |

### `OrderBookLevel`

| Field | Type | Meaning |
|---|---|---|
| `price` | positive decimal | Level price |
| `quantity` | non-negative decimal | Resting quantity; zero deletes a level in updates |

### `OrderBookSnapshot`

`bids` are strictly descending, `asks` strictly ascending, and best bid must be below best ask.
`depth` states the intended retained depth; `checksum` is nullable and venue-specific.
`generation` increments whenever local continuity is invalidated.

### `OrderBookUpdate`

Contains changed `bids`/`asks`, current `sequence_id`, optional `previous_sequence_id`, and
optional checksum. At least one side must change. Applying it safely requires venue-specific
sequence rules and an active snapshot.

### `BestBidAsk`

`bid_price`, `bid_quantity`, `ask_price`, `ask_quantity`; crossed/locked quotes are rejected.
`mid_price` is computed as `(bid + ask) / 2`.

### `OpenInterest`

| Field | Type | Nullable | Unit |
|---|---|---:|---|
| `open_interest_contracts` | non-negative decimal | no | Venue contracts |
| `open_interest_base` | non-negative decimal | yes | Base asset after instrument conversion |
| `open_interest_quote` | non-negative decimal | yes | Quote notional at documented reference price |

Each venue is standardized independently. Absolute OI is never compared without contract/unit
conversion.

### `FundingRate`

`funding_rate` is a decimal fraction for the venue's stated interval.
`next_funding_timestamp` is an optional UTC datetime.

### `MarkPrice` and `IndexPrice`

Contain one positive decimal, `mark_price` or `index_price`. The raw reference preserves the
venue's index composition/mark methodology context.

### `LiquidationEvent`

`position_side` names the liquidated position (`LONG` or `SHORT`), not an ambiguously inferred
trade side. `price`, `quantity`, and computed `notional` are positive. Public feeds are relative
activity samples and not complete liquidation totals.

## 4. Health and features

### `ExchangeHealth`

| Field | Meaning |
|---|---|
| `status` | `CONNECTED`, `DEGRADED`, `STALE`, `RESYNCING`, `DISCONNECTED` |
| `is_connected` | Transport connection state |
| `last_event_timestamp` | Most recent valid exchange event time |
| `last_latency_ms` | Most recent observed receive latency |
| `average_latency_ms`, `maximum_latency_ms` | Running adjusted latency statistics |
| `last_normalization_latency_ms` | Most recent local normalization time |
| `clock_skew_ms` | Estimated venue/local clock difference |
| `messages_received` | Accepted normalized events |
| `duplicate_events` | Count in the current observation scope |
| `sequence_gaps`, `checksum_failures` | Lifetime continuity failures |
| `reconnects`, `resubscriptions` | Lifecycle recovery counters |
| `parse_errors`, `dropped_events`, `backpressure_events` | Ingestion/storage diagnostics |
| `book_generation` | Current local-book generation |
| `sequence_gap_detected` | Whether continuity failed |
| `resyncing` | Whether snapshot/state recovery is active |
| `rest_healthy` | Public REST dependency state |
| `open_interest_stale` | OI freshness flag |
| `last_error` | Latest structured-error summary |
| `details` | Small typed diagnostic map |

Exchange-wide health uses `symbol="*"`.

After connector deduplication and normalization, accepted market events are persisted in raw
Parquet with `channel="_normalized_event"`. The collector's periodic `ExchangeHealth` records
are written through the same journal. Their `raw_payload` is canonical typed JSON and their row
metadata must match the decoded event exactly during replay. These internal rows supplement the
original public WebSocket/REST payload records.

### Receive-time feature timeline

`FeatureRuntime` is the shared state/calculation/persistence owner for collection, standard
replay, and acceptance. `ReceiveTimeFeatureDriver` orders normalized events by
`local_receive_timestamp` within `features.receive_time_reorder_ms` (250 ms by default). Its
buffer is bounded; an event at or behind the watermark fails closed. Live wall-clock advancement
continues decision ticks during quiet periods. Standard feature replay is receive-time-only.

### `FeatureSnapshot` schema v1

The legacy `MarketFeature.values` map remains a compatibility model. Phase 3 uses the typed,
versioned `FeatureSnapshot` contract:

| Field | Meaning |
|---|---|
| `feature_snapshot_id` | Deterministic UUID binding code/config, source lineage/content, availability, and typed feature values |
| `schema_version`, `strategy_version` | Immutable schema and strategy identities |
| `calculation_timestamp`, `decision_timestamp` | Calculation time and no-lookahead boundary |
| `window_seconds` | Trailing `(decision-window, decision]` interval |
| `book_generation`, `source_sequence_id` | Exact order-book lifecycle/source boundary |
| `source_event_count` | Valid accepted source events used |
| `oldest_source_timestamp`, `newest_source_timestamp` | Auditable event-time bounds |
| `data_age_ms` | Age of the newest source at decision time; null when no source exists |
| `is_warm`, `is_healthy` | Separate statistical warmup and operational health gates |
| `unavailable_reasons` | Structured missing, stale, generation, health, or backlog blockers |
| typed feature groups | Trade flow, order book, price, OI, crowding, and liquidation |

Null means unavailable/undefined and is distinct from true numeric zero. An empty snapshot must
use null source bounds and null `data_age_ms`; it cannot fabricate age `0`. A non-warm or
unhealthy snapshot must contain at least one structured reason. A source timestamp after the
decision boundary is rejected. A metric with zero historical variance has no defined Z-score;
it remains null and blocks warm readiness rather than becoming numeric `0`.

Single-venue typed groups include:

| Group | Values |
|---|---|
| trade flow | buy/sell notional, taker imbalance/Z-score, notional/count impulse, average size, large-trade share |
| order book | weighted depth, bid/ask change, additions/removals, recovery quantity per second, imbalance, spread, mid, microprice, depth-walk slippage, OFI/Z-score |
| price | return/impulse Z-score, realized volatility, 1-second-bucket ATR, trailing high/low, ATR-relative move, abnormal-jump flag |
| open interest | absolute/percentage change, venue-local Z-score, age, `PriceOpenInterestState` |
| crowding | funding/Z-score, mark-index premium/Z-score, taker bias, `CrowdingState` |
| liquidation | public-sample long/short notional, activity Z-score, activity-with-OI-decline flag |

`removed_liquidity_quantity` is a depth-update removal proxy, not proof that the quantity was
cancelled rather than executed. Liquidation fields intentionally use `public_sample` naming
because neither venue feed represents total market liquidations.

### `CrossVenueFeatureSnapshot` schema v1

The Phase 3C cross-venue record has `exchange=CROSS_VENUE` and preserves:

| Group | Values |
|---|---|
| lineage | strategy/code versions, config hash, deterministic ID, both source snapshot IDs, prior paired-spread history count/SHA-256, book generations, event count, and source time bounds |
| alignment | both source IDs/timestamps/ages, absolute data-age difference, source timestamp difference, typed status, quality, and structured reasons |
| price | both mids, signed/absolute spread, explicit symmetric denominator, percentage spread/Z-score, return/impulse direction, impulse strength, volatility, and relative-spread differences |
| order flow | taker and OFI direction/difference, taker strength, depth difference, liquidity additions/removals, recovery difference, and typed liquidity divergence |
| positioning | percentage-OI directions and price/OI context, funding direction/abnormality, premium difference, crowding agreement, and public-sample liquidation confirmation |
| confirmation | typed price/impulse/taker/OFI/crowding/liquidation observations, a bounded research-only aggregate, and divergence input |
| lead/lag | research-only typed fields plus explicit unavailable reasons; local arrival order is prohibited |

The percentage mid spread is:

```text
(binance_mid - okx_mid) / ((abs(binance_mid) + abs(okx_mid)) / 2)
```

The denominator is stored explicitly. A zero denominator, missing side, or insufficient prior
paired history produces null plus a structured reason; it never becomes numeric zero.
Cross-venue OI uses percentage change and venue-local `PriceOpenInterestState` only. Absolute OI
is not compared because venue contract units are not interchangeable.

`spread_history_pair_count` and `spread_history_sha256` describe the complete eligible prior
paired-spread sequence used at that decision. They are persisted in the typed payload and bound
into `feature_snapshot_id`; they are not advisory audit labels.

### Feature Parquet schema v1

Physical layout:

```text
data/processed/
  feature_schema=v1/
    date=YYYY-MM-DD/
      symbol=BTC-USDT-PERP/
        scope=BINANCE|OKX|CROSS_VENUE/
          part-....parquet
```

| Field family | Values |
|---|---|
| identity/partition | `feature_schema_version`, feature UUID, scope, symbol, window, decision/calculation timestamps |
| runtime lineage | strategy version, code version, full config SHA-256 |
| source lineage | source snapshot IDs, prior paired-spread history count/SHA-256, source sequence, event count, oldest/newest source times, raw reference, venue book generations |
| availability | data age, warm/healthy flags, structured top-level reason codes |
| content integrity | canonical typed `payload_json` and its `payload_sha256` |

The canonical payload excludes computed display fields and can be validated directly back into
`FeatureSnapshot` or `CrossVenueFeatureSnapshot`. Query columns must exactly match that payload.
Any schema drift, hash mismatch, metadata mismatch, wrong partition, duplicate UUID, or
non-monotonic file order fails the reader/audit.

The writer's `.feature-deduplication-v1.sqlite3` is a disposable, rebuildable ID/content index,
not part of the feature schema and not the source of truth. Committed Parquet rebuilds it on
every writer start. The bounded in-memory ID cache is only a hot-path accelerator.

Each writable feature root also carries `.feature-writer-v1.lock`. A process-local registry and
an OS-level exclusive lock (`msvcrt` on Windows, `flock` on Linux/Unix) make `root` and its
`root/feature_schema=v1` alias one writer claim. The claim is acquired before stale SQLite
sidecars can be removed or the index rebuilt, is held until queue drain and full close, and makes
a competing instance or process fail closed. Startup/close failure and cancellation release the
claim; process exit releases the OS lock. The persistent lock file is coordination metadata, not
feature schema or a source of truth.

Tree comparison hashes logical records rather than filenames. Live and replay outputs may use
different batch boundaries but must have identical IDs, canonical payloads, code/config lineage,
partitions, scopes, and decision bounds. Float values are exact persisted inputs; no unstated
tolerance is applied.

## 5. Acceptance artifacts

Acceptance summaries and checkpoints are operational evidence, not normalized market events.
They are written outside the feature Parquet tree and include:

| Field family | Meaning |
|---|---|
| input identity | raw path/audit, settings fingerprint, package-source SHA-256, replay order |
| checkpoint state | `REPLAY_COMPLETE` or `AUDIT_COMPLETE`, output path, writer batch/flush settings |
| replay | raw/normalized/skipped counts, event/state outcomes, decision span, wall/CPU time |
| performance | throughput and captured-rate multiplier, initial/final/peak RSS, calculation/receive/write latency |
| persistence | accepted/deduplicated snapshots, files, flushes, backpressure, worker error, feature audit |
| correctness | no-lookahead violations, per-tree digest, exact comparison, warm/health/reason counts |
| safety | runtime component inventory, observed feature-output types, and an explicit statement that network requests are not instrumented as counters |
| stability | fixed-replay stress duration kept separate from the pending continuous live-feed soak |

The requested stability duration is never substituted for actual observation time. A capped or
interrupted run remains machine-readably incomplete.

The authoritative 2026-07-31 v0.3.1 evidence at
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2` contains 32,490 unique feature rows
in each tree: 21,660 single-venue plus 10,830 cross-venue snapshots. Both trees have exact
logical digest
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`,
zero no-lookahead violations, and package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`.
Run throughput was 1.237678x/1.229822x event time
(1,598.962/1,588.812 raw records per second). The 2,925.389-second fixed-replay observation is
not a live soak: `live_stability_duration_completed=false`, so the continuous six-hour
public-feed criterion remains pending.

The older `fixed-30m-v0.3.1-final` tree predates the final feature-root locking,
journal-lineage, and cancellation-race corrections. It is superseded diagnostic evidence and
must not be presented as the current acceptance result.

## 6. Planned signal schema (Phase 4; not implemented)

The following table describes an inactive contract/proposal only. Data-model classes may exist
for validation and planning, but the current source tree has no score calculator, signal state
machine, signal producer, or signal persistence pipeline. The active v0.3.1 configuration has no
score weights or LONG/SHORT signal thresholds. A future signal record is expected to add:

| Field | Meaning |
|---|---|
| `signal_id` | UUID |
| `timestamp` | Decision time |
| `signal_type` | Entry, exit, hold, no-trade, or emergency exit |
| `binance_score`, `okx_score`, `combined_score` | Unrounded decision scores |
| `confidence` | Bounded research confidence `[0, 1]` |
| `suggested_exchange` | Selected execution venue or null |
| `suggested_entry_price` | Depth-aware expected entry or null |
| `stop_loss`, `take_profit_1`, `take_profit_2` | Ordered levels for entries |
| `expires_at` | Strictly after decision time |
| `reasons` | Structured `code/message/exchange/value/threshold` records |
| `blocking_conditions` | Structured blockers explaining `NO_TRADE` |
| `feature_snapshot_id` | Exact feature join key |
| `strategy_version` | Immutable decision logic/config version |

Future long-entry validation must require `stop < entry < TP1 < TP2`; short levels would be
symmetric.

## 7. Planned paper-trading schemas (Phase 5; not implemented)

The following objects are inactive contract/design targets only. No simulated execution,
position ledger, fill engine, fee/slippage accounting, or risk runtime exists in the current
release, and active configuration contains no execution, exit, or risk thresholds.

### `SimulatedOrder`

Tracks requested/filled quantity, average fill price, fee, estimated slippage, order status,
creation/completion timestamps, and rejection reason. Only `MARKET` is supported in the MVP.

### `SimulatedPosition`

Tracks opening signal, side/status, entry, original/remaining quantity, stop/targets,
open/close times, realized/unrealized PnL, fees, and funding. A `CLOSED` position must have zero
remaining quantity.

### `SimulatedTrade`

Represents one fill or exit slice with order/position IDs, purpose, side, execution price,
quantity, fee, slippage, realized PnL, and execution time.

## 8. Symbol mapping

| Canonical | Binance | OKX perpetual | OKX index instrument |
|---|---|---|---|
| `BTC-USDT-PERP` | `BTCUSDT` | `BTC-USDT-SWAP` | `BTC-USDT` |
| `ETH-USDT-PERP` | `ETHUSDT` | `ETH-USDT-SWAP` | `ETH-USDT` |

Mapping is exact and fail-closed. The OKX index instrument is converted to its configured
perpetual canonical symbol only inside the index-channel mapping.

## 9. Raw Parquet schema

Implemented layout:

```text
data/raw/date=YYYY-MM-DD/exchange=BINANCE/symbol=BTC-USDT-PERP/channel=aggTrade/*.parquet
data/raw/date=YYYY-MM-DD/exchange=BINANCE/symbol=BTC-USDT-PERP/channel=_normalized_event/*.parquet
```

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | int16 | Raw schema, currently `1` |
| `record_id` | UUID string | Unique row identity |
| `raw_payload_reference` | string | Stable `raw://<record_id>` lineage key |
| `exchange`, `symbol`, `channel` | string | Routing and partition metadata |
| `message_kind` | string | Market data, control, lifecycle, or parse error |
| `transport` | string | `websocket`, `rest`, or `internal` |
| `exchange_timestamp` | UTC timestamp | Nullable when the venue supplies none |
| `local_receive_timestamp` | UTC timestamp | First local receipt |
| `normalization_timestamp` | UTC timestamp | Nullable for raw-first writes |
| `sequence_id` | string | Nullable venue sequence/trade ID |
| `connection_generation` | int64 | WebSocket lifecycle generation |
| `raw_payload` | binary | Exact received frame/HTTP response, or canonical normalized-event JSON for the internal journal |

### Collection lifecycle manifest

Every new `v0.3.1` collection root is exclusively claimed by
`_collection_manifest.json` before a producer starts:

| Field | Meaning |
|---|---|
| `run_id`, `started_at` | Single-run ownership and UTC start |
| `terminal` | `IN_PROGRESS` or `CLEAN_END` |
| `code_version`, `code_sha256` | Capture package identity |
| `strategy_version`, `settings_sha256` | Complete runtime configuration identity |
| `terminal_at`, `feature_timeline_end_at` | Present only at clean completion; timeline end is within collection lifetime |
| `normalized_event_count` | Exact reconciled post-dedup journal event count |
| `raw_audit` | Logical row, UUID, content, payload-byte, partition, and time evidence |

`CLEAN_END` also requires exactly one internal feature-timeline terminal marker, no normalized
event after that marker, matching journal counts, matching raw audit, and no unfinished `.tmp`
or compaction sentinel. The manifest proves clean collection completeness and lineage; it is not
a per-source-frame 0/1/N normalization ledger, database WAL, or authorization to merge runs.

Malformed WebSocket bytes are persisted under `_unparsed` before the session is recovered.
Connection lifecycle records use wildcard symbol and `_session_*` channels. Journal rows use
`message_kind="normalized_event"` and `transport="internal"`; their embedded typed event retains
the original raw reference where one exists. Outer row metadata, including connection
generation, must match the decoded typed event. A reconciled `CLEAN_END` tree is replayed from
that complete post-dedup journal without partial filters. A legacy tree with no manifest,
journal, or incomplete evidence can be re-normalized from public payloads; any partial strict
evidence fails closed.

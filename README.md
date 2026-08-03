# Cross-Venue Short-Term Flow Strategy (CVF-01)

CVF-01 is a research-first, paper-trading-only system for short-horizon Binance and OKX
USDT perpetual research. Version 0.3.1 is the corrective release for the
Phase 3 feature pipeline: deterministic single-venue and cross-venue snapshots, audited Parquet
persistence, fixed-data replay acceptance, resumable checkpoints, and explicit live-soak
instrumentation. The published v0.3.0 release remains historical; v0.3.1 corrects its production
live/replay integration, identity, persistence, audit, and evidence semantics. It still does not
score markets, generate signals, or place orders.

## Current status

The completed Phase 2 data foundation includes:

- Binance USDⓈ-M public WebSocket collection on the current `/public` and `/market` routes;
- OKX v5 public WebSocket collection with text `ping`/`pong`;
- BTC/ETH trades, BBO/order books, OI, funding, mark/index, and public liquidation samples;
- typed fixture-backed normalization with exact `Decimal` quantities and both timestamps;
- Binance REST-snapshot plus diff-depth reconstruction with strict `pu` continuity;
- OKX `books` sequence validation, current zero-checksum handling, and historical CRC32 support;
- bounded TTL/LRU deduplication;
- reconnect, resubscribe, exponential backoff with jitter, and clean shutdown;
- per-exchange/symbol/channel health, latency, clock-skew, REST, gap, checksum, and
  backpressure accounting;
- exact raw payload bytes written through a bounded asynchronous queue to atomic,
  Zstandard-compressed Parquet partitions.

Phase 2.5 additionally includes:

- an ordered normalized-event bus with independent bounded consumer queues;
- observable consumer backlog, processing latency, backpressure, and fatal failures;
- UTC live/replay clocks and deterministic 1-second/5-second decision scheduling;
- exact original public payload retention plus a post-dedup `_normalized_event` journal,
  including internally emitted `ExchangeHealth` events;
- an atomic `IN_PROGRESS`/`CLEAN_END` collection manifest that binds the run, source code,
  settings, exact journal count/terminal watermark, and logical raw audit;
- filtered raw Parquet reads with stable event-time or receive-time ordering and a bounded,
  disk-backed external sort instead of one materialized batch per input file;
- fail-closed source modes: `auto` uses an exact typed journal only after full manifest
  validation, legacy trees with no evidence use raw normalization, and explicit `raw` is the
  recovery path;
- read-only-source raw compaction with disk-backed UUID audit, content/payload/partition
  reconciliation, and an interruption sentinel;
- an offline replay/compaction CLI and a GitHub Actions quality/wheel gate.

Phase 3A additionally includes:

- isolated per-exchange/per-symbol trade, price, OI, funding, mark/index, liquidation, and
  order-book state;
- event-time windows with explicit `(start, end]` boundaries, hard item limits, and configured
  late-event behavior for commutative streams;
- snapshot/update book reconstruction for feature state, bounded pre-snapshot buffering, and
  generation-specific warmup reset; the non-commutative book is always forward-only and rejects
  stale timestamps, generations, and non-advancing sequences;
- structured warmup, missing-data, stale-OI, stream-health, and pipeline-backlog blockers;
- a versioned typed `FeatureSnapshot` schema that cannot hide missing values as zero;
- the same feature-state consumer registered in live collection and offline replay.

Phase 3B additionally includes:

- deterministic 5-second, 15-second, and 60-second single-venue snapshots;
- trade-flow notional, taker imbalance, impulses, average size, and large-trade share;
- weighted depth, liquidity additions/removals, recovery rate, spread, microprice,
  depth-walk slippage, OFI, and venue-local Z-scores;
- returns, realized volatility, one-second-bucket ATR, trailing extremes, and jump flags;
- OI change/age and price/OI regime, funding/premium crowding state, and explicitly labeled
  public-sample liquidation activity;
- deterministic feature IDs, strict decision-time source bounds, and explicit rejection of a
  book containing post-decision updates;
- incremental price-window aggregates and versioned
  `feature-sources://sha256-sum-xor-v1/...` multiset commitments, avoiding repeated
  canonicalization of the complete high-frequency history;
- order-book synchronization epochs that preserve ordinary same-generation snapshot history but
  restart availability warmup after an observed loss and recovery of synchronization.

Phase 3C additionally includes:

- deterministic Binance/OKX snapshot selection at or before an explicit decision boundary;
- configurable source-age and inter-venue timestamp thresholds with typed alignment states;
- symmetric mid-price spread, spread percentage/Z-score, direction, impulse, volatility, and
  relative-spread comparisons;
- typed taker-flow, OFI, depth, liquidity, OI-context, funding, crowding, and public-liquidation
  confirmations or divergences;
- config/code/current-source lineage plus a count and SHA-256 of the exact prior paired spread
  history, all bound into deterministic cross-venue snapshot IDs;
- explicitly research-only confirmation and lead/lag fields; lead/lag remains unavailable until
  independently validated event-time history exists and never uses local arrival order.

Phase 3D additionally includes:

- atomic Zstandard Parquet under
  `feature_schema=v1/date=.../symbol=.../scope=...`;
- bounded asynchronous writes, observable backpressure, a bounded hot ID cache, and a
  restart-safe SQLite ID index rebuilt from committed Parquet truth;
- canonical typed payload JSON with SHA-256, code/config versions, source IDs, generations,
  source bounds, health/warmup, and structured reason lineage;
- strict deterministic readers with time/scope/symbol/window/health filters;
- fail-closed layout/schema/payload/partition/duplicate-ID/content-digest audits and exact
  live/replay tree comparison;
- `cvf audit-features` and `cvf compare-features` offline verification commands.

Phase 3 acceptance additionally includes:

- the same `FeatureRuntime` and `ReceiveTimeFeatureDriver` in `collect`, `replay`, and
  `accept-phase3`;
- configurable bounded receive-time reordering (250 ms by default), fail-closed late-watermark
  handling, and wall-clock feature ticks during quiet live periods;
- two independent fixed-data replays with different writer batch boundaries;
- exact logical-tree equality, schema/lineage audits, and immediate no-lookahead failure;
- observed throughput, CPU, RSS, calculation/receive/write latency, state outcomes, and safety
  evidence based on the runtime component inventory and observed output event types;
- source/settings/package-bound atomic checkpoints and `--resume`;
- JSON plus Markdown evidence, a fixed-replay stability harness, and a separate repeatable
  six-hour public-feed soak command.

The authoritative v0.3.1 fixed 30-minute acceptance completed on 2026-07-31 under
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2`. Both independent replays processed
2,331,346 raw records and produced 32,490 logically identical snapshots (21,660 single-venue
and 10,830 cross-venue) with digest
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`,
zero no-lookahead violations, 1598.962/1588.812 raw records/s, and
1.237678×/1.229822× event-time throughput across 2925.389 seconds of fixed-replay observation.
The evidence binds package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`. All 275 tests pass;
Ruff, strict mypy, and `pip check` also pass. Distribution build, clean-wheel verification, and
artifact hashes are maintained in `docs/phase3_acceptance.md`. The true continuous six-hour
public-feed soak remains pending and is
not inferred from the fixed replay. Scoring, signal production, paper execution, risk execution,
private APIs, and real orders remain unimplemented. Their former active
threshold/configuration sections have been removed; the scheduler's reserved signal boundary
does not produce a signal.

## Safety boundary

- No API keys or private account endpoints are used.
- `paper_trading_only` is type-locked to `true`.
- There is no live-order model or execution path.
- Acceptance reports list instantiated components and observed output types. They do not present
  fabricated private-network, signal, or order counters.
- `cvf` and `cvf --once` are network-free. Network access starts only with the explicit
  `cvf collect` subcommand.

## Requirements and installation

- Python 3.12+
- pip

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

The wheel includes the public-collection runtime dependencies (`httpx`, `websockets`, and
`pyarrow`). Database, analytics, and dashboard packages remain optional extras.

## Network-free inspection

Print validated subscription plans and disconnected health, then exit:

```powershell
python -m cvf --once
# or
cvf --once
```

Run the network-free shutdown harness:

```powershell
python -m cvf
```

It waits for `Ctrl+C`, `SIGINT`, or `SIGTERM` without opening an exchange connection.

## Public collection

Five-minute acceptance run:

```powershell
python -m cvf collect `
  --duration 300 `
  --output data/raw `
  --feature-output data/processed/live-5m
```

Thirty-minute soak:

```powershell
python -m cvf collect `
  --duration 1800 `
  --output data/raw `
  --feature-output data/processed/live-30m
```

`--feature-output` defaults to `storage.processed_data_path/live`. Raw and feature roots must be
disjoint. Omit `--duration` to run until a shutdown signal. Shutdown closes both connectors,
flushes buffered receive-time events and final due ticks, drains the raw and feature queues, and
closes both Parquet writers.

Raw files use UTC receipt-date partitions:

```text
data/raw/
  date=YYYY-MM-DD/
    exchange=BINANCE/
      symbol=BTC-USDT-PERP/
        channel=aggTrade/
          part-....parquet
```

Each row contains the exact frame/HTTP response bytes, stable `raw://<uuid>` reference,
transport, route metadata, both timestamps when available, sequence ID, and connection
generation. The same tree also contains `channel=_normalized_event` rows: canonical JSON for
each event after connector deduplication and normalization, plus periodic `ExchangeHealth`
events. This journal supplements rather than replaces the exact source payload. It is the
deterministic replay input only when `_collection_manifest.json` reaches an audited
`CLEAN_END`. The manifest is a clean-shutdown completeness proof, not a per-raw 0/1/N
normalization ledger and not a crash-safe WAL.

## Offline replay and compaction

Replay retained normalized events at maximum speed through the production feature runtime:

```powershell
python -m cvf replay `
  --input data/raw `
  --feature-output data/processed/replay `
  --order receive-time `
  --speed 0
```

Standard feature replay is fail-closed unless `--order receive-time` is used, because live
feature boundaries advance on local receipt time. In the default `--source-mode auto`, any
journal, manifest, compaction sentinel, or unfinished manifest evidence opts the tree into
strict validation; only a reconciled `CLEAN_END` journal is consumed. A tree with no such
evidence is legacy raw and is re-normalized from retained public payloads. Explicit
`--source-mode raw` ignores journal rows for recovery. Exact journal replay rejects partial
filters because they would break the recorded sequence. A positive `--speed` preserves
recorded receipt timing at the requested multiplier.

Replay ordering and exact UUID audits use temporary SQLite files under `.tmp` (or the OS
temporary directory fallback), so peak memory is bounded by the batch/cache settings while
temporary disk must be sized for the selected raw data and index overhead. The files and SQLite
sidecars are removed on completion, early iterator close, and failure.

Compact small raw files into a separate, fully audited tree:

```powershell
python -m cvf compact-raw `
  --input data/raw `
  --output data/raw_compacted `
  --target-rows 100000
```

The input tree is never modified. The command creates `_compaction_in_progress` before its first
output file and removes it only after the complete output audit (and clean-manifest preservation,
when applicable). An interrupted output is therefore rejected by `auto` replay and cannot look
like a complete legacy tree. The command fails unless row count, unique UUIDs, exact row-content
digest, payload bytes, lineage, and partitions agree before and after.

Audit a persisted feature tree or compare live/replay outputs:

```powershell
python -m cvf audit-features --input data/processed
python -m cvf compare-features `
  --left data/acceptance/live `
  --right data/acceptance/replay
```

Comparison is exact at the canonical payload/hash layer and allows only physical Parquet batch
and file-boundary differences.

## Phase 3 acceptance

Run the fixed wall-clock collection twice, using receive-time ordering to reproduce live
decision scheduling:

```powershell
cvf accept-phase3 `
  --input data/processed/phase3-acceptance/raw-compacted `
  --output data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2 `
  --order receive-time
```

Add `--resume` only to reuse package/settings/input-matched checkpoints; reused feature trees
are audited again. `fixed-30m-v0.3.1-final-2` is the authoritative current-source evidence.
The complete `fixed-30m-v0.3.1-final` run remains historical evidence bound to the older
package-source SHA-256
`0324bb5110eff55205298c61b788dd1ae0cab58490c1f1844b6746f2a9d5b5db`; it is not evidence for
the current package. The still earlier `fixed-30m-v0.3.1` directory is an interrupted diagnostic,
not completed acceptance evidence.

Run the repeatable stability harness with:

```powershell
cvf stability-phase3 `
  --input data/processed/phase3-acceptance/raw-compacted `
  --output data/processed/phase3-acceptance/stability-v0.3.1 `
  --target-hours 6
```

`--maximum-iterations 1` is useful for a bounded harness check, but does not satisfy the
six-hour criterion. `stability-phase3` measures repeated fixed-replay process time; it is never
reported as a continuous live-feed soak. From a GitHub source checkout, run the convenience
script with explicit, disjoint output roots:

```powershell
python scripts/run_phase3_live_soak.py `
  --output data/raw/phase3-live-6h `
  --feature-output data/processed/phase3-live-6h
```

The script is not installed by the wheel. An installed distribution uses the equivalent command:

```powershell
cvf collect --duration 21600 `
  --output data/raw/phase3-live-6h `
  --feature-output data/processed/phase3-live-6h
```

The true six-hour continuous public-feed/reconnect/resynchronization observation remains
pending. The authoritative fixed replay observed 2925.389 seconds of process time across two
runs and is not continuous-live evidence. See
[Phase 3 acceptance evidence](docs/phase3_acceptance.md).

## Configuration

`config/default.yaml` is merged with an optional overlay and then `CVF__...` environment
overrides:

```powershell
$env:CVF__LOGGING__LEVEL = "DEBUG"
$env:CVF__STORAGE__PARQUET_BATCH_ROWS = "5000"
python -m cvf collect --config config/development.yaml --duration 300
```

Important collection controls are configured rather than hard-coded: venue URLs, symbols,
channels, timeouts, heartbeat, reconnect/backoff, snapshot depth/buffer, OI cadence, stale and
latency thresholds, dedupe TTL/capacity, the receive-time reorder bound, Parquet
batch/flush/queue sizes, and output paths. Phase 4 scoring/signal and Phase 5 execution/risk
thresholds are not active configuration in v0.3.1.

## Verification

```powershell
python -m pytest -q
python -m ruff check .
python -m mypy --strict src/cvf
python -m pip check
```

The automated suite uses fixtures and mock transports; it requires no credentials and no
exchange connectivity. The latest v0.3.1 source-tree run passed 275 tests, Ruff, strict mypy,
and `pip check`. Distribution build and clean-wheel verification are recorded separately in
`docs/phase3_acceptance.md` so this packaged README does not encode transient release state.

## Project layout

```text
config/                  validated YAML defaults and overlays
docs/                    architecture, strategy, data dictionary, operations
src/cvf/
  collector.py           explicit public collection and shared Phase-3 runtime orchestration
  exchanges/             lifecycle, connectors, dedupe, symbol routing
  normalization/         typed Binance/OKX parsing and unit conversion
  orderbook/             exact venue-specific local books
  monitoring/            per-stream health accounting
  storage/               bounded raw/feature Parquet writers and audits
  acceptance/            fixed-data acceptance, checkpoints, and stability evidence
  pipeline/              bounded ordered normalized-event fan-out
  clock/                 live/replay clock and deterministic scheduler
  replay/                raw scanning, ordering, normalization, replay runner
  models/                immutable normalized records
  features/              shared live/replay runtime, bounded state, typed venue features
  strategy/              Phase-4+ boundary; not active
tests/                   unit/integration tests and versioned payload fixtures
scripts/                 raw validators and Phase 3 stability entry point
data/raw/                ignored runtime collection output
data/processed/          ignored feature/acceptance output
```

## Documentation

- [Architecture](docs/architecture.md)
- [Cross-venue feature contract](docs/cross_venue_features.md)
- [Data dictionary](docs/data_dictionary.md)
- [Feature persistence and audit](docs/feature_persistence.md)
- [Phase 3 acceptance evidence](docs/phase3_acceptance.md)
- [Operations](docs/operations.md)
- [Strategy specification](docs/strategy.md)

The implementation follows the current official
[Binance routed WebSocket notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice),
[Binance local-book procedure](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly),
and [OKX API v5 documentation](https://www.okx.com/docs-v5/en/). See
[Operations](docs/operations.md) for the recorded protocol differences and acceptance checks.

## Roadmap

1. **Phase 2 (implemented):** public collection, normalization, order books, health, raw
   Parquet, recovery, and soak validation.
2. **Phase 2.5 (implemented):** event bus, deterministic clock/scheduler, raw replay,
   compaction audit, and CI.
3. **Phase 3A (implemented):** bounded venue/symbol state, late-event rules, book-generation
   reset, feature availability, and typed snapshot schema.
4. **Phase 3B (implemented):** deterministic single-venue trade, book, price, OI, crowding,
   and public-sample liquidation features.
5. **Phase 3C (implemented):** deterministic cross-venue alignment, comparisons,
   confirmation/divergence observations, and research-only lead/lag schema.
6. **Phase 3D (implemented):** versioned feature Parquet, bounded atomic writer, strict reader,
   lineage audit, and exact live/replay logical-tree comparison.
7. **Phase 3 acceptance (fixed 30-minute replay passed; six-hour live observation pending):**
   repeatable deterministic replay, no-lookahead, performance/resource evidence, checkpoints,
   and reports.
8. **Phase 4:** scores and entry/exit/hold/no-trade signals.
9. **Phase 5:** order-book-based paper fills and risk controls.
10. **Phase 6:** backtests, evaluation, and parameter sensitivity.
11. **Phase 7:** a small monitoring dashboard.

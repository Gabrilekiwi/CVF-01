# Cross-Venue Short-Term Flow Strategy (CVF-01)

CVF-01 is a research-first, paper-trading-only system for short-horizon Binance and OKX
USDT perpetual research. Version 0.3.0 completes the Phase 3 feature pipeline: deterministic
single-venue and cross-venue snapshots, audited Parquet persistence, fixed-data replay
acceptance, resumable checkpoints, and a repeatable stability harness. It still does not score
markets, generate signals, or place orders.

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
- filtered raw Parquet reads with stable event-time or receive-time ordering;
- replay through the same Binance/OKX normalizers and normalized-event bus;
- read-only-source raw compaction with UUID, content hash, payload, and partition audit;
- an offline replay/compaction CLI and a GitHub Actions quality/wheel gate.

Phase 3A additionally includes:

- isolated per-exchange/per-symbol trade, price, OI, funding, mark/index, liquidation, and
  order-book state;
- event-time windows with explicit `(start, end]` boundaries, hard item limits, and configured
  late-event behavior;
- snapshot/update book reconstruction for feature state, bounded pre-snapshot buffering, and
  generation-specific warmup reset;
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
  book containing post-decision updates.

Phase 3C additionally includes:

- deterministic Binance/OKX snapshot selection at or before an explicit decision boundary;
- configurable source-age and inter-venue timestamp thresholds with typed alignment states;
- symmetric mid-price spread, spread percentage/Z-score, direction, impulse, volatility, and
  relative-spread comparisons;
- typed taker-flow, OFI, depth, liquidity, OI-context, funding, crowding, and public-liquidation
  confirmations or divergences;
- config/code/source lineage and deterministic cross-venue snapshot IDs;
- explicitly research-only confirmation and lead/lag fields; lead/lag remains unavailable until
  independently validated event-time history exists and never uses local arrival order.

Phase 3D additionally includes:

- atomic Zstandard Parquet under
  `feature_schema=v1/date=.../symbol=.../scope=...`;
- bounded asynchronous writes, observable backpressure, and bounded snapshot-ID deduplication;
- canonical typed payload JSON with SHA-256, code/config versions, source IDs, generations,
  source bounds, health/warmup, and structured reason lineage;
- strict deterministic readers with time/scope/symbol/window/health filters;
- schema, payload, partition, duplicate-ID, content-digest, and live/replay tree audits;
- `cvf audit-features` and `cvf compare-features` offline verification commands.

Phase 3 acceptance additionally includes:

- two independent fixed-data replays with different writer batch boundaries;
- exact logical-tree equality, schema/lineage audits, and immediate no-lookahead failure;
- observed throughput, CPU, RSS, calculation/receive/write latency, state outcomes, and safety
  counters;
- source/settings/package-bound atomic checkpoints and `--resume`;
- JSON plus Markdown evidence, and a repeatable six-hour stability command.

Scoring, signals, paper trading, private APIs, and real orders remain unimplemented.

## Safety boundary

- No API keys or private account endpoints are used.
- `paper_trading_only` is type-locked to `true`.
- There is no live-order model or execution path.
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
python -m cvf collect --duration 300 --output data/raw
```

Thirty-minute soak:

```powershell
python -m cvf collect --duration 1800 --output data/raw
```

Omit `--duration` to run until a shutdown signal. Shutdown closes both connectors, drains the
raw queue, flushes the final partial batch, and closes Parquet files.

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
generation.

## Offline replay and compaction

Replay retained payloads at maximum speed through the live normalizers and event bus:

```powershell
python -m cvf replay --input data/raw --speed 0
```

Use `--start`, `--end`, `--exchange`, `--symbol`, and `--channel` to filter. Choose
`--order event-time` or `--order receive-time`. A positive `--speed` preserves original
timing at the requested multiplier.

Compact small raw files into a separate, fully audited tree:

```powershell
python -m cvf compact-raw `
  --input data/raw `
  --output data/raw_compacted `
  --target-rows 100000
```

The input tree is never modified. The command fails unless row count, unique UUIDs, exact
row-content digest, payload bytes, lineage, and partitions agree before and after.

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
  --output data/processed/phase3-acceptance/fixed-30m-v0.3.0 `
  --order receive-time
```

Add `--resume` only to reuse package/settings/input-matched checkpoints; reused feature trees
are audited again. Run the repeatable stability harness with:

```powershell
cvf stability-phase3 `
  --input data/processed/phase3-acceptance/raw-compacted `
  --output data/processed/phase3-acceptance/stability-v0.3.0 `
  --target-hours 6
```

`--maximum-iterations 1` is useful for a bounded harness check, but does not satisfy the
six-hour criterion. The longest retained fixed-data acceptance passed determinism,
no-lookahead, throughput, schema/lineage, and safety checks; a true six-hour continuous
live/reconnect observation remains pending. See
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
latency thresholds, dedupe TTL/capacity, Parquet batch/flush/queue sizes, and output path.

## Verification

```powershell
python -m pytest -q
python -m ruff check .
python -m mypy --strict src/cvf
python -m pip check
```

The automated suite uses fixtures and mock transports; it requires no credentials and no
exchange connectivity.

## Project layout

```text
config/                  validated YAML defaults and overlays
docs/                    architecture, strategy, data dictionary, operations
src/cvf/
  collector.py           explicit phase-2 orchestration
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
  features/              bounded state, availability, typed snapshots, venue features
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
7. **Phase 3 acceptance (fixed-data passed; six-hour live observation pending):** repeatable
   deterministic replay, no-lookahead, performance/resource evidence, checkpoints, and reports.
8. **Phase 4:** scores and entry/exit/hold/no-trade signals.
9. **Phase 5:** order-book-based paper fills and risk controls.
10. **Phase 6:** backtests, evaluation, and parameter sensitivity.
11. **Phase 7:** a small monitoring dashboard.

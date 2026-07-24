# Cross-Venue Short-Term Flow Strategy (CVF-01)

CVF-01 is a research-first, paper-trading-only system for short-horizon Binance and OKX
USDT perpetual signals. Phase 2 implements public market-data collection only: it does not
generate factors or signals and cannot place orders.

## Current status

Phase 2 includes:

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

Phase 3 features, scoring, signals, paper trading, private APIs, and real orders are not
implemented.

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
python -m mypy src/cvf
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
  storage/               bounded asynchronous raw Parquet writer
  models/                immutable normalized records
  features/, strategy/   future phase boundaries; not active
tests/                   unit/integration tests and versioned payload fixtures
data/raw/                ignored runtime collection output
```

## Documentation

- [Architecture](docs/architecture.md)
- [Data dictionary](docs/data_dictionary.md)
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
2. **Phase 3:** per-venue and cross-venue features.
3. **Phase 4:** scores and entry/exit/hold/no-trade signals.
4. **Phase 5:** order-book-based paper fills and risk controls.
5. **Phase 6:** deterministic replay and backtests.
6. **Phase 7:** a small monitoring dashboard.

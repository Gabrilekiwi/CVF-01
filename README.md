# Cross-Venue Short-Term Flow Strategy (CVF-01)

CVF-01 is a research-first, paper-trading-only system for short-horizon Binance and
OKX USDT perpetual signals. The intended decision horizon is 3–30 minutes, with
high-frequency public market data feeding one-second features and five-second signal checks.

## Current status

Phase 1 is implemented:

- strict YAML configuration with `CVF__...` environment overlays;
- normalized Pydantic models for market events, health, signals, orders, positions, and fills;
- exact BTC/ETH canonical symbol mappings for Binance and OKX;
- an abstract connector contract plus Binance/OKX subscription planners;
- structured JSON logging;
- a signal-aware minimal process entry point;
- initial model, configuration, symbol-mapping, connector, and entry-point tests.

The connector classes are intentionally **network-disabled skeletons**. They report
`DISCONNECTED`, and calling `connect()` raises a clear phase-2 error. There is no live order
code, no private-account integration, and no path that can send a real order.

## Safety boundary

- Public market-data plans only; no API key fields are accepted.
- `paper_trading_only` is type-locked to `true`.
- Martingale and adding to losing positions are type-locked to `false`.
- The MVP configuration permits at most one simulated position.
- Phase 1 does not claim to collect, normalize, score, or trade live data.

## Requirements

- Python 3.12+
- pip
- Docker is optional and only needed for the PostgreSQL development service.

## Install

PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

For only the phase-1 runtime:

```powershell
python -m pip install -e .
```

## Run

Perform one deterministic startup/health pass and exit:

```powershell
python -m cvf --once
```

or, after installation:

```powershell
cvf --once
```

Run until `Ctrl+C`, `SIGINT`, or `SIGTERM`:

```powershell
python -m cvf
```

The long-running phase-1 process does not open network connections. It prints planned
subscriptions, reports both connectors as `DISCONNECTED`, and waits so shutdown handling can
be tested safely.

Use the development overlay:

```powershell
python -m cvf --config config/development.yaml --once
```

## Configuration

`config/default.yaml` is the complete research configuration. A selected YAML file is merged
on top of it, followed by environment-variable overrides.

Double underscores select nested keys:

```powershell
$env:CVF__LOGGING__LEVEL = "DEBUG"
$env:CVF__APP__STATUS_INTERVAL_SECONDS = "2.5"
python -m cvf --once
```

`CVF_CONFIG_FILE=config/development.yaml` selects an overlay without a CLI flag. Values are
parsed as YAML scalars, so booleans and numbers remain typed.

## Test

```powershell
python -m pytest -q
```

Optional checks after installing the `dev` extra:

```powershell
python -m ruff check .
python -m mypy
```

## Optional PostgreSQL service

SQLite remains the default for local development. To start the future PostgreSQL backend:

```powershell
docker compose up -d postgres
```

Then override:

```powershell
$env:CVF__STORAGE__DATABASE_URL = "postgresql+asyncpg://cvf:cvf_development_only@localhost:5432/cvf"
```

No storage adapter is implemented in phase 1; this service definition reserves the intended
development topology.

## Project layout

```text
config/                  versioned YAML defaults and overlays
docs/                    architecture, strategy, data dictionary, operations
src/cvf/
  models/                normalized immutable Pydantic records
  exchanges/             connector contract, planners, symbol mapping
  normalization/         venue payload mapping (phase 2)
  orderbook/             local book reconstruction (phase 2)
  storage/               buffered Parquet/database writes (phase 2)
  features/              single-venue and cross-venue factors (phase 3)
  strategy/              scoring and signal state machine (phase 4)
  risk/                  portfolio/risk gates (phase 5)
  execution/             cost and fill estimation (phase 5)
  paper_trading/         simulated account lifecycle (phase 5)
  replay/, backtest/     shared-pipeline historical evaluation (phase 6)
  monitoring/            console/web status (phase 7)
tests/                   unit, integration, and future payload fixtures
data/                    ignored runtime raw/processed output
```

## Documentation

- [Architecture](docs/architecture.md)
- [Strategy specification](docs/strategy.md)
- [Data dictionary](docs/data_dictionary.md)
- [Operations](docs/operations.md)

The venue channel plan is based on the current official
[Binance derivatives documentation](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/Introduction)
and [OKX API v5 documentation](https://www.okx.com/docs-v5/en/). Phase 2 must pin fixture
payloads and record any differences discovered while implementing normalization.

## Roadmap

1. **Phase 2:** real public WebSocket/REST collection, reconnection, book resync, health, and
   buffered raw storage.
2. **Phase 3:** trade imbalance, OFI, price/OI impulse, crowding, liquidation, and
   cross-venue features.
3. **Phase 4:** per-venue scoring, combined scoring, entry/exit/hold/no-trade signals.
4. **Phase 5:** order-book-based paper fills, account/risk controls, fees, funding, exits.
5. **Phase 6:** deterministic replay, backtest metrics, sensitivity analysis.
6. **Phase 7:** a small monitoring dashboard.

Every phase must reuse the same normalized models and downstream pipeline, add tests, and pass
its own acceptance checks before the next phase begins.


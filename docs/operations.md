# CVF-01 Phase-1 Operations

## Startup check

```powershell
python -m cvf --once
```

Expected result:

- process exit code `0`;
- one `application_initialized` JSON record;
- one subscription plan and one health record per enabled venue;
- both health states are `DISCONNECTED`;
- `network_attempted` is `false`;
- final `one_shot_complete` record.

`DISCONNECTED` is correct in phase 1. A `CONNECTED` claim would be a defect because network
collection has not been implemented.

## Long-running shutdown check

```powershell
python -m cvf
```

Press `Ctrl+C`. Expected logs include `shutdown_requested` (where the platform exposes the
signal to Python) followed by `application_stopped`, and the process returns without a
traceback. `SIGTERM` uses the same path on platforms that support it.

## Configuration precedence

1. `config/default.yaml`;
2. `--config PATH`, or `CVF_CONFIG_FILE` when the flag is absent;
3. `CVF__SECTION__KEY` environment values.

Invalid YAML, unknown keys, type mismatches, symbol-map gaps, inconsistent weights, or disabled
safety invariants stop startup with exit code `2`.

## Tests

```powershell
python -m pytest -q
```

The suite requires no exchange connectivity and no credentials.

## Common problems

### `No module named cvf`

Install the project from its root:

```powershell
python -m pip install -e .
```

### PowerShell blocks virtual-environment activation

Use the interpreter directly:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m cvf --once
```

### Docker is unavailable

Docker is optional in phase 1. Keep the default SQLite URL; storage itself begins in phase 2.

### Connector `connect()` raises `ConnectorNotImplementedError`

This is intentional fail-closed behavior. Phase 1 permits construction, planning, and health
inspection only. Do not suppress the error or relabel the connector connected.

## Phase-2 operational work

Before real public data collection is accepted:

- capture versioned Binance/OKX payload fixtures;
- test heartbeat, reconnect/backoff, deduplication, and sequence gaps;
- prove snapshot/resync behavior;
- record clock/receive latency;
- batch raw writes and measure queue lag;
- run a multi-hour soak without private credentials;
- document current official API differences and rate limits.


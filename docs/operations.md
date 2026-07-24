# CVF-01 Phase-2/2.5 Operations

## Install and preflight

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m cvf --once
```

`--once` must exit `0`, report planned subscriptions as `DISCONNECTED`, and state
`network_attempted=false`. It never opens a socket or sends HTTP.

Run the deterministic gate:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m mypy src/cvf
.\.venv\Scripts\python.exe -m pip check
```

## Collection commands

Five-minute functional acceptance:

```powershell
.\.venv\Scripts\python.exe -m cvf collect --duration 300 --output data/raw
```

Thirty-minute soak:

```powershell
.\.venv\Scripts\python.exe -m cvf collect --duration 1800 --output data/raw
```

Indefinite run:

```powershell
.\.venv\Scripts\python.exe -m cvf collect --output data/raw
```

Press `Ctrl+C` once. A clean run logs `collection_complete` only after connectors and the raw
writer have closed.

## Acceptance checks

For both finite runs:

- Binance and OKX connect without credentials;
- BTC and ETH normalized events appear from both venues;
- Binance depth and OKX books leave `RESYNCING` after valid snapshots;
- `parquet.accepted_records == parquet.written_records`;
- `parquet.last_error` is null;
- Parquet files exist under both exchange partitions;
- raw payload references are unique and their bytes are nonempty;
- sequence/checksum failures, reconnects, duplicates, and backpressure are reported rather
  than hidden;
- shutdown returns without pending-task warnings or a traceback.

The 30-minute run additionally checks that memory does not grow with message count due to
dedupe or book buffers and that partial batches flush on time.

## Recorded live acceptance (2026-07-24)

The checked-in Phase-2 implementation was exercised against the public production endpoints:

- 5 minutes: 347,433 accepted and written raw rows in 1,437 files; zero gaps, checksum
  failures, reconnects, parse errors, drops, backpressure, or writer errors.
- 30 minutes: 2,331,346 accepted and written raw rows in 8,362 files and 379 flushes;
  queue depth returned to zero, with zero gaps, checksum failures, parse errors, drops,
  backpressure, or writer errors.
- The 30-minute audit reopened every file and checked 453,728,434 payload bytes, unique record
  UUIDs, exact `raw://<record_id>` lineage, physical schema, partition agreement, and the
  absence of temporary files.
- Observed process memory warmed from roughly 128 MiB into a 199–222 MiB band and fell to
  roughly 206 MiB near shutdown while rows continued increasing, rather than growing with
  cumulative message count.
- One Binance public WebSocket reconnect recovered through automatic resubscription and a
  fresh depth snapshot. One later Binance OI REST connect timeout also recovered without
  interrupting WebSocket collection. That live fault exposed and prompted a regression-tested
  fix so OI REST failure now degrades only OI health keys.

`DEGRADED` or transient `STALE` status can still be expected under the intentionally strict
latency/freshness thresholds, especially for event-driven OKX OI/funding/index channels. It is
an observable health result, not a hidden drop; continuity and persistence counters determine
whether the data path itself failed.

## Inspect captured Parquet

```powershell
.\.venv\Scripts\python.exe scripts\validate_raw_collection.py data\raw
```

The validator scans payloads in bounded batches, opens every Parquet file, checks physical
columns, unique UUIDs, nonempty exact payload bytes, `raw://<record_id>` lineage,
row/partition agreement, and the absence of unfinished `.tmp` files. Add
`--expected-rows N` when reconciling against the final `collection_complete` record.

## Offline replay

```powershell
.\.venv\Scripts\python.exe -m cvf replay --input data\raw --speed 0
```

Replay is network-free. It retains raw lineage and connection generations, supports bounded
filters, and uses a stable tie-break rule after event time or local receive time. A positive
speed preserves recorded timing; zero runs as fast as processing allows.

## Raw compaction

```powershell
.\.venv\Scripts\python.exe -m cvf compact-raw `
  --input data\raw `
  --output data\raw_compacted `
  --target-rows 100000
```

Input and output must be disjoint, and a nonempty output is rejected. The command audits both
trees and succeeds only when row count, UUID uniqueness, exact content digest, payload bytes,
lineage, and partitions agree. The source tree remains read-only.

## Current official protocol decisions

- Binance USDⓈ-M uses root `wss://fstream.binance.com`, with depth/BBO on `/public` and
  aggregate trades, mark/funding, and force orders on `/market`. The previous unrouted
  endpoint is not used. See the
  [official routing notice](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Important-WebSocket-Change-Notice).
- Binance connections are treated as renewable, and protocol ping/pong is handled by the
  client. Limits and lifetime come from the
  [official connect documentation](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect).
- Binance local depth follows the
  [official local-book procedure](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).
- OKX sends text `ping` after a quiet receive deadline and requires `pong`; notice/error
  control frames force recovery.
- OKX production `books` checksum was deprecated on 2026-06-23 and is now zero. Sequence IDs
  are authoritative; historical nonzero fixtures retain CRC32 validation. See the
  [official OKX change log](https://my.okx.com/docs-v5/log_en/).
- OKX `liquidation-orders` is subscribed once at `instType=SWAP` because the channel can
  broadcast instruments beyond the requested trading set. The exact complete frame is stored
  under raw symbol `*`; normalization then admits only configured instruments and ignores all
  others.
- OKX derivatives sizes are contracts. Live public instrument metadata (`ctVal`, `ctMult`,
  `ctValCcy`) converts them to canonical base quantity.

## Common failures

### Binance connects but market events are absent

Confirm the configured root is `wss://fstream.binance.com` and logs show both `/public` and
`/market` routes.

### Binance depth stays `RESYNCING`

Check public REST reachability, snapshot response status, buffer overflow, and `pu` gap
counters. Do not mark the book healthy manually.

### OKX immediately reconnects after subscribe

Inspect the preserved control payload for `event=error` and its code/message. Subscription IDs
must remain alphanumeric.

### OKX checksum is always zero

This is the expected current production behavior. Sequence continuity is still mandatory.

### Parquet producer appears slow

Inspect queue depth and `backpressure_events`. Increasing queue capacity only postpones a
sustained throughput problem; verify disk performance and batch/flush settings.

### Collection exits with a writer error

Treat the run as incomplete. Raw producer failures are fatal by design, because continuing
would create an unobservable data hole.

## Configuration precedence

1. `config/default.yaml`;
2. `--config PATH` or `CVF_CONFIG_FILE`;
3. `CVF__SECTION__KEY` overrides.

Unknown keys, invalid symbols, unsafe booleans, malformed YAML, or inconsistent cross-section
settings stop before collection.

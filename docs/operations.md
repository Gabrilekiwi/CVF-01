# CVF-01 Operations

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
.\.venv\Scripts\python.exe -m mypy --strict src/cvf
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

## Feature persistence audit

Feature writers use the processed root and always add the version partition:

```text
data/processed/feature_schema=v1/date=.../symbol=.../scope=...
```

Audit a full tree:

```powershell
.\.venv\Scripts\python.exe -m cvf audit-features `
  --input data\processed
```

Filters are inclusive and may repeat:

```powershell
.\.venv\Scripts\python.exe -m cvf audit-features `
  --input data\processed `
  --start 2026-07-27T00:00:00Z `
  --end 2026-07-27T00:30:00Z `
  --scope CROSS_VENUE `
  --symbol BTC-USDT-PERP `
  --window 5
```

Compare separately generated live and replay feature trees:

```powershell
.\.venv\Scripts\python.exe -m cvf compare-features `
  --left data\acceptance\live `
  --right data\acceptance\replay
```

Exit `0` means logical content is exactly equal. Different Parquet file counts and batch
boundaries are permitted; IDs, canonical payload hashes, code/config lineage, partitions,
scopes, and decision-time bounds must match. A mismatch exits `1`.

Treat any schema, payload hash, metadata/payload, partition, duplicate-ID, or consistency error
as a failed run. Do not manually edit or merge feature files.

## Phase 3 fixed-data acceptance

The retained 30-minute collection is a wall-clock capture, so its final acceptance uses local
receive-time ordering. Exchange timestamps remain in every event and are still enforced by
decision-time source bounds and the no-lookahead check.

```powershell
cvf accept-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\fixed-30m-v0.3.0 `
  --order receive-time
```

The command audits the raw tree, executes two independent replays with writer batches of 1,000
and 777 rows, audits both feature trees, compares their exact logical content, and writes
machine-readable JSON plus a Markdown summary. It fails immediately on a source timestamp after
its decision boundary. Use `--resume` only after interruption: checkpoints are accepted only
when the input path, package-source hash, complete settings fingerprint, replay order, writer
configuration, and output paths match, and completed trees are audited again.

The retained raw tree was compacted without modifying its source:

- 8,362 files to 53 files across 34 partitions;
- 2,331,346 rows and 2,331,346 unique IDs before and after;
- 453,728,434 exact payload bytes before and after;
- content digest
  `d001dddf13ef8c797117a5a3de0e0b49a519d8b388f6cd4a96514e578e894ecd`.

The recorded pre-release implementation run produced 32,478 audited snapshots in each replay
(21,642 single-venue and 10,836 cross-venue), zero no-lookahead violations, zero signals,
orders, or private-API calls, and exact logical digest
`782a997fc98e6ac3ce1f8a5ade0c5943fc9cdeb9939d84bc71a0fe1bb31b575e` in both trees.
The two physical trees deliberately differed (198 versus 252 files) while their logical records
remained identical. Replay throughput was 1,666 and 1,511 raw records/second, respectively,
or 1.29× and 1.17× the captured wall-clock rate.

This evidence also exposes a real limitation: all retained fixed-set snapshots have at least one
structured unavailable reason. The configured Z-score history is 1,800 seconds, while the
decision span is only about 1,805 seconds and some required metric histories start later. The
fixed set therefore proves deterministic unavailable-path behavior, formula/reference paths in
tests, persistence, and performance; it does not prove a fully warm and aligned live
cross-venue path.

## Phase 3 stability observation

Run repeated acceptance toward a six-hour process-lifetime target:

```powershell
cvf stability-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\stability-v0.3.0 `
  --target-hours 6
```

`--maximum-iterations 1` verifies the harness and emits an honest pending result, but is not a
six-hour acceptance. The recorded two-replay observation totals about 49.04 minutes. A true
six-hour continuous live-feed/reconnect/resynchronization observation has not been performed
and remains a release limitation; repeated fixed-data process time must not be described as
equivalent evidence.

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

# CVF-01 Operations

These commands target the v0.3.1 corrective release. The published v0.3.0 release remains a
historical artifact; its recorded fixed-data results are retained below but do not substitute
for the completed corrective v0.3.1 fixed-data acceptance or the still-pending live soak.

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
.\.venv\Scripts\python.exe -m cvf collect `
  --duration 300 `
  --output data\raw\live-5m `
  --feature-output data\processed\live-5m
```

Thirty-minute soak:

```powershell
.\.venv\Scripts\python.exe -m cvf collect `
  --duration 1800 `
  --output data\raw\live-30m `
  --feature-output data\processed\live-30m
```

Indefinite run:

```powershell
.\.venv\Scripts\python.exe -m cvf collect `
  --output data\raw\live `
  --feature-output data\processed\live
```

Raw and feature roots must be disjoint. `--feature-output` otherwise defaults to
`storage.processed_data_path/live`. Press `Ctrl+C` once. A clean run logs
`collection_complete` only after connectors close, the receive-time driver flushes buffered
events/final due ticks, and both writers close.

Collection uses the same `FeatureRuntime` and `ReceiveTimeFeatureDriver` as standard replay and
acceptance. The driver reorders within `features.receive_time_reorder_ms` (250 ms by default),
has a hard capacity, fails events at or behind its watermark, and advances from wall clock during
quiet periods.

## Acceptance checks

For both finite runs:

- Binance and OKX connect without credentials;
- BTC and ETH normalized events appear from both venues;
- Binance depth and OKX books leave `RESYNCING` after valid snapshots;
- `parquet.accepted_records == parquet.written_records`;
- `parquet.last_error` is null;
- `feature_runtime.writer.last_error` is null and feature queue depth returns to zero;
- Parquet files exist under both exchange partitions;
- raw payload references are unique and their bytes are nonempty;
- accepted post-dedup events and periodic `ExchangeHealth` records appear under
  `channel=_normalized_event`;
- `_collection_manifest.json` is claimed exclusively before producers start, remains
  `IN_PROGRESS` after any failed or cancelled run, and reaches `CLEAN_END` only after
  raw/journal counts, digests, and terminal watermarks reconcile;
- sequence/checksum failures, reconnects, duplicates, and backpressure are reported rather
  than hidden;
- shutdown returns without pending-task warnings or a traceback.

The 30-minute run additionally checks that memory does not grow with message count due to
dedupe, reorder, feature-history, or book buffers and that partial raw/feature batches flush on
time.

## Historical v0.3.0-era live acceptance (2026-07-24)

The Phase-2 collector was exercised against public production endpoints before the v0.3.1
shared feature runtime and normalized journal existed:

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

For a v0.3.1 collection, also confirm `_normalized_event` partitions exist. Original public
payload rows remain exact bytes. Internal journal rows contain canonical typed JSON for events
after connector deduplication/normalization, including collector-generated `ExchangeHealth`;
they do not replace the source frames.

## Offline replay

```powershell
.\.venv\Scripts\python.exe -m cvf replay `
  --input data\raw\live `
  --feature-output data\processed\replay `
  --order receive-time `
  --speed 0
```

Replay is network-free and writes features through the production `FeatureRuntime`. Standard
feature replay rejects event-time order; receive time is required to reproduce the live
decision timeline. When `_normalized_event` exists, replay validates row/event metadata and
uses that exact post-dedup sequence. A legacy tree without the journal is re-normalized through
the live venue normalizers. Journal replay rejects every start/end/exchange/symbol/channel
filter because any partial selection would break the captured sequence; use such filters only
with explicit raw recovery/research replay. A positive speed preserves recorded receipt timing;
zero runs as fast as processing allows.

## Raw compaction

```powershell
.\.venv\Scripts\python.exe -m cvf compact-raw `
  --input data\raw `
  --output data\raw_compacted `
  --target-rows 100000
```

Input and output must be disjoint, and a nonempty output is rejected. Before its first output
row the command exclusively creates `_compaction_in_progress`. It removes the sentinel only
after the output audit and any clean-manifest preservation/revalidation succeed; interruption
leaves the sentinel so `auto` replay fails closed rather than consuming a partial legacy tree.
The command audits both trees and succeeds only when row count, UUID uniqueness, exact content
digest, payload bytes, lineage, and partitions agree. Ordering and UUID audit use bounded
disk-backed SQLite scratch space; the source tree remains read-only.

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
as a failed run. A nonexistent root, missing/unknown schema tree, Parquet outside
`feature_schema=v1`, no Parquet files, or zero physical rows is also a failed audit. A valid
nonempty tree may legitimately produce zero filtered rows. Do not manually edit or merge
feature files.

The writer keeps `.feature-deduplication-v1.sqlite3` beside the schema tree. It is a disposable
restart-safe ID/content index rebuilt from committed Parquet on every writer start; Parquet is
the source of truth. Rebuild removes stale SQLite journal/WAL/SHM sidecars before opening the
new index. A single writer owns one feature root; deleting or copying only the sidecar is not a
backup/restore procedure.

## Phase 3 fixed-data acceptance

The retained 30-minute collection is a wall-clock capture, so its final acceptance uses local
receive-time ordering. Exchange timestamps remain in every event and are still enforced by
decision-time source bounds and the no-lookahead check.

```powershell
cvf accept-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\fixed-30m-v0.3.1-final-2 `
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

The authoritative v0.3.1 run completed on 2026-07-31 under
`fixed-30m-v0.3.1-final-2`. Both checkpoints reached `AUDIT_COMPLETE`, and both trees contained
32,490 unique audited snapshots (21,660 single-venue and 10,830 cross-venue). Their exact
logical digest was
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`.
The 1,000-row and 777-row writer batches produced 204 and 252 physical files respectively while
remaining logically identical. Replay wall time was 1,458.037 and 1,467.352 seconds. Raw
throughput was 1,598.962 and 1,588.812 records/second, or 1.237678× and 1.229822× event time.
Average feature calculation latency was 426.751 and 427.929 ms; maxima were 2,306.487 and
5,178.535 ms. Maximum writer file latency was 1,141 and 734 ms. Both runs reported zero
no-lookahead violations, zero writer/runtime backpressure, zero forbidden output events, and an
exact feature-only component inventory. Peak RSS was 6,070,730,752 and 6,492,758,016 bytes. Both
metrics checkpoints bind
package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`
and settings SHA-256
`a73a6cd8d7e14abbbe14cb6e631df084b730ef223f2431c62b5c67d99aa12610`.
The SHA-256 values of both metrics files and both summaries are recorded in
`docs/phase3_acceptance.md`.

The `fixed-30m-v0.3.1` directory is an interrupted diagnostic attempt that advanced only
about 352 decision seconds and never completed two-run comparison; do not resume, merge, or cite
it as acceptance evidence.

The completed `fixed-30m-v0.3.1-final` directory is retained as superseded pre-final-source
historical evidence. It binds package-source SHA-256
`0324bb5110eff55205298c61b788dd1ae0cab58490c1f1844b6746f2a9d5b5db`
and is not the current authority. Do not confuse it with the interrupted directory above or
with the authoritative `fixed-30m-v0.3.1-final-2` result.

The historical v0.3.0 release-candidate run produced 32,478 audited snapshots in each replay
(21,642 single-venue and 10,836 cross-venue) and reported exact logical digest
`e885ba7f1c2305bbf7d768e2ae56fe8dd702bfce3f568faebceeb620424c92c4` in both trees.
The two physical trees deliberately differed (198 versus 252 files) while their logical records
remained identical. Replay throughput was 1,654 and 1,573 raw records/second, respectively,
or 1.28× and 1.22× the captured wall-clock rate. Both audited trees report code version
`0.3.0`.

Those v0.3.0 counts/digest remain historical only. Its reported zero signal/order/private-API
counters were hard-coded fields and are not valid evidence. v0.3.1 replaces them with an
allowlisted component inventory and observed output event types; it states explicitly that
network requests are not instrumented counters. The old no-lookahead field also did not cover
the corrected health-as-of and complete cross-history identity semantics. The v0.3.1 final-source
fixed-data counts, digest, and performance are the authoritative values above. The final
sdist/wheel build and isolated clean-wheel `cvf --once` verification pass; artifact hashes are
recorded in `docs/phase3_acceptance.md`.

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
  --output data\processed\phase3-acceptance\stability-v0.3.1 `
  --target-hours 6
```

`--maximum-iterations 1` verifies the harness and emits an honest pending result, but is not a
six-hour acceptance. The corrective v0.3.1 final-source fixed acceptance observed 2,925.389
process seconds across two replays; this is still only fixed-replay evidence.
`stability-phase3` records fixed-replay target and actual wall time separately and always leaves
continuous-live-soak completion false.

From a GitHub source checkout, run the distinct continuous public-feed convenience harness with
explicit, disjoint roots:

```powershell
.\.venv\Scripts\python.exe scripts\run_phase3_live_soak.py `
  --output data\raw\phase3-live-6h `
  --feature-output data\processed\phase3-live-6h
```

The script is not part of the wheel. From an installed distribution, run the equivalent public
collector command:

```powershell
cvf collect --duration 21600 `
  --output data\raw\phase3-live-6h `
  --feature-output data\processed\phase3-live-6h
```

The script supplies `--duration 21600` unless overridden and then uses the ordinary `collect`
path, including the shared feature runtime, reconnect/resync handling, normalized journal, and
wall-clock quiet ticks. A true six-hour v0.3.1 live-feed/reconnect/resynchronization observation
has not yet been performed and remains pending; fixed replay must not be described as equivalent
evidence. The larger final-source replay peaked at about 6.05 GiB and grew by about 5.10 GiB,
which does not establish six-hour live memory stability. Use a dedicated machine with enough
memory headroom and retain the complete raw, feature, manifest, and process-monitoring outputs.

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

### Feature replay rejects ordering or a late event

Use `--order receive-time`. An event at or behind the driver watermark means it exceeded the
configured reorder guarantee; the run fails closed instead of silently moving the event across
a decision boundary. Diagnose source ordering before increasing
`features.receive_time_reorder_ms`.

## Configuration precedence

1. `config/default.yaml`;
2. `--config PATH` or `CVF_CONFIG_FILE`;
3. `CVF__SECTION__KEY` overrides.

Unknown keys, invalid symbols, unsafe booleans, malformed YAML, or inconsistent cross-section
settings stop before collection.

The active v0.3.1 configuration contains collection, feature, health, storage, pipeline, and
replay controls. It intentionally contains no Phase 4 score/LONG/SHORT signal thresholds and no
Phase 5 execution, exit, or risk thresholds. `timing.signal_check_seconds` reserves scheduler
boundaries only; it does not activate a signal path.

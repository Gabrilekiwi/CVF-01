# Phase 3 Acceptance Evidence

## Current status

Phase 3A through 3D are implemented in the `v0.3.1` corrective release. The correction
unifies live collection, standard replay, and acceptance behind the same receive-time feature
runtime and adds fail-closed collection, replay, persistence, and lineage contracts. The formal
fixed 30-minute `v0.3.1` acceptance completed successfully on 2026-07-31.

The release gates pass 275 tests, Ruff, strict mypy across 72 source files, `pip check`, the final
sdist/wheel build, and an isolated clean-wheel offline CLI smoke test. The following evidence
remains pending and must not be inferred from either the fixed replay or build artifacts:

- a continuous six-hour public-feed collection with reconnect/resynchronization and memory
  evidence;
- a new matching-lineage `CLEAN_END` capture proving exact live-journal replay equivalence;
- a sustained fully warm, healthy, and aligned cross-venue path.

The published `v0.3.0` tag and release remain immutable historical artifacts. Their recorded
counts and hashes are retained below, but the post-release audit found correctness and evidence
gaps that prevent them from serving as current Phase 3 acceptance proof.

## Reproducible commands

Run the corrective fixed-data acceptance into a new empty output:

```powershell
cvf accept-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\fixed-30m-v0.3.1-final-2 `
  --order receive-time
```

Resume only an interrupted matching run:

```powershell
cvf accept-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\fixed-30m-v0.3.1-final-2 `
  --order receive-time `
  --resume
```

The `fixed-30m-v0.3.1` directory is an interrupted performance-diagnostic attempt, not acceptance
evidence: it advanced only about 352 decision seconds and ended with 6,337 committed rows plus
an uncommitted partial batch. It has no completed two-run summary.

The completed `fixed-30m-v0.3.1-final` directory is not interrupted, but it binds the superseded
pre-final-source package SHA-256
`0324bb5110eff55205298c61b788dd1ae0cab58490c1f1844b6746f2a9d5b5db`.
It is retained as historical evidence for that source state and is no longer the current
acceptance authority. Only `fixed-30m-v0.3.1-final-2` and its reports below are authoritative for
the final source state.

The retained input predates the exact normalized-event journal and collection manifest.
`auto` therefore resolves it as legacy `raw` input and re-normalizes it through the current
venue normalizers. This can prove deterministic current-code replay over the retained bytes; it
cannot prove exact equivalence to the historical live post-dedup event sequence.

For a new journal-backed capture, acceptance first requires an audited `CLEAN_END` manifest and
an exact match between capture and replay code version, package-source SHA-256, strategy
version, and settings SHA-256. Any journal, manifest, incomplete-compaction, or unfinished-run
evidence opts `auto` into strict validation rather than silently falling back to raw replay.

Run the repeatable fixed-replay stress harness separately:

```powershell
cvf stability-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\stability-v0.3.1 `
  --target-hours 6
```

This command records requested and actual process time separately and never marks the continuous
live-soak criterion complete. The true live observation uses
`scripts/run_phase3_live_soak.py`.

## Corrective `v0.3.1` evidence contract

The acceptance runner:

1. audits the fixed raw input with bounded disk-backed ordering and UUID checks;
2. drives two independent `FeatureRuntime` instances through
   `ReceiveTimeFeatureDriver`, using different writer batch boundaries;
3. audits both feature trees and compares exact logical content;
4. fails on a source or health timestamp after its decision boundary;
5. binds single-venue IDs to exact semantic source content through the versioned
   `sha256-sum-xor-v1` mergeable multiset commitment, and cross-venue IDs to both selected
   source IDs plus the count and SHA-256 of the complete prior paired-spread history;
6. records an allowlisted runtime component inventory and observed output types instead of
   fabricated signal/order/private-request counters;
7. publishes replay and audit checkpoints only after each corresponding durable boundary.

The production collector additionally persists exact post-dedup normalized events and a terminal
feature-timeline marker. Its exclusive `IN_PROGRESS`/`CLEAN_END` manifest binds collection
ownership, code/config lineage, raw audit, journal count, and terminal watermarks. Repeated task
cancellation cannot interrupt the shared writer, event bus, feature runtime, or collector
shutdown lifecycle.

## Retained input

The source is the public Phase 2 30-minute capture made on 2026-07-24. Historical read-only
compaction recorded:

| Measure | Before | After |
|---|---:|---:|
| Parquet files | 8,362 | 53 |
| Rows | 2,331,346 | 2,331,346 |
| Unique raw IDs | 2,331,346 | 2,331,346 |
| Payload bytes | 453,728,434 | 453,728,434 |
| Partitions | 34 | 34 |

Historical compacted content digest:
`d001dddf13ef8c797117a5a3de0e0b49a519d8b388f6cd4a96514e578e894ecd`.

Receive time is the correct decision ordering for this wall-clock capture. Exchange timestamps
remain present and are checked against the decision boundary, but ordering by them would expand
the run with delayed or old venue timestamps and would not reproduce local live scheduling.

## Corrective `v0.3.1` fixed 30-minute result

The authoritative output is
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2`. Both atomic per-run checkpoints
reached `AUDIT_COMPLETE`; the command then exited `0` and wrote `summary.json` plus
`summary.md`.

The report artifacts are bound by these lowercase SHA-256 values:

- `run-1-metrics.json`:
  `7ca84ee432255e93752cf694aca7db30bdfc4c6d230e2d5b3b484b9a3a58d9e6`;
- `run-2-metrics.json`:
  `3dd5a883aac4a9ba38e854f54c801ec8d31276b2a535d51770fa2c55e35ab02e`;
- `summary.json`:
  `56df1d566a47f73f578d328bee01a38cf11faa20e809f94fa1a2a8289a899bda`;
- `summary.md`:
  `98c15f1c7c41cd14d6e224ea3a69d4f91ba5e94022ee02a1f445929ef676ba53`.

| Measure | Run 1 | Run 2 |
|---|---:|---:|
| Raw rows | 2,331,346 | 2,331,346 |
| Normalized events | 1,173,012 | 1,173,012 |
| Skipped raw rows | 1,165,522 | 1,165,522 |
| Feature calculation ticks | 1,805 | 1,805 |
| Single-venue snapshots | 21,660 | 21,660 |
| Cross-venue snapshots | 10,830 | 10,830 |
| Persisted/audited snapshots | 32,490 | 32,490 |
| Physical files | 204 | 252 |
| Replay wall time | 1,458.037 s | 1,467.352 s |
| Raw throughput | 1,598.962 rows/s | 1,588.812 rows/s |
| Event-time multiplier | 1.237678× | 1.229822× |
| Peak RSS | 6,070,730,752 B | 6,492,758,016 B |
| Final RSS | 6,035,619,840 B | 6,342,709,248 B |
| Average feature calculation | 426.751 ms | 427.929 ms |
| Maximum feature calculation | 2,306.487 ms | 5,178.535 ms |
| Maximum writer file latency | 1,141.000 ms | 734.000 ms |
| Writer/runtime backpressure | 0 | 0 |
| No-lookahead violations | 0 | 0 |
| Forbidden output events | 0 | 0 |

Both audited trees contain 32,490 unique IDs and the same exact logical digest:
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`.
They bind code version `0.3.1`, settings SHA-256
`a73a6cd8d7e14abbbe14cb6e631df084b730ef223f2431c62b5c67d99aa12610`,
and package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`.
The decision interval is `2026-07-24T01:21:30Z` through `01:51:34Z`; the raw audit covers
53 files, 34 partitions, 2,331,346 unique IDs, and digest
`d001dddf13ef8c797117a5a3de0e0b49a519d8b388f6cd4a96514e578e894ecd`.

The different physical file counts are intentional: run 1 used 1,000-row writer batches and
run 2 used 777-row batches. Equality is asserted over every canonical typed record, ID, payload
hash, lineage field, unavailable reason, partition, and decision boundary—not filenames.

Safety evidence observed only 32,490 `MARKET_FEATURE` outputs and the exact feature-only
allowlisted component graph. It observed zero forbidden output events. The offline graph contains
no connector, account, signal producer, execution component, or order writer; because private
network requests are not instrumented, the report deliberately does not invent a zero request
counter.

All 32,490 snapshots still carry at least one structured unavailable reason. Dominant counts
include `FEATURE_INPUT_MISSING` 24,519, `NOT_WARM` 21,660,
`BINANCE_NOT_WARM`/`OKX_NOT_WARM` 10,830 each, `TIME_ALIGNMENT` 7,461,
`OPEN_INTEREST_STALE` 7,323, and `BOOK_GENERATION_WARMUP` 6,030. This is an honest limitation
of the retained 30-minute input, not a deterministic-replay failure.

The two fixed runs observed 2,925.389 seconds of process wall time in total. The machine-readable
report sets `live_stability_duration_completed=false`; this is not a continuous six-hour live
soak and gives no live reconnect/resynchronization claim.

The larger fixed replay peaked at about 6.05 GiB and grew by about 5.10 GiB within one run.
These finite replay measurements do not establish safe six-hour live memory behavior. The
repeatable public-feed script is present, but the real observation remains pending for a host
with adequate memory headroom.

## Historical `v0.3.0` result — not current acceptance

The historical release-candidate run reported:

| Measure | Run 1 | Run 2 |
|---|---:|---:|
| Raw rows | 2,331,346 | 2,331,346 |
| Normalized events | 1,173,012 | 1,173,012 |
| Skipped raw rows | 1,165,522 | 1,165,522 |
| Persisted/audited snapshots | 32,478 | 32,478 |
| Single-venue snapshots | 21,642 | 21,642 |
| Cross-venue snapshots | 10,836 | 10,836 |
| Physical files | 198 | 252 |
| Replay wall time | 1,409.76 s | 1,481.79 s |
| Throughput | 1,653.72 rows/s | 1,573.34 rows/s |
| Captured-rate multiplier | 1.281× | 1.219× |
| Peak RSS | 6,526,091,264 B | 6,624,317,440 B |

The two trees reported logical digest
`e885ba7f1c2305bbf7d768e2ae56fe8dd702bfce3f568faebceeb620424c92c4`,
code version `0.3.0`, config hash
`120d54938b8cd89e3ce741f2bdb2943365868fa01b3f62892230985375a84af6`, and
package-source digest
`5aa9819151b54c34e5d7f2296bb7f3f95180426ec9864a6284d24eab843cbe81`.
The two replay process observations totaled about 48.19 minutes.

These facts establish only what the old implementation recorded:

- its `signals/orders/private APIs = 0/0/0` fields were hard-coded and are invalid evidence;
- its no-lookahead counter did not cover the later-corrected health-as-of and complete
  cross-history identity semantics;
- its replay did not preserve the exact live post-dedup normalized journal;
- its roughly 6.6 GB peak RSS and two fixed replays do not establish six-hour memory stability;
- all snapshots were unavailable on at least one required input, so no sustained fully warm and
  aligned path was demonstrated.

The dominant historical unavailable reasons included `FEATURE_INPUT_MISSING` 30,828,
`NOT_WARM` 21,640, `BINANCE_NOT_WARM` 10,812, `OKX_NOT_WARM` 10,812,
`OPEN_INTEREST_STALE` 7,323, and `TIME_ALIGNMENT` 1,366. The retained decision span is only
about 1,805 seconds while the configured Z-score history is 1,800 seconds and some required
inputs begin later.

## Acceptance decision

| Criterion | Current result |
|---|---|
| Phase 3A–3D corrective implementation | Implemented and release-ready as `v0.3.1` |
| Repeated `v0.3.1` fixed-set logical equality | Passed: 32,490 rows/run, identical digest |
| Large-set no-lookahead and complete-history identity | Passed: zero violations in both runs |
| Throughput and resource evidence | Passed throughput: 1.237678×/1.229822×; resource metrics recorded above |
| Feature schema, IDs, hashes, lineage, and partitions | Passed strict audit in both trees |
| Runtime boundary excludes scores, signals, orders, credentials, and private APIs | Passed observed graph/output audit; no fabricated network counter |
| Pytest, Ruff, strict mypy, `pip check` | Passed: 275 tests, 72 source files |
| Build and isolated clean-wheel CLI | Passed from a fresh final-6 environment |
| Exact live/journal equivalence | Requires a new matching-lineage `CLEAN_END` capture |
| Continuous six-hour stability and live recovery | Pending |
| Fully warm/aligned path | Not demonstrated |

No pending criterion is permission to enable scoring, signal production, private APIs, or
trading.

## `v0.3.1` release artifacts

The prior local artifacts predate the final README state and are retired. The final-6 wheel and
sdist were built after the packaged README became release-state neutral, then installed with all
dependencies into a fresh venv. That environment loaded `cvf` from its own `site-packages`,
reported module and distribution version `0.3.1`, passed `pip check`, exposed all release CLI
commands, and ran `cvf --once` with both connectors reporting `network_attempted=false`.

- `cvf_01-0.3.1-py3-none-any.whl` SHA-256:
  `496954e4682b10a3f1ab0dbf276d7cf695e9e0231cd1027e4840f863a0a34cd9`;
- `cvf_01-0.3.1.tar.gz` SHA-256:
  `f489b15b041a243c303d632ef216536e93f1f12e10886059f190de9aad998aaf`.

The `v0.3.1` release asset set comprises these distributions and the four authoritative `final-2`
evidence files whose hashes are recorded above.

## Historical `v0.3.0` release artifacts

The old release gate reported 174 tests, Ruff, strict mypy for 66 source files, `pip check`,
sdist/wheel build, and an offline wheel smoke test. These are historical `v0.3.0` facts, not
`v0.3.1` gate results.

- wheel SHA-256:
  `9fb9359c9f6e21cdf516cee38e6febc96c695b895cdb9aa073a38af3fbd21944`;
- sdist SHA-256:
  `649939853aa11fefd8d31a2957b14684adf37c4cc4b9da3da152bda0d8dd8439`.

The `v0.3.1` final-source fixed-run counts, digest, evidence hashes, source-tree gates, and
release artifact hashes are recorded above.

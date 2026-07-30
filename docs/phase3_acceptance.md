# Phase 3 Acceptance Evidence

## Status

Phase 3A through 3D are implemented. The longest retained fixed public dataset passes
deterministic replay, strict no-lookahead, feature schema/lineage audit, above-real-time
throughput, and the no-signal/order/private-API safety boundary.

The six-hour criterion is not complete. The available process-lifetime observation is about
48.19 minutes across two independent replays; it is not a continuous live-feed,
reconnect/resynchronization soak. This limitation is intentional and machine-readable in the
acceptance result.

## Reproducible commands

Run the fixed-data acceptance:

```powershell
cvf accept-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\fixed-30m-v0.3.0 `
  --order receive-time
```

Resume an interrupted matching run:

```powershell
cvf accept-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\fixed-30m-v0.3.0 `
  --order receive-time `
  --resume
```

Run the repeatable stability harness:

```powershell
cvf stability-phase3 `
  --input data\processed\phase3-acceptance\raw-compacted `
  --output data\processed\phase3-acceptance\stability-v0.3.0 `
  --target-hours 6
```

The acceptance output contains per-run atomic checkpoints, audited feature trees, a JSON
summary, and a Markdown summary. Checkpoints are bound to package-source SHA-256, settings,
paths, replay order, batch size, and flush interval. `REPLAY_COMPLETE` is written only after
replay closes; `AUDIT_COMPLETE` only after a strict feature-tree audit.

## Retained input

The source is the public Phase 2 30-minute capture made on 2026-07-24. Read-only compaction
preserved:

| Measure | Before | After |
|---|---:|---:|
| Parquet files | 8,362 | 53 |
| Rows | 2,331,346 | 2,331,346 |
| Unique raw IDs | 2,331,346 | 2,331,346 |
| Payload bytes | 453,728,434 | 453,728,434 |
| Partitions | 34 | 34 |

Exact compacted content digest:
`d001dddf13ef8c797117a5a3de0e0b49a519d8b388f6cd4a96514e578e894ecd`.

Receive-time is the correct decision ordering for this dataset because it was collected for
30 minutes of local wall time. Event-time ordering would expand the decision span with old or
delayed exchange timestamps and would not reproduce live scheduling. Exchange timestamps are
still retained, measured, and rejected if they exceed the decision boundary.

## Recorded v0.3.0 fixed-set result

The v0.3.0 release-candidate command completed in about 54.5 minutes including input audit,
both replays, both feature audits, and exact comparison. The two replay process-lifetime
observations total 2,891.54 seconds.

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
| No-lookahead violations | 0 | 0 |
| Signals/orders/private APIs | 0/0/0 | 0/0/0 |

Both feature trees contain 32,478 unique snapshot IDs and exact logical digest
`e885ba7f1c2305bbf7d768e2ae56fe8dd702bfce3f568faebceeb620424c92c4`.
Both audits report code version `0.3.0`; their config hash is
`120d54938b8cd89e3ce741f2bdb2943365868fa01b3f62892230985375a84af6`.
Different file and flush boundaries therefore do not change logical output.
Both per-run checkpoints reached `AUDIT_COMPLETE` and are bound to package-source digest
`5aa9819151b54c34e5d7f2296bb7f3f95180426ec9864a6284d24eab843cbe81`.

Normalized event counts per run:

| Event | Count |
|---|---:|
| Best bid/ask | 1,012,010 |
| Funding rate | 3,642 |
| Index price | 13,840 |
| Liquidation sample | 20 |
| Mark price | 17,754 |
| Open interest | 1,138 |
| Order-book snapshot | 6 |
| Order-book update | 61,688 |
| Trade | 62,914 |

Feature-state outcomes per run were 1,081,599 accepted, 91,413 rejected, four retained
venue/symbol states, and 1,189,384 retained bounded items. The run emitted 1,806 feature
boundaries and observed 361 reserved signal boundaries, but produced no signal payload.

Writer evidence:

- no backpressure and no writer errors;
- run 1: 33 flushes, average file latency 844.69 ms, maximum 3,579 ms;
- run 2: 42 flushes, average file latency 864.09 ms, maximum 3,796 ms;
- run 1 RSS changed from 4,886,568,960 B to 5,331,152,896 B; run 2 changed from
  5,483,147,264 B to 5,275,021,312 B;
- this mixed 48.19-minute result is not evidence of six-hour memory stability.

## Availability limitation

All 32,478 snapshots contain at least one structured unavailable reason. This is evidence,
not a suppressed failure:

- the configured Z-score lookback is 1,800 seconds;
- the retained decision span is about 1,805 seconds;
- some required metric histories begin more than five seconds after the first decision;
- Binance ETH finishes with a bounded pre-snapshot buffer capacity condition;
- OKX open interest is stale and trade samples are sparse near the end;
- the retained cross-venue pairs are not warm/aligned.

The dominant reason counts include `FEATURE_INPUT_MISSING` 30,828,
`NOT_WARM` 21,640, `BINANCE_NOT_WARM` 10,812, `OKX_NOT_WARM` 10,812,
`OPEN_INTEREST_STALE` 7,323, and `TIME_ALIGNMENT` 1,366.

Consequently, this fixed set proves deterministic cold/unavailable behavior, integrity,
performance, and safety. Dedicated formula tests cover warm/available calculations, but a
longer live capture is still required to demonstrate sustained fully warm and aligned output.

## Acceptance decision

| Criterion | Result |
|---|---|
| Phase 3A–3D implementation | Pass |
| Repeated fixed-set logical equality | Pass |
| Strict no-lookahead | Pass |
| Throughput above captured real time | Pass |
| Feature schema, IDs, hashes, lineage, and partitions | Pass |
| No scores, signals, orders, credentials, or private APIs | Pass |
| Continuous six-hour stability and live recovery | Pending |
| Fully warm/aligned path on retained 30-minute data | Not demonstrated |

The pending criteria must remain visible in release notes. They are not permission to enable
signals or trading.

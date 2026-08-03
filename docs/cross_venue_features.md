# Phase 3C Cross-Venue Features

## Boundary and selection

`CrossVenueFeatureEngine` is a stateless research calculation over typed
`FeatureSnapshot` records. For a canonical symbol, configured window, and UTC decision time it:

1. filters to Binance and OKX records for the exact symbol/window;
2. discards every snapshot after the decision boundary;
3. selects the latest eligible record per venue, with UUID as the deterministic tie-break;
4. computes source ages from each selected snapshot's newest source event;
5. applies configured age and source-time separation thresholds.

Threshold equality is accepted. A value over the threshold is stale or degraded. Missing,
future-only, stale, non-warm, unhealthy, or timestamp-less inputs use structured reason codes.
An unavailable pair does not expose paired numeric feature groups.

## Alignment states

| State | Meaning |
|---|---|
| `ALIGNED` | both sources exist, are warm/healthy, and meet age/separation thresholds |
| `DEGRADED` | both exist, but time separation, warmup, health, or source metadata is degraded |
| `STALE_BINANCE` | only the Binance source age exceeds the threshold |
| `STALE_OKX` | only the OKX source age exceeds the threshold |
| `UNAVAILABLE` | a venue is missing/future-only, or both sources are stale |

Quality is `1 / (1 + worst_ratio)`, where `worst_ratio` is the largest normalized Binance age,
OKX age, or timestamp-separation ratio. Missing age/time inputs produce quality `0`.

## Formula contract

Price spread uses a symmetric denominator:

```text
difference = binance_mid - okx_mid
absolute_spread = abs(difference)
denominator = (abs(binance_mid) + abs(okx_mid)) / 2
percentage_spread = difference / denominator
```

The spread Z-score uses only prior exact paired decision timestamps inside the configured
lookback. The current decision is excluded. History is isolated by canonical symbol and feature
window, and the configured minimum sample count is enforced. Every output persists the prior
paired-history count and SHA-256; both fields are bound into the deterministic snapshot ID
alongside the selected Binance/OKX source IDs. Changing any eligible historical pair therefore
changes the current identity even when the selected current source snapshots are unchanged.

Other numeric differences are consistently `Binance - OKX`. Direction fields use the configured
epsilon and return a typed positive/negative/flat agreement or divergence. Taker strength is
consistent when the difference between absolute venue imbalances is within its configured
tolerance.

Open-interest comparison uses only percentage change and venue-local price/OI state. Absolute OI
is never compared. Funding abnormality is `abs(venue funding Z-score)`. Public-liquidation
activity uses only venue-local sample Z-scores and remains explicitly labeled as a sample.

## Research and safety boundary

The confirmation aggregate and divergence fraction are research-only feature inputs. They are
not strategy scores and do not emit long, short, hold, or execution instructions.

Lead/lag fields are schema placeholders with `LEAD_LAG_INSUFFICIENT`. They cannot be populated
from local receive order. A future implementation must first define and validate an event-time
history method that is robust to clock offsets and asynchronous transport.

The implementation uses no credentials, private endpoints, accounts, positions, or order APIs.

## Fixed-data evidence

The historical `v0.3.0` run recorded 10,836 cross-venue snapshots per replay, but none was fully
warm and aligned. The configured history was 1,800 seconds while the usable decision span was
only about 1,805 seconds, and some required histories began later; stale OI, sparse late trades,
and source-time alignment also blocked availability. The old implementation reported the same
structured unavailable output and exact logical tree across its two runs.

Those counts remain historical only. The old IDs did not bind the complete paired-history
fingerprint and the old no-lookahead evidence did not cover the corrected health-as-of
semantics.

The authoritative corrective `v0.3.1` acceptance completed on 2026-07-31 and is retained at
`data/processed/phase3-acceptance/fixed-30m-v0.3.1-final-2`. Each replay produced 21,660
single-venue plus 10,830 cross-venue snapshots, for 32,490 rows per feature tree. The two
complete trees had identical logical digest
`09ebc2e9039ad04705d7bae65452c84507458f7a064d6c274205019396e38ba2`
and zero no-lookahead violations; both runs used package-source SHA-256
`5e05912737c52a21d9d075d301bee90ad00026deafba085c65da9ea87c7e7d12`. Throughput was
1.237678x/1.229822x event time (1,598.962/1,588.812 raw records per second). All 10,830
cross-venue snapshots remained non-aligned and all complete snapshots carried at least one
structured unavailable reason. The retained 30-minute set therefore proves corrected
deterministic unavailable-path behavior, but not sustained warm/aligned operation. Its fixed
observation was 2,925.389 seconds and did not complete the six-hour live soak. Warm/aligned
formulas have dedicated reference tests; the live path still requires a longer public-feed
capture.

The older `fixed-30m-v0.3.1-final` evidence predates the final feature-root locking,
journal-lineage, and cancellation-race corrections. It is superseded and is not current
acceptance evidence.

## Verification

The dedicated suite covers formula checks, threshold equality/overrun, missing/stale/future
sources, health as-of selection, warmup, zero denominators, complete-history identity,
deterministic repeat, iterable-order stability, independent live/replay engine equality,
symbol/window isolation, and explicit unavailable values. The latest complete source-tree gate
passes 275 tests, Ruff, strict mypy across 72 source files, and `pip check`. The final
`v0.3.1` sdist/wheel build and isolated clean-wheel offline CLI gate also pass; release artifact
hashes are recorded in [Phase 3 acceptance](phase3_acceptance.md).

```powershell
python -m pytest tests/unit/test_cross_venue_features.py -q
python -m pytest -q
python -m ruff check .
python -m mypy --strict src/cvf
python -m pip check
```

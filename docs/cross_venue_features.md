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
window, and the configured minimum sample count is enforced.

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

## Verification

The dedicated suite contains 31 tests, including formula checks, threshold equality/overrun,
missing/stale/future sources, warmup and health, zero denominators, deterministic repeat,
iterable-order stability, independent live/replay engine equality, symbol/window isolation, and
explicit unavailable values:

```powershell
python -m pytest tests/unit/test_cross_venue_features.py -q
python -m pytest -q
python -m ruff check .
python -m mypy --strict src/cvf
```

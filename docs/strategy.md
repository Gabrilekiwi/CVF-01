# CVF-01 Strategy Specification

## 1. Research status

All weights and thresholds are initial research hypotheses, not evidence of profitability.
Phase 3B calculates single-venue observations but does not calculate market scores or signals.
Evaluation must include fees, depth-based slippage, funding, latency, data gaps, and regime
segmentation before any parameter is considered useful.

## 2. Universe and cadence

- Binance: `BTCUSDT`, `ETHUSDT` USDⓈ-M perpetuals.
- OKX: `BTC-USDT-SWAP`, `ETH-USDT-SWAP`.
- Canonical form: `BTC-USDT-PERP`, `ETH-USDT-PERP`.
- Raw cadence: roughly 100 ms–1 s, channel dependent.
- Features: every second over 5 s, 15 s, and 60 s windows.
- Signal decision: every 5 s.
- Intended holding: 3–30 minutes; hard maximum 30 minutes.
- At most one paper position across all symbols and venues.

## 3. Input features

All rolling calculations are trailing and closed at the decision timestamp. No future event,
future bar high/low, or post-decision book state may enter a feature.

### 3.1 Taker imbalance

For each venue and window:

```text
(aggressive_buy_notional - aggressive_sell_notional)
----------------------------------------------------------------
(aggressive_buy_notional + aggressive_sell_notional)
```

The aggressor is derived from the venue's explicit maker/taker semantics and verified with
fixtures. Zero-volume windows return an unavailable feature, not a fabricated zero.

### 3.2 Order book and OFI

The first five levels use weights `1.0, 0.8, 0.6, 0.4, 0.2`. Outputs include weighted bid/ask
depth, depth imbalance, spread, mid, microprice, depth-walk slippage, cancellation change, and
simplified order-flow imbalance (OFI).

OFI reflects signed price-level additions/removals and is calculated only from a sequence-valid
local book.

The implemented removal quantity is an observable book-change proxy; public depth updates alone
cannot attribute every removal to cancellation versus execution. Recovery rate is additions
observed after the first removal in the window, divided by elapsed seconds.

### 3.3 Price impulse

- 5 s, 15 s, and 60 s returns;
- standardized impulse using trailing statistics;
- trailing 3-minute high/low;
- 1-minute ATR;
- short realized volatility.

A breakout uses only highs/lows known strictly before or at the decision boundary.

### 3.4 Open-interest impulse

Compute 15 s/60 s change, percentage change, and a venue-local Z-score. Binance and OKX
contract definitions and absolute OI are not directly compared. For either trade direction,
rising OI can confirm new position formation; the price/flow direction determines whether that
confirmation supports long or short.

### 3.5 Crowding, basis, and liquidation

Crowding combines venue-local funding Z-score, perpetual/index premium, taker bias, and the
joint price/OI state.

Public liquidation feeds are incomplete or aggregated. Their 5 s/15 s/60 s notional and
Z-score are relative activity indicators only and must never be labeled total market
liquidations.

## 4. Cross-venue alignment

Aligned snapshots produce:

- mid-price spread, percentage spread, and trailing Z-score;
- lead/lag evidence using exchange timestamps plus measured clock/receive behavior;
- agreement of taker flow, OFI, price impulse, and OI context;
- a confirmation score and divergence penalty.

No venue is called the leader solely because its packet reached this process first. If time
alignment is ambiguous, confidence falls or the decision becomes `NO_TRADE`.

## 5. Scores

Per venue:

```text
exchange_score =
    0.30 * taker_imbalance_z
  + 0.25 * ofi_z
  + 0.20 * price_impulse_z
  + 0.15 * oi_impulse_z
  + 0.10 * liquidation_impulse_z
  - liquidity_penalty
  - crowding_penalty
  - data_health_penalty
```

Combined:

```text
combined_score =
    0.45 * binance_score
  + 0.45 * okx_score
  + 0.10 * cross_exchange_confirmation
  - divergence_penalty
```

Candidate long requires `combined_score > 1.8`; candidate short requires
`combined_score < -1.8`. Values between those thresholds are neutral and cannot enter.

All coefficients and limits come from configuration.

## 6. Entry state machine

The state machine emits exactly one of:

`LONG_ENTRY`, `SHORT_ENTRY`, `LONG_EXIT`, `SHORT_EXIT`, `HOLD`, `NO_TRADE`,
`EMERGENCY_EXIT`.

### 6.1 Long candidate

Required conditions:

1. positive 15 s impulse on both venues;
2. 15 s taker imbalance above `0.12` on both;
3. 5 s taker imbalance above `0.18` on at least one;
4. positive OFI on both, with one OFI Z-score above `1.2`;
5. break of the trailing 3-minute high;
6. one OI Z-score above `0.8`, the other not below `-0.3`;
7. normal cross-venue spread, spread/slippage, and crowding;
8. combined score above `1.8`;
9. no current position or daily risk lock;
10. all core data warm and healthy.

### 6.2 Short candidate

The directional flow, OFI, impulse, and breakout tests are symmetric. OI confirmation remains
positive because increasing OI with falling price/negative flow can represent new shorts.
Combined score must be below `-1.8`.

### 6.3 Blocking conditions

Any blocking condition produces `NO_TRADE`, with structured reason codes:

- venue direction conflict or neutral combined score;
- recent move beyond `1.5 ATR`;
- liquidation spike combined with fast OI decline;
- abnormal venue spread;
- spread above its trailing 24-hour 90th percentile;
- estimated slippage above 20% of expected profit;
- core latency above 500 ms;
- unhealthy socket, invalid/gapped book, or unwarmed feature;
- an existing position;
- daily trade/loss or consecutive-loss lock.

Signals include the feature snapshot ID and expiration time so stale decisions cannot execute.

## 7. Venue selection and simulated fills

For each candidate:

```text
execution_cost =
    taker_fee
  + half_spread
  + depth_walk_slippage
  + latency_penalty
  + depth_penalty
```

Only a healthy venue may be selected. The paper fill walks up to the configured book depth to
calculate volume-weighted price. A missing depth estimate blocks entry or uses an explicitly
configured conservative fallback; it never assumes a mid-price fill.

## 8. Position sizing and risk

Default equity is 10,000 USDT. Risk per trade is 0.2%:

```text
quantity = equity * risk_fraction / abs(entry_price - stop_price)
```

Quantity is capped so notional leverage does not exceed 2x. Daily loss is capped at 1%, daily
trades at 10, and three consecutive losses block further entries for that trading day. There
is no loss adding, martingale, duplicate same-direction venue position, or stop widening.

## 9. Exit policy

- initial stop: `0.55 * 1-minute ATR`;
- TP1: `0.8 ATR`, close 50%;
- TP2: `1.3 ATR`, close the remainder;
- after TP1, move remaining stop close to cost without increasing original risk;
- at 10 minutes, exit if profit is below `0.25 ATR`;
- exit on a materially reversed combined score;
- hard exit at 30 minutes;
- `EMERGENCY_EXIT` on severe liquidity/data failure using conservative simulated pricing.

Funding, fees, slippage, realized/unrealized PnL, equity, drawdown, and win/loss streaks are
accounted explicitly.

## 10. Evaluation discipline

Live and replay modes use the same feature and signal classes. Deterministic replay must
produce identical signals/trades from identical input, configuration, code version, and seed.
Reports include trade counts, win/loss distribution, profit factor, drawdown, Sharpe, Sortino,
cost components, holding time, venue/symbol/regime breakdown, and parameter sensitivity.

Parameter selection must use train/validation/test time splits and include an embargo where
overlapping windows could leak information. Results are rejected if timestamp alignment,
book reconstruction, or raw-data completeness cannot be demonstrated.

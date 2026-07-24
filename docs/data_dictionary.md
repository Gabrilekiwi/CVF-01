# CVF-01 Data Dictionary

## 1. Conventions

- Canonical symbols are uppercase `BASE-QUOTE-PERP`.
- All timestamps are timezone-aware and normalized to UTC in memory.
- Prices, quantities, fees, funding, and PnL use `Decimal`; binary floats are reserved for
  scores, ratios, latency, and statistical features.
- Quantity units must be stated by the normalized field; venue contract counts are not silently
  treated as base-asset quantity.
- `sequence_id` is nullable because some public channels have no usable sequence. Null never
  means that ordering was validated.
- `raw_payload_reference` points to the retained raw record/partition rather than copying the
  payload into each structured row.

## 2. Common event fields

Every normalized market, health, feature, signal, and paper-trading event inherits these fields:

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `exchange` | enum | no | `BINANCE`, `OKX`, `CROSS_VENUE`, or `SIMULATED` |
| `symbol` | string | no | Canonical symbol; `*` is allowed only for exchange-wide health |
| `exchange_timestamp` | UTC datetime | no | Venue-declared event time, or decision/event clock for derived records |
| `local_receive_timestamp` | UTC datetime | no | First local observation time |
| `normalization_timestamp` | UTC datetime | no | Time normalized construction began/completed |
| `event_type` | enum | no | Stable normalized record discriminator |
| `sequence_id` | integer/string | yes | Venue sequence/trade/update identifier when available |
| `raw_payload_reference` | string | yes | URI/path plus optional row locator in raw storage |
| `receive_latency_ms` | computed float | no | Local receive time minus exchange time; may expose clock skew |

## 3. Market-data models

### `Trade`

| Field | Type | Meaning |
|---|---|---|
| `trade_id` | string | Venue trade/aggregate-trade identifier |
| `price` | positive decimal | Execution price in quote per base |
| `quantity` | positive decimal | Normalized base quantity; conversion must be documented |
| `contract_quantity` | positive decimal | Venue contract quantity when the venue reports contracts |
| `aggressor_side` | `BUY`/`SELL` | Side that crossed the spread |
| `notional` | computed decimal | `price * quantity` |

### `OrderBookLevel`

| Field | Type | Meaning |
|---|---|---|
| `price` | positive decimal | Level price |
| `quantity` | non-negative decimal | Resting quantity; zero deletes a level in updates |

### `OrderBookSnapshot`

`bids` are strictly descending, `asks` strictly ascending, and best bid must be below best ask.
`depth` states the intended retained depth; `checksum` is nullable and venue-specific.
`generation` increments whenever local continuity is invalidated.

### `OrderBookUpdate`

Contains changed `bids`/`asks`, current `sequence_id`, optional `previous_sequence_id`, and
optional checksum. At least one side must change. Applying it safely requires venue-specific
sequence rules and an active snapshot.

### `BestBidAsk`

`bid_price`, `bid_quantity`, `ask_price`, `ask_quantity`; crossed/locked quotes are rejected.
`mid_price` is computed as `(bid + ask) / 2`.

### `OpenInterest`

| Field | Type | Nullable | Unit |
|---|---|---:|---|
| `open_interest_contracts` | non-negative decimal | no | Venue contracts |
| `open_interest_base` | non-negative decimal | yes | Base asset after instrument conversion |
| `open_interest_quote` | non-negative decimal | yes | Quote notional at documented reference price |

Each venue is standardized independently. Absolute OI is never compared without contract/unit
conversion.

### `FundingRate`

`funding_rate` is a decimal fraction for the venue's stated interval.
`next_funding_timestamp` is an optional UTC datetime.

### `MarkPrice` and `IndexPrice`

Contain one positive decimal, `mark_price` or `index_price`. The raw reference preserves the
venue's index composition/mark methodology context.

### `LiquidationEvent`

`position_side` names the liquidated position (`LONG` or `SHORT`), not an ambiguously inferred
trade side. `price`, `quantity`, and computed `notional` are positive. Public feeds are relative
activity samples and not complete liquidation totals.

## 4. Health and features

### `ExchangeHealth`

| Field | Meaning |
|---|---|
| `status` | `CONNECTED`, `DEGRADED`, `STALE`, `RESYNCING`, `DISCONNECTED` |
| `is_connected` | Transport connection state |
| `last_event_timestamp` | Most recent valid exchange event time |
| `last_latency_ms` | Most recent observed receive latency |
| `average_latency_ms`, `maximum_latency_ms` | Running adjusted latency statistics |
| `last_normalization_latency_ms` | Most recent local normalization time |
| `clock_skew_ms` | Estimated venue/local clock difference |
| `messages_received` | Accepted normalized events |
| `duplicate_events` | Count in the current observation scope |
| `sequence_gaps`, `checksum_failures` | Lifetime continuity failures |
| `reconnects`, `resubscriptions` | Lifecycle recovery counters |
| `parse_errors`, `dropped_events`, `backpressure_events` | Ingestion/storage diagnostics |
| `book_generation` | Current local-book generation |
| `sequence_gap_detected` | Whether continuity failed |
| `resyncing` | Whether snapshot/state recovery is active |
| `rest_healthy` | Public REST dependency state |
| `open_interest_stale` | OI freshness flag |
| `last_error` | Latest structured-error summary |
| `details` | Small typed diagnostic map |

Exchange-wide health uses `symbol="*"`.

### `FeatureSnapshot` schema v1

The legacy `MarketFeature.values` map remains a compatibility model. Phase 3 uses the typed,
versioned `FeatureSnapshot` contract:

| Field | Meaning |
|---|---|
| `feature_snapshot_id` | UUID joining later signals to exact features |
| `schema_version`, `strategy_version` | Immutable schema and strategy identities |
| `calculation_timestamp`, `decision_timestamp` | Calculation time and no-lookahead boundary |
| `window_seconds` | Trailing `(decision-window, decision]` interval |
| `book_generation`, `source_sequence_id` | Exact order-book lifecycle/source boundary |
| `source_event_count` | Valid accepted source events used |
| `oldest_source_timestamp`, `newest_source_timestamp` | Auditable event-time bounds |
| `data_age_ms` | Age of the newest required source at decision time |
| `is_warm`, `is_healthy` | Separate statistical warmup and operational health gates |
| `unavailable_reasons` | Structured missing, stale, generation, health, or backlog blockers |
| typed feature groups | Trade flow, order book, price, OI, crowding, and liquidation |

Null means unavailable/undefined and is distinct from true numeric zero. A non-warm or unhealthy
snapshot must contain at least one structured reason. A source timestamp after the decision
boundary is rejected.

## 5. Signal model

`TradingSignal` adds:

| Field | Meaning |
|---|---|
| `signal_id` | UUID |
| `timestamp` | Decision time |
| `signal_type` | Entry, exit, hold, no-trade, or emergency exit |
| `binance_score`, `okx_score`, `combined_score` | Unrounded decision scores |
| `confidence` | Bounded research confidence `[0, 1]` |
| `suggested_exchange` | Selected execution venue or null |
| `suggested_entry_price` | Depth-aware expected entry or null |
| `stop_loss`, `take_profit_1`, `take_profit_2` | Ordered levels for entries |
| `expires_at` | Strictly after decision time |
| `reasons` | Structured `code/message/exchange/value/threshold` records |
| `blocking_conditions` | Structured blockers explaining `NO_TRADE` |
| `feature_snapshot_id` | Exact feature join key |
| `strategy_version` | Immutable decision logic/config version |

Long entry levels satisfy `stop < entry < TP1 < TP2`; short levels are symmetric.

## 6. Paper-trading models

### `SimulatedOrder`

Tracks requested/filled quantity, average fill price, fee, estimated slippage, order status,
creation/completion timestamps, and rejection reason. Only `MARKET` is supported in the MVP.

### `SimulatedPosition`

Tracks opening signal, side/status, entry, original/remaining quantity, stop/targets,
open/close times, realized/unrealized PnL, fees, and funding. A `CLOSED` position must have zero
remaining quantity.

### `SimulatedTrade`

Represents one fill or exit slice with order/position IDs, purpose, side, execution price,
quantity, fee, slippage, realized PnL, and execution time.

## 7. Symbol mapping

| Canonical | Binance | OKX perpetual | OKX index instrument |
|---|---|---|---|
| `BTC-USDT-PERP` | `BTCUSDT` | `BTC-USDT-SWAP` | `BTC-USDT` |
| `ETH-USDT-PERP` | `ETHUSDT` | `ETH-USDT-SWAP` | `ETH-USDT` |

Mapping is exact and fail-closed. The OKX index instrument is converted to its configured
perpetual canonical symbol only inside the index-channel mapping.

## 8. Raw Parquet schema

Implemented layout:

```text
data/raw/date=YYYY-MM-DD/exchange=BINANCE/symbol=BTC-USDT-PERP/channel=aggTrade/*.parquet
```

| Field | Type | Meaning |
|---|---|---|
| `schema_version` | int16 | Raw schema, currently `1` |
| `record_id` | UUID string | Unique row identity |
| `raw_payload_reference` | string | Stable `raw://<record_id>` lineage key |
| `exchange`, `symbol`, `channel` | string | Routing and partition metadata |
| `message_kind` | string | Market data, control, lifecycle, or parse error |
| `transport` | string | `websocket`, `rest`, or `internal` |
| `exchange_timestamp` | UTC timestamp | Nullable when the venue supplies none |
| `local_receive_timestamp` | UTC timestamp | First local receipt |
| `normalization_timestamp` | UTC timestamp | Nullable for raw-first writes |
| `sequence_id` | string | Nullable venue sequence/trade ID |
| `connection_generation` | int64 | WebSocket lifecycle generation |
| `raw_payload` | binary | Exact received frame or HTTP response bytes |

Malformed WebSocket bytes are persisted under `_unparsed` before the session is recovered.
Connection lifecycle records use wildcard symbol and `_session_*` channels.

"""Reference single-venue feature calculations over bounded event-time state."""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import pairwise
from statistics import fmean, pstdev
from uuid import NAMESPACE_URL, uuid5

from cvf.config import Settings
from cvf.features.availability import evaluate_availability
from cvf.features.models import (
    CrowdingFeatureValues,
    CrowdingState,
    FeatureSnapshot,
    FeatureUnavailableCode,
    FeatureUnavailableReason,
    LiquidationFeatureValues,
    OpenInterestFeatureValues,
    OrderBookFeatureValues,
    PriceFeatureValues,
    PriceOpenInterestState,
    TradeFlowFeatureValues,
)
from cvf.features.rolling import BoundedTimeWindow, LateEventPolicy, TimedValue
from cvf.features.state import FeatureBookView, VenueSymbolState
from cvf.models.enums import AggressorSide, HealthStatus, LiquidatedPositionSide
from cvf.models.market import (
    BestBidAsk,
    MarkPrice,
    OpenInterest,
    Trade,
)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("feature decision timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _ratio_impulse(current: Decimal, previous: Decimal) -> float | None:
    if previous == 0:
        return None
    return float(current / previous - 1)


def _count_impulse(current: int, previous: int) -> float | None:
    if previous == 0:
        return None
    return current / previous - 1.0


def _reference_price(event: Trade | BestBidAsk | MarkPrice) -> Decimal:
    if isinstance(event, Trade):
        return event.price
    if isinstance(event, BestBidAsk):
        return event.mid_price
    return event.mark_price


def _latest_at_or_before[T](
    values: Iterable[TimedValue[T]],
    decision: datetime,
) -> TimedValue[T] | None:
    for item in reversed(list(values)):
        if item.timestamp <= decision:
            return item
    return None


def _at_or_before[T](
    values: Iterable[TimedValue[T]],
    boundary: datetime,
) -> TimedValue[T] | None:
    candidate: TimedValue[T] | None = None
    for item in values:
        if item.timestamp > boundary:
            break
        candidate = item
    return candidate


class _MetricHistory:
    def __init__(self, settings: Settings) -> None:
        retention = timedelta(seconds=settings.features.zscore_lookback_seconds)
        self._retention = retention
        self._maximum_items = settings.features.maximum_events_per_stream
        self._windows: dict[str, BoundedTimeWindow[float]] = {}
        self._last: dict[str, tuple[datetime, float | None, bool]] = {}

    def record(
        self,
        key: str,
        timestamp: datetime,
        value: float | None,
    ) -> tuple[float | None, bool]:
        cached = self._last.get(key)
        if cached is not None and cached[0] == timestamp:
            return cached[1], cached[2]
        window = self._windows.setdefault(
            key,
            BoundedTimeWindow(
                retention=self._retention,
                maximum_items=self._maximum_items,
                late_event_policy=LateEventPolicy.DROP,
            ),
        )
        history = [item.value for item in window]
        coverage_warm = bool(window) and next(iter(window)).timestamp <= timestamp - self._retention
        zscore: float | None = None
        if value is not None and len(history) >= 2:
            deviation = pstdev(history)
            if deviation > 0:
                zscore = (value - fmean(history)) / deviation
            elif value == history[-1]:
                zscore = 0.0
        if value is not None:
            window.append(timestamp, value)
        self._last[key] = (timestamp, zscore, coverage_warm)
        return zscore, coverage_warm


def _depth_walk_bps(
    levels: Iterable[tuple[Decimal, Decimal]],
    *,
    target_notional: Decimal,
    mid_price: Decimal,
    buy: bool,
) -> float | None:
    remaining = target_notional
    acquired = Decimal(0)
    spent = Decimal(0)
    for price, quantity in levels:
        available = price * quantity
        used = min(available, remaining)
        if used > 0:
            acquired += used / price
            spent += used
            remaining -= used
        if remaining <= 0:
            break
    if remaining > 0 or acquired <= 0:
        return None
    average = spent / acquired
    relative = average / mid_price - 1
    return float((relative if buy else -relative) * Decimal(10_000))


def _one_second_atr(
    prices: list[TimedValue[Trade | BestBidAsk | MarkPrice]],
) -> Decimal | None:
    if len(prices) < 2:
        return None
    buckets: dict[int, list[Decimal]] = {}
    for item in prices:
        second = int(item.timestamp.timestamp())
        buckets.setdefault(second, []).append(_reference_price(item.value))
    previous_close: Decimal | None = None
    true_ranges: list[Decimal] = []
    for second in sorted(buckets):
        values = buckets[second]
        high = max(values)
        low = min(values)
        true_range = high - low
        if previous_close is not None:
            true_range = max(
                true_range,
                abs(high - previous_close),
                abs(low - previous_close),
            )
        true_ranges.append(true_range)
        previous_close = values[-1]
    if not true_ranges:
        return None
    return sum(true_ranges, Decimal(0)) / len(true_ranges)


class SingleVenueFeatureEngine:
    """Calculate typed observations without applying any trade-direction rules."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._history = _MetricHistory(settings)

    def calculate(
        self,
        state: VenueSymbolState,
        *,
        decision_timestamp: datetime,
        window_seconds: int,
    ) -> FeatureSnapshot:
        decision = _utc(decision_timestamp)
        if window_seconds not in self.settings.timing.feature_windows_seconds:
            raise ValueError("window_seconds is not a configured feature window")
        window = timedelta(seconds=window_seconds)
        start = decision - window
        reasons = list(
            evaluate_availability(
                state,
                decision_timestamp=decision,
                warmup=window,
                open_interest_stale_after=timedelta(
                    milliseconds=self.settings.health.open_interest_stale_after_ms
                ),
                blocked_health_statuses=frozenset(
                    HealthStatus(value)
                    for value in self.settings.health.block_entry_statuses
                ),
            ).reasons
        )

        trades = [
            item
            for item in state.trades
            if start < item.timestamp <= decision
        ]
        previous_trades = [
            item
            for item in state.trades
            if start - window < item.timestamp <= start
        ]
        if not trades and not any(
            reason.code is FeatureUnavailableCode.NO_TRADES for reason in reasons
        ):
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.NO_TRADES,
                    detail="no accepted trades fall inside the feature window",
                )
            )
        buy_notional = sum(
            (
                item.value.notional
                for item in trades
                if item.value.aggressor_side is AggressorSide.BUY
            ),
            Decimal(0),
        )
        sell_notional = sum(
            (
                item.value.notional
                for item in trades
                if item.value.aggressor_side is AggressorSide.SELL
            ),
            Decimal(0),
        )
        total_notional = buy_notional + sell_notional
        previous_notional = sum(
            (item.value.notional for item in previous_trades),
            Decimal(0),
        )
        imbalance = (
            None
            if total_notional == 0
            else float((buy_notional - sell_notional) / total_notional)
        )
        prefix = f"{state.exchange.value}:{state.symbol}:{window_seconds}"
        imbalance_z, imbalance_warm = self._history.record(
            f"{prefix}:taker_imbalance",
            decision,
            imbalance,
        )
        large_notional = sum(
            (
                item.value.notional
                for item in trades
                if item.value.notional >= self.settings.features.large_trade_notional_usdt
            ),
            Decimal(0),
        )
        trade_flow = TradeFlowFeatureValues(
            aggressive_buy_notional=buy_notional,
            aggressive_sell_notional=sell_notional,
            taker_imbalance=imbalance,
            taker_imbalance_zscore=imbalance_z,
            trade_notional_impulse=_ratio_impulse(total_notional, previous_notional),
            trade_count_impulse=_count_impulse(len(trades), len(previous_trades)),
            average_trade_notional=(
                None if not trades else total_notional / len(trades)
            ),
            large_trade_share=(
                None if total_notional == 0 else float(large_notional / total_notional)
            ),
        )

        book_items = list(state.book_updates)
        has_future_book = bool(book_items and book_items[-1].timestamp > decision)
        book_view = state.order_book.view(depth=self.settings.features.order_book_depth)
        order_book = None if has_future_book else self._order_book_values(
            state,
            book_view,
            start=start,
            decision=decision,
        )
        ofi_z, ofi_warm = self._history.record(
            f"{prefix}:ofi",
            decision,
            None if order_book is None else order_book.order_flow_imbalance,
        )
        if order_book is not None:
            order_book = order_book.model_copy(
                update={"order_flow_imbalance_zscore": ofi_z}
            )
        if has_future_book:
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.EVENT_GAP,
                    detail="current order book contains an event after the decision boundary",
                    channel="order_book",
                )
            )

        price_values, return_z, return_warm = self._price_values(
            state,
            start=start,
            decision=decision,
            history_prefix=prefix,
        )
        oi_values, oi_z, oi_warm = self._open_interest_values(
            state,
            start=start,
            decision=decision,
            history_prefix=prefix,
        )
        if oi_values is not None:
            oi_values = oi_values.model_copy(
                update={
                    "zscore": oi_z,
                    "price_oi_state": self._price_oi_state(
                        price_values.return_value,
                        oi_values.percentage_change,
                    ),
                }
            )
        crowding, premium_warm = self._crowding_values(
            state,
            decision=decision,
            history_prefix=prefix,
            taker_bias=imbalance,
            price_return=price_values.return_value,
            oi_change=(
                None if oi_values is None else oi_values.percentage_change
            ),
        )
        liquidation, liquidation_warm = self._liquidation_values(
            state,
            start=start,
            decision=decision,
            history_prefix=prefix,
            oi_change=(
                None if oi_values is None else oi_values.percentage_change
            ),
        )

        statistical_warm = all(
            (
                imbalance_warm,
                ofi_warm,
                return_warm,
                oi_warm,
                premium_warm,
                liquidation_warm,
            )
        )
        availability = evaluate_availability(
            state,
            decision_timestamp=decision,
            warmup=window,
            open_interest_stale_after=timedelta(
                milliseconds=self.settings.health.open_interest_stale_after_ms
            ),
            blocked_health_statuses=frozenset(
                HealthStatus(value)
                for value in self.settings.health.block_entry_statuses
            ),
        )
        if not statistical_warm:
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.NOT_WARM,
                    detail="derived statistical history has not covered the configured lookback",
                )
            )
        reasons = list(dict.fromkeys(reasons))

        sources = self._source_items(state, start=start, decision=decision)
        oldest = min((item.timestamp for item in sources), default=None)
        newest = max((item.timestamp for item in sources), default=None)
        source_count = len(sources)
        data_age_ms = (
            0.0
            if newest is None
            else max(0.0, (decision - newest).total_seconds() * 1000.0)
        )
        feature_id = uuid5(
            NAMESPACE_URL,
            (
                f"cvf:{self.settings.app.strategy_version}:{state.exchange.value}:"
                f"{state.symbol}:{decision.isoformat()}:{window_seconds}:"
                f"{book_view.generation}:{book_view.sequence_id}"
            ),
        )
        is_warm = availability.is_warm and statistical_warm and bool(trades)
        is_healthy = availability.is_healthy and not has_future_book
        return FeatureSnapshot(
            exchange=state.exchange,
            symbol=state.symbol,
            exchange_timestamp=decision,
            local_receive_timestamp=decision,
            normalization_timestamp=decision,
            sequence_id=book_view.sequence_id,
            feature_snapshot_id=feature_id,
            strategy_version=self.settings.app.strategy_version,
            calculation_timestamp=decision,
            decision_timestamp=decision,
            window_seconds=window_seconds,
            book_generation=book_view.generation,
            source_sequence_id=book_view.sequence_id,
            source_event_count=source_count,
            oldest_source_timestamp=oldest,
            newest_source_timestamp=newest,
            data_age_ms=data_age_ms,
            is_warm=is_warm,
            is_healthy=is_healthy,
            unavailable_reasons=tuple(reasons),
            trade_flow=trade_flow,
            order_book=order_book,
            price=price_values.model_copy(update={"impulse_zscore": return_z}),
            open_interest=oi_values,
            crowding=crowding,
            liquidation=liquidation,
        )

    def calculate_all(
        self,
        states: Iterable[VenueSymbolState],
        *,
        decision_timestamp: datetime,
    ) -> list[FeatureSnapshot]:
        return [
            self.calculate(
                state,
                decision_timestamp=decision_timestamp,
                window_seconds=window_seconds,
            )
            for state in states
            for window_seconds in self.settings.timing.feature_windows_seconds
        ]

    def _order_book_values(
        self,
        state: VenueSymbolState,
        view: FeatureBookView,
        *,
        start: datetime,
        decision: datetime,
    ) -> OrderBookFeatureValues | None:
        if not view.synchronized or not view.bids or not view.asks:
            return None
        weights = [
            Decimal(str(value))
            for value in self.settings.features.order_book_level_weights
        ]
        bid_depth = sum(
            (level.quantity * weights[index] for index, level in enumerate(view.bids)),
            Decimal(0),
        )
        ask_depth = sum(
            (level.quantity * weights[index] for index, level in enumerate(view.asks)),
            Decimal(0),
        )
        depth_total = bid_depth + ask_depth
        best_bid = view.bids[0]
        best_ask = view.asks[0]
        spread = best_ask.price - best_bid.price
        mid = (best_ask.price + best_bid.price) / 2
        top_quantity = best_bid.quantity + best_ask.quantity
        microprice = (
            None
            if top_quantity == 0
            else (
                best_ask.price * best_bid.quantity
                + best_bid.price * best_ask.quantity
            )
            / top_quantity
        )
        changes = [
            item.value
            for item in state.book_changes
            if start < item.timestamp <= decision
        ]
        ofi = sum(
            (change.order_flow_imbalance for change in changes),
            Decimal(0),
        )
        bid_change = sum(
            (change.bid_quantity_delta for change in changes),
            Decimal(0),
        )
        ask_change = sum(
            (change.ask_quantity_delta for change in changes),
            Decimal(0),
        )
        removed_quantity = sum(
            (
                change.removed_bid_quantity + change.removed_ask_quantity
                for change in changes
            ),
            Decimal(0),
        )
        added_quantity = sum(
            (
                change.added_bid_quantity + change.added_ask_quantity
                for change in changes
            ),
            Decimal(0),
        )
        first_removal_at = next(
            (
                item.timestamp
                for item in state.book_changes
                if start < item.timestamp <= decision
                and (
                    item.value.removed_bid_quantity
                    + item.value.removed_ask_quantity
                    > 0
                )
            ),
            None,
        )
        recovery_rate = None
        if first_removal_at is not None:
            recovery_seconds = (decision - first_removal_at).total_seconds()
            recovered = sum(
                (
                    item.value.added_bid_quantity
                    + item.value.added_ask_quantity
                    for item in state.book_changes
                    if first_removal_at < item.timestamp <= decision
                ),
                Decimal(0),
            )
            if recovery_seconds > 0:
                recovery_rate = float(recovered) / recovery_seconds
        return OrderBookFeatureValues(
            weighted_bid_depth=bid_depth,
            weighted_ask_depth=ask_depth,
            bid_liquidity_change=bid_change,
            ask_liquidity_change=ask_change,
            added_liquidity_quantity=added_quantity,
            removed_liquidity_quantity=removed_quantity,
            liquidity_recovery_quantity_per_second=recovery_rate,
            depth_imbalance=(
                None
                if depth_total == 0
                else float((bid_depth - ask_depth) / depth_total)
            ),
            spread=spread,
            relative_spread=float(spread / mid),
            mid_price=mid,
            microprice=microprice,
            buy_slippage_bps=_depth_walk_bps(
                ((level.price, level.quantity) for level in view.asks),
                target_notional=self.settings.features.depth_walk_notional_usdt,
                mid_price=mid,
                buy=True,
            ),
            sell_slippage_bps=_depth_walk_bps(
                ((level.price, level.quantity) for level in view.bids),
                target_notional=self.settings.features.depth_walk_notional_usdt,
                mid_price=mid,
                buy=False,
            ),
            order_flow_imbalance=float(ofi),
        )

    def _price_values(
        self,
        state: VenueSymbolState,
        *,
        start: datetime,
        decision: datetime,
        history_prefix: str,
    ) -> tuple[PriceFeatureValues, float | None, bool]:
        prices = [
            item
            for item in state.prices
            if start < item.timestamp <= decision
        ]
        first = None if not prices else _reference_price(prices[0].value)
        last = None if not prices else _reference_price(prices[-1].value)
        return_value = (
            None
            if first is None or last is None or first == 0
            else float(last / first - 1)
        )
        return_z, warm = self._history.record(
            f"{history_prefix}:return",
            decision,
            return_value,
        )
        log_returns: list[float] = []
        for left, right in pairwise(prices):
            left_price = _reference_price(left.value)
            right_price = _reference_price(right.value)
            if left_price > 0 and right_price > 0:
                log_returns.append(math.log(float(right_price / left_price)))
        atr_prices = [
            item
            for item in state.prices
            if decision - timedelta(seconds=self.settings.features.atr_period_seconds)
            < item.timestamp
            <= decision
        ]
        atr = _one_second_atr(atr_prices)
        breakout_prices = [
            _reference_price(item.value)
            for item in state.prices
            if decision
            - timedelta(seconds=self.settings.features.breakout_lookback_seconds)
            < item.timestamp
            <= decision
        ]
        return (
            PriceFeatureValues(
                return_value=return_value,
                realized_volatility=(
                    None
                    if len(log_returns) < 2
                    else math.sqrt(sum(value * value for value in log_returns))
                ),
                atr_1m=atr,
                trailing_high=max(breakout_prices) if breakout_prices else None,
                trailing_low=min(breakout_prices) if breakout_prices else None,
                recent_move_atr=self._recent_move_atr(first, last, atr),
                abnormal_jump=(
                    None
                    if return_z is None
                    else abs(return_z) >= self.settings.features.abnormal_jump_zscore
                ),
            ),
            return_z,
            warm,
        )

    @staticmethod
    def _recent_move_atr(
        first: Decimal | None,
        last: Decimal | None,
        atr: Decimal | None,
    ) -> float | None:
        if first is None or last is None or atr is None or atr == 0:
            return None
        return float((last - first) / atr)

    def _open_interest_values(
        self,
        state: VenueSymbolState,
        *,
        start: datetime,
        decision: datetime,
        history_prefix: str,
    ) -> tuple[OpenInterestFeatureValues | None, float | None, bool]:
        latest = _latest_at_or_before(state.open_interest, decision)
        anchor = _at_or_before(state.open_interest, start)
        if latest is None:
            return None, None, False

        def quantity(event: OpenInterest) -> Decimal:
            if event.open_interest_base is not None:
                return event.open_interest_base
            return event.open_interest_contracts

        latest_value = quantity(latest.value)
        anchor_value = None if anchor is None else quantity(anchor.value)
        change = None if anchor_value is None else latest_value - anchor_value
        percentage = None
        if anchor_value is not None and anchor_value != 0:
            percentage = float(latest_value / anchor_value - 1)
        zscore, warm = self._history.record(
            f"{history_prefix}:oi_change",
            decision,
            percentage,
        )
        return (
            OpenInterestFeatureValues(
                change=change,
                percentage_change=percentage,
                data_age_ms=max(
                    0.0,
                    (decision - latest.timestamp).total_seconds() * 1000.0,
                ),
            ),
            zscore,
            warm,
        )

    def _crowding_values(
        self,
        state: VenueSymbolState,
        *,
        decision: datetime,
        history_prefix: str,
        taker_bias: float | None,
        price_return: float | None,
        oi_change: float | None,
    ) -> tuple[CrowdingFeatureValues, bool]:
        funding = _latest_at_or_before(state.funding_rates, decision)
        mark = _latest_at_or_before(state.mark_prices, decision)
        index = _latest_at_or_before(state.index_prices, decision)
        premium: float | None = None
        if mark is not None and index is not None and index.value.index_price != 0:
            premium = float(mark.value.mark_price / index.value.index_price - 1)
        premium_z, premium_warm = self._history.record(
            f"{history_prefix}:premium",
            decision,
            premium,
        )
        funding_value = None if funding is None else funding.value.funding_rate
        funding_z, funding_warm = self._history.record(
            f"{history_prefix}:funding",
            decision,
            None if funding_value is None else float(funding_value),
        )
        joint_state = self._crowding_state(
            taker_bias=taker_bias,
            price_return=price_return,
            oi_change=oi_change,
            funding_rate=funding_value,
        )
        return (
            CrowdingFeatureValues(
                funding_rate=funding_value,
                funding_zscore=funding_z,
                mark_index_premium=premium,
                premium_zscore=premium_z,
                taker_bias=taker_bias,
                joint_state=joint_state,
            ),
            premium_warm and funding_warm,
        )

    def _liquidation_values(
        self,
        state: VenueSymbolState,
        *,
        start: datetime,
        decision: datetime,
        history_prefix: str,
        oi_change: float | None,
    ) -> tuple[LiquidationFeatureValues, bool]:
        events = [
            item.value
            for item in state.liquidations
            if start < item.timestamp <= decision
        ]
        long_notional = sum(
            (
                event.notional
                for event in events
                if event.position_side is LiquidatedPositionSide.LONG
            ),
            Decimal(0),
        )
        short_notional = sum(
            (
                event.notional
                for event in events
                if event.position_side is LiquidatedPositionSide.SHORT
            ),
            Decimal(0),
        )
        activity = float(long_notional + short_notional)
        zscore, warm = self._history.record(
            f"{history_prefix}:liquidation",
            decision,
            activity,
        )
        return (
            LiquidationFeatureValues(
                public_sample_long_notional=long_notional,
                public_sample_short_notional=short_notional,
                public_sample_activity_zscore=zscore,
                activity_with_oi_decline=(
                    None
                    if not events or oi_change is None
                    else oi_change < 0
                ),
            ),
            warm,
        )

    @staticmethod
    def _price_oi_state(
        price_return: float | None,
        oi_change: float | None,
    ) -> PriceOpenInterestState | None:
        if price_return is None or oi_change is None:
            return None
        if price_return == 0 or oi_change == 0:
            return PriceOpenInterestState.FLAT
        if price_return > 0:
            return (
                PriceOpenInterestState.PRICE_UP_OI_UP
                if oi_change > 0
                else PriceOpenInterestState.PRICE_UP_OI_DOWN
            )
        return (
            PriceOpenInterestState.PRICE_DOWN_OI_UP
            if oi_change > 0
            else PriceOpenInterestState.PRICE_DOWN_OI_DOWN
        )

    @staticmethod
    def _crowding_state(
        *,
        taker_bias: float | None,
        price_return: float | None,
        oi_change: float | None,
        funding_rate: Decimal | None,
    ) -> CrowdingState | None:
        if (
            taker_bias is None
            or price_return is None
            or oi_change is None
            or funding_rate is None
        ):
            return None
        if all(value > 0 for value in (taker_bias, price_return, oi_change)) and (
            funding_rate > 0
        ):
            return CrowdingState.CROWDED_LONG
        if all(value < 0 for value in (taker_bias, price_return, oi_change)) and (
            funding_rate < 0
        ):
            return CrowdingState.CROWDED_SHORT
        return CrowdingState.MIXED

    @staticmethod
    def _source_items(
        state: VenueSymbolState,
        *,
        start: datetime,
        decision: datetime,
    ) -> list[TimedValue[object]]:
        sources: list[TimedValue[object]] = []
        windows: tuple[Iterable[TimedValue[object]], ...] = (
            state.trades,
            state.open_interest,
            state.funding_rates,
            state.mark_prices,
            state.index_prices,
            state.liquidations,
            state.book_updates,
        )
        for window in windows:
            sources.extend(
                item
                for item in window
                if start < item.timestamp <= decision
            )
        return sources

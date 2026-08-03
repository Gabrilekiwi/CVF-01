"""Reference single-venue feature calculations over bounded event-time state."""

from __future__ import annotations

import math
from bisect import bisect_left, bisect_right
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import fmean, pstdev
from uuid import NAMESPACE_URL, UUID, uuid5

from cvf import __version__
from cvf.config import Settings
from cvf.features.availability import FeatureAvailability, evaluate_availability
from cvf.features.lineage import SourceLineage, semantic_source_digest
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
from cvf.features.state import BookChange, FeatureBookView, VenueSymbolState
from cvf.models.enums import (
    AggressorSide,
    Exchange,
    HealthStatus,
    LiquidatedPositionSide,
)
from cvf.models.market import (
    BestBidAsk,
    MarkPrice,
    OpenInterest,
    Trade,
)
from cvf.utils.fingerprint import (
    canonical_json,
    canonicalize_for_hash,
    settings_fingerprint,
    sha256_text,
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


class _MetricHistory:
    def __init__(self, settings: Settings) -> None:
        retention = timedelta(seconds=settings.features.zscore_lookback_seconds)
        self._retention = retention
        self._maximum_items = settings.features.maximum_events_per_stream
        self._windows: dict[str, BoundedTimeWindow[float]] = {}
        self._last: dict[
            str,
            tuple[datetime, float | None, float | None, bool],
        ] = {}

    def _new_window(self) -> BoundedTimeWindow[float]:
        return BoundedTimeWindow(
            retention=self._retention,
            maximum_items=self._maximum_items,
            late_event_policy=LateEventPolicy.DROP,
        )

    def clear_scope(self, scope_prefix: str) -> None:
        """Drop every derived history associated with a rebuilt book scope."""

        self._windows = {
            key: value
            for key, value in self._windows.items()
            if not key.startswith(scope_prefix)
        }
        self._last = {
            key: value
            for key, value in self._last.items()
            if not key.startswith(scope_prefix)
        }

    def record(
        self,
        key: str,
        timestamp: datetime,
        value: float | None,
    ) -> tuple[float | None, bool]:
        cached = self._last.get(key)
        if cached is not None and cached[0] == timestamp:
            if cached[1] == value:
                return cached[2], cached[3]
            retained = self._new_window()
            for item in self._windows.get(key, ()):
                if item.timestamp != timestamp:
                    retained.append(item.timestamp, item.value)
            self._windows[key] = retained
        window = self._windows.setdefault(key, self._new_window())
        history = [item.value for item in window]
        coverage_warm = bool(window) and next(iter(window)).timestamp <= timestamp - self._retention
        zscore: float | None = None
        if value is not None and len(history) >= 2:
            deviation = pstdev(history)
            if deviation > 0:
                zscore = (value - fmean(history)) / deviation
        if value is not None:
            window.append(timestamp, value)
        zscore_ready = coverage_warm and zscore is not None
        self._last[key] = (timestamp, value, zscore, zscore_ready)
        return zscore, zscore_ready


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


type _PriceEvent = Trade | BestBidAsk | MarkPrice
_DIGEST_MASK = (1 << 256) - 1


@dataclass(frozen=True, slots=True)
class _CachedPricePoint:
    timestamp: datetime
    ordinal: int
    price: Decimal
    source: object
    prefix_squared_log_return: float
    prefix_valid_log_returns: int
    prefix_digest_sum: int
    prefix_digest_xor: int


@dataclass(slots=True)
class _PriceSecondBucket:
    second: int
    high: Decimal
    low: Decimal
    close: Decimal

    def append(self, price: Decimal) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price


@dataclass(frozen=True, slots=True)
class _PriceRange:
    count: int
    first: Decimal | None
    last: Decimal | None
    realized_volatility: float | None


class _PriceHistoryCache:
    """Incremental exact queries over one append-mostly price stream."""

    _TRIM_BATCH = 4_096

    def __init__(self) -> None:
        self._points: list[_CachedPricePoint] = []
        self._timestamps: list[datetime] = []
        self._bucket_seconds: list[int] = []
        self._buckets: list[_PriceSecondBucket] = []
        self._source_positions_by_object: dict[
            int,
            tuple[object, datetime],
        ] = {}
        self._active_start = 0
        self._last_ordinal = 0
        self._base_squared_log_return = 0.0
        self._base_digest_sum = 0
        self._base_digest_xor = 0

    @staticmethod
    def _second(timestamp: datetime) -> int:
        return int(timestamp.timestamp())

    def _append(self, item: TimedValue[_PriceEvent]) -> None:
        timestamp = item.timestamp.astimezone(UTC)
        price = _reference_price(item.value)
        previous = self._points[-1] if self._points else None
        squared = (
            self._base_squared_log_return
            if previous is None
            else previous.prefix_squared_log_return
        )
        valid_log_returns = (
            0 if previous is None else previous.prefix_valid_log_returns
        )
        if previous is not None and previous.price > 0 and price > 0:
            log_return = math.log(float(price / previous.price))
            squared += log_return * log_return
            valid_log_returns += 1
        source_digest = semantic_source_digest(timestamp, item.value)
        digest_value = int(source_digest, 16)
        previous_sum = (
            self._base_digest_sum
            if previous is None
            else previous.prefix_digest_sum
        )
        previous_xor = (
            self._base_digest_xor
            if previous is None
            else previous.prefix_digest_xor
        )
        self._points.append(
            _CachedPricePoint(
                timestamp=timestamp,
                ordinal=item.ordinal,
                price=price,
                source=item.value,
                prefix_squared_log_return=squared,
                prefix_valid_log_returns=valid_log_returns,
                prefix_digest_sum=(previous_sum + digest_value) & _DIGEST_MASK,
                prefix_digest_xor=previous_xor ^ digest_value,
            )
        )
        self._timestamps.append(timestamp)
        self._source_positions_by_object[id(item.value)] = (
            item.value,
            timestamp,
        )
        second = self._second(timestamp)
        if self._bucket_seconds and self._bucket_seconds[-1] == second:
            self._buckets[-1].append(price)
        else:
            self._bucket_seconds.append(second)
            self._buckets.append(
                _PriceSecondBucket(
                    second=second,
                    high=price,
                    low=price,
                    close=price,
                )
            )

    def _rebuild(
        self,
        items: Iterable[TimedValue[_PriceEvent]],
        *,
        latest_ordinal: int,
    ) -> None:
        self._points.clear()
        self._timestamps.clear()
        self._bucket_seconds.clear()
        self._buckets.clear()
        self._source_positions_by_object.clear()
        self._active_start = 0
        self._base_squared_log_return = 0.0
        self._base_digest_sum = 0
        self._base_digest_xor = 0
        for item in items:
            self._append(item)
        self._last_ordinal = latest_ordinal

    def _rebuild_first_bucket(self) -> None:
        if not self._points or not self._buckets:
            return
        first_second = self._second(self._points[0].timestamp)
        if self._bucket_seconds[0] != first_second:
            raise RuntimeError("price bucket index diverged from cached points")
        end = bisect_left(
            self._timestamps,
            datetime.fromtimestamp(first_second + 1, tz=UTC),
        )
        prices = [point.price for point in self._points[:end]]
        self._buckets[0] = _PriceSecondBucket(
            second=first_second,
            high=max(prices),
            low=min(prices),
            close=prices[-1],
        )

    def _trim_to_retained_window(
        self,
        window: BoundedTimeWindow[_PriceEvent],
    ) -> None:
        earliest = window.earliest
        if earliest is None:
            if self._points:
                self._rebuild((), latest_ordinal=window.latest_ordinal)
            return
        trim = bisect_left(
            self._timestamps,
            earliest.timestamp,
            lo=self._active_start,
        )
        while (
            trim < len(self._points)
            and self._points[trim].timestamp == earliest.timestamp
            and self._points[trim].ordinal != earliest.ordinal
        ):
            trim += 1
        if (
            trim >= len(self._points)
            or self._points[trim].ordinal != earliest.ordinal
        ):
            self._rebuild(window, latest_ordinal=window.latest_ordinal)
            return
        if trim < self._active_start:
            raise RuntimeError("price cache active boundary moved backwards")
        for point in self._points[self._active_start:trim]:
            cached = self._source_positions_by_object.get(
                id(point.source)
            )
            if (
                cached is not None
                and cached[0] is point.source
                and cached[1] == point.timestamp
            ):
                del self._source_positions_by_object[id(point.source)]
        self._active_start = trim
        if self._active_start < self._TRIM_BATCH and len(self._points) <= (
            window.maximum_items + self._TRIM_BATCH
        ):
            return
        if self._active_start <= 0:
            return
        removed_points = self._points[: self._active_start]
        removed = removed_points[-1]
        self._base_squared_log_return = removed.prefix_squared_log_return
        self._base_digest_sum = removed.prefix_digest_sum
        self._base_digest_xor = removed.prefix_digest_xor
        del self._points[: self._active_start]
        del self._timestamps[: self._active_start]
        self._active_start = 0
        if not self._points:
            self._bucket_seconds.clear()
            self._buckets.clear()
            return
        first_second = self._second(self._points[0].timestamp)
        bucket_trim = bisect_left(self._bucket_seconds, first_second)
        del self._bucket_seconds[:bucket_trim]
        del self._buckets[:bucket_trim]
        self._rebuild_first_bucket()

    def update(self, window: BoundedTimeWindow[_PriceEvent]) -> None:
        new_items = window.items_appended_after(self._last_ordinal)
        previous_key = (
            None
            if not self._points
            else (
                self._points[-1].timestamp,
                self._points[-1].ordinal,
            )
        )
        for item in new_items:
            item_key = (item.timestamp, item.ordinal)
            if previous_key is not None and item_key < previous_key:
                self._rebuild(
                    window,
                    latest_ordinal=window.latest_ordinal,
                )
                return
            previous_key = item_key
        for item in new_items:
            self._append(item)
        self._last_ordinal = window.latest_ordinal
        self._trim_to_retained_window(window)

    def _bounds(self, start: datetime, end: datetime) -> tuple[int, int]:
        start_utc = _utc(start)
        end_utc = _utc(end)
        if end_utc < start_utc:
            raise ValueError("price query end cannot precede start")
        return (
            bisect_right(
                self._timestamps,
                start_utc,
                lo=self._active_start,
            ),
            bisect_right(
                self._timestamps,
                end_utc,
                lo=self._active_start,
            ),
        )

    def range(self, start: datetime, end: datetime) -> _PriceRange:
        left, right = self._bounds(start, end)
        if left >= right:
            return _PriceRange(0, None, None, None)
        first = self._points[left]
        last = self._points[right - 1]
        squared = max(
            0.0,
            last.prefix_squared_log_return
            - first.prefix_squared_log_return,
        )
        valid_log_returns = (
            last.prefix_valid_log_returns
            - first.prefix_valid_log_returns
        )
        count = right - left
        return _PriceRange(
            count=count,
            first=first.price,
            last=last.price,
            realized_volatility=(
                None if valid_log_returns < 2 else math.sqrt(squared)
            ),
        )

    def lineage(self, start: datetime, end: datetime) -> SourceLineage:
        left, right = self._bounds(start, end)
        if left >= right:
            return SourceLineage()
        first = self._points[left]
        last = self._points[right - 1]
        before_sum = (
            self._base_digest_sum
            if left == 0
            else self._points[left - 1].prefix_digest_sum
        )
        before_xor = (
            self._base_digest_xor
            if left == 0
            else self._points[left - 1].prefix_digest_xor
        )
        return SourceLineage(
            count=right - left,
            digest_sum=(last.prefix_digest_sum - before_sum) & _DIGEST_MASK,
            digest_xor=last.prefix_digest_xor ^ before_xor,
            oldest_timestamp=first.timestamp,
            newest_timestamp=last.timestamp,
        )

    def covers_source(
        self,
        source: object,
        *,
        start: datetime,
        end: datetime,
    ) -> bool:
        cached = self._source_positions_by_object.get(id(source))
        if cached is None or cached[0] is not source:
            return False
        _cached_source, timestamp = cached
        return _utc(start) < timestamp <= _utc(end)

    def _partial_bucket(
        self,
        second: int,
        *,
        left: int,
        right: int,
    ) -> _PriceSecondBucket | None:
        second_start = datetime.fromtimestamp(second, tz=UTC)
        second_end = datetime.fromtimestamp(second + 1, tz=UTC)
        bucket_left = max(left, bisect_left(self._timestamps, second_start))
        bucket_right = min(right, bisect_left(self._timestamps, second_end))
        if bucket_left >= bucket_right:
            return None
        prices = [
            point.price for point in self._points[bucket_left:bucket_right]
        ]
        return _PriceSecondBucket(
            second=second,
            high=max(prices),
            low=min(prices),
            close=prices[-1],
        )

    def buckets(
        self,
        start: datetime,
        end: datetime,
    ) -> list[_PriceSecondBucket]:
        start_utc = _utc(start)
        end_utc = _utc(end)
        left, right = self._bounds(start_utc, end_utc)
        if left >= right:
            return []
        first_second = self._second(self._points[left].timestamp)
        last_second = self._second(self._points[right - 1].timestamp)
        bucket_left = bisect_left(self._bucket_seconds, first_second)
        bucket_right = bisect_right(self._bucket_seconds, last_second)
        selected: list[_PriceSecondBucket] = []
        for index in range(bucket_left, bucket_right):
            bucket = self._buckets[index]
            if bucket.second in {first_second, last_second}:
                partial = self._partial_bucket(
                    bucket.second,
                    left=left,
                    right=right,
                )
                if partial is not None:
                    selected.append(partial)
            else:
                selected.append(bucket)
        return selected

    def high_low(
        self,
        start: datetime,
        end: datetime,
    ) -> tuple[Decimal | None, Decimal | None]:
        buckets = self.buckets(start, end)
        if not buckets:
            return None, None
        return (
            max(bucket.high for bucket in buckets),
            min(bucket.low for bucket in buckets),
        )

    def atr(self, start: datetime, end: datetime) -> Decimal | None:
        left, right = self._bounds(start, end)
        if right - left < 2:
            return None
        buckets = self.buckets(start, end)
        if not buckets:
            return None
        previous_close: Decimal | None = None
        true_ranges: list[Decimal] = []
        for bucket in buckets:
            true_range = bucket.high - bucket.low
            if previous_close is not None:
                true_range = max(
                    true_range,
                    abs(bucket.high - previous_close),
                    abs(bucket.low - previous_close),
                )
            true_ranges.append(true_range)
            previous_close = bucket.close
        return sum(true_ranges, Decimal(0)) / len(true_ranges)


class SingleVenueFeatureEngine:
    """Calculate typed observations without applying any trade-direction rules."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.config_hash = settings_fingerprint(settings)
        self._history = _MetricHistory(settings)
        self._book_generations: dict[tuple[Exchange, str], int] = {}
        self._price_caches: dict[
            tuple[int, Exchange, str],
            _PriceHistoryCache,
        ] = {}

    def _price_cache(self, state: VenueSymbolState) -> _PriceHistoryCache:
        key = (id(state), state.exchange, state.symbol)
        cache = self._price_caches.setdefault(key, _PriceHistoryCache())
        cache.update(state.prices)
        return cache

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
        price_cache = self._price_cache(state)
        book_view = self._book_view_as_of(state, decision=decision)
        generation_key = (state.exchange, state.symbol)
        previous_generation = self._book_generations.get(generation_key)
        if previous_generation is not None and previous_generation != book_view.generation:
            self._history.clear_scope(f"{state.exchange.value}:{state.symbol}:")
        self._book_generations[generation_key] = book_view.generation
        availability = self._availability_as_of(
            state,
            decision=decision,
            warmup=window,
            book_view=book_view,
        )
        reasons = list(
            availability.reasons
        )

        trades = state.trades.items_between(start, decision)
        previous_trades = state.trades.items_between(start - window, start)
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

        order_book = self._order_book_values(
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
        price_values, return_z, return_warm = self._price_values(
            price_cache,
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
        if not statistical_warm:
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.NOT_WARM,
                    detail="derived statistical history has not covered the configured lookback",
                )
            )
        reasons = list(dict.fromkeys(reasons))

        lineage = self._source_lineage(
            state,
            start=start,
            decision=decision,
            book_view=book_view,
            price_cache=price_cache,
        )
        oldest = lineage.oldest_timestamp
        newest = lineage.newest_timestamp
        source_count = lineage.count
        data_age_ms = (
            None
            if newest is None
            else max(0.0, (decision - newest).total_seconds() * 1000.0)
        )
        is_warm = availability.is_warm and statistical_warm and bool(trades)
        is_healthy = availability.is_healthy
        source_fingerprint = lineage.fingerprint
        feature_id = self._feature_id(
            {
                "strategy_version": self.settings.app.strategy_version,
                "code_version": __version__,
                "config_hash": self.config_hash,
                "exchange": state.exchange,
                "symbol": state.symbol,
                "decision_timestamp": decision,
                "window_seconds": window_seconds,
                "book_generation": book_view.generation,
                "source_sequence_id": book_view.sequence_id,
                "source_event_count": source_count,
                "oldest_source_timestamp": oldest,
                "newest_source_timestamp": newest,
                "data_age_ms": data_age_ms,
                "source_fingerprint": source_fingerprint,
                "is_warm": is_warm,
                "is_healthy": is_healthy,
                "unavailable_reasons": tuple(reasons),
                "trade_flow": trade_flow,
                "order_book": order_book,
                "price": price_values.model_copy(
                    update={"impulse_zscore": return_z}
                ),
                "open_interest": oi_values,
                "crowding": crowding,
                "liquidation": liquidation,
            }
        )
        return FeatureSnapshot(
            exchange=state.exchange,
            symbol=state.symbol,
            exchange_timestamp=decision,
            local_receive_timestamp=decision,
            normalization_timestamp=decision,
            sequence_id=book_view.sequence_id,
            raw_payload_reference=(
                "feature-sources://sha256-sum-xor-v1/"
                f"{source_fingerprint}"
            ),
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

    def _book_view_as_of(
        self,
        state: VenueSymbolState,
        *,
        decision: datetime,
    ) -> FeatureBookView:
        checkpoint = state.book_history.latest_at_or_before(decision)
        if checkpoint is None:
            return FeatureBookView(
                generation=0,
                epoch=0,
                sequence_id=None,
                synchronized=False,
                synchronized_since=None,
                bids=(),
                asks=(),
                pending_updates=0,
                last_error="local feature book has no valid snapshot",
                lineage=SourceLineage(),
            )
        view = checkpoint.value.view
        depth = self.settings.features.order_book_depth
        return FeatureBookView(
            generation=view.generation,
            epoch=view.epoch,
            sequence_id=view.sequence_id,
            synchronized=view.synchronized,
            synchronized_since=view.synchronized_since,
            bids=view.bids[:depth],
            asks=view.asks[:depth],
            pending_updates=view.pending_updates,
            last_error=view.last_error,
            lineage=view.lineage,
        )

    def _availability_as_of(
        self,
        state: VenueSymbolState,
        *,
        decision: datetime,
        warmup: timedelta,
        book_view: FeatureBookView,
    ) -> FeatureAvailability:
        current = evaluate_availability(
            state,
            decision_timestamp=decision,
            warmup=warmup,
            open_interest_stale_after=timedelta(
                milliseconds=self.settings.health.open_interest_stale_after_ms
            ),
            blocked_health_statuses=frozenset(
                HealthStatus(value)
                for value in self.settings.health.block_entry_statuses
            ),
        )
        book_codes = {
            FeatureUnavailableCode.BOOK_UNSYNCHRONIZED,
            FeatureUnavailableCode.BOOK_GENERATION_WARMUP,
        }
        reasons = [
            reason for reason in current.reasons if reason.code not in book_codes
        ]
        if not book_view.synchronized:
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.BOOK_UNSYNCHRONIZED,
                    detail=(
                        book_view.last_error
                        or "local feature book has no valid snapshot"
                    ),
                    channel="order_book",
                )
            )
        generation_started_at = book_view.synchronized_since
        if (
            generation_started_at is None
            or generation_started_at > decision - warmup
        ):
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.BOOK_GENERATION_WARMUP,
                    detail="current book generation has not covered the required warmup",
                    channel="order_book",
                )
            )
        warm_codes = {
            FeatureUnavailableCode.NO_TRADES,
            FeatureUnavailableCode.BOOK_GENERATION_WARMUP,
            FeatureUnavailableCode.OPEN_INTEREST_MISSING,
        }
        health_codes = {
            FeatureUnavailableCode.BOOK_UNSYNCHRONIZED,
            FeatureUnavailableCode.OPEN_INTEREST_STALE,
            FeatureUnavailableCode.HEALTH_BLOCKED,
            FeatureUnavailableCode.PIPELINE_BACKLOG,
        }
        unique_reasons = tuple(dict.fromkeys(reasons))
        return FeatureAvailability(
            is_warm=not any(
                reason.code in warm_codes for reason in unique_reasons
            ),
            is_healthy=not any(
                reason.code in health_codes for reason in unique_reasons
            ),
            reasons=unique_reasons,
        )

    @staticmethod
    def _direct_lineage(
        sources: Iterable[TimedValue[object]],
    ) -> SourceLineage:
        unique: dict[str, TimedValue[object]] = {}
        for item in sources:
            digest = semantic_source_digest(item.timestamp, item.value)
            unique.setdefault(digest, item)
        lineage = SourceLineage()
        for digest in sorted(unique):
            item = unique[digest]
            lineage = lineage.combine(
                SourceLineage.from_digest(item.timestamp, digest)
            )
        return lineage

    @staticmethod
    def _feature_id(identity: object) -> UUID:
        digest = sha256_text(canonical_json(canonicalize_for_hash(identity)))
        return uuid5(NAMESPACE_URL, f"cvf:single-venue:{digest}")

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
        change_items: list[TimedValue[BookChange]] = []
        for checkpoint in state.book_history.items_between(start, decision):
            change = checkpoint.value.change
            if (
                change is not None
                and checkpoint.value.view.generation == view.generation
                and checkpoint.value.view.epoch == view.epoch
            ):
                change_items.append(
                    TimedValue(
                        timestamp=checkpoint.timestamp,
                        value=change,
                        ordinal=checkpoint.ordinal,
                    )
                )
        changes = [item.value for item in change_items]
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
                for item in change_items
                if (
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
                    for item in change_items
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
        price_cache: _PriceHistoryCache,
        *,
        start: datetime,
        decision: datetime,
        history_prefix: str,
    ) -> tuple[PriceFeatureValues, float | None, bool]:
        price_range = price_cache.range(start, decision)
        first = price_range.first
        last = price_range.last
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
        atr = price_cache.atr(
            decision - timedelta(seconds=self.settings.features.atr_period_seconds),
            decision,
        )
        trailing_high, trailing_low = price_cache.high_low(
            decision
            - timedelta(seconds=self.settings.features.breakout_lookback_seconds),
            decision,
        )
        return (
            PriceFeatureValues(
                return_value=return_value,
                realized_volatility=price_range.realized_volatility,
                atr_1m=atr,
                trailing_high=trailing_high,
                trailing_low=trailing_low,
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
        latest = state.open_interest.latest_at_or_before(decision)
        anchor = state.open_interest.latest_at_or_before(start)
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
        funding = state.funding_rates.latest_at_or_before(decision)
        mark = state.mark_prices.latest_at_or_before(decision)
        index = state.index_prices.latest_at_or_before(decision)
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
        events = state.liquidations.values_between(start, decision)
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

    def _source_lineage(
        self,
        state: VenueSymbolState,
        *,
        start: datetime,
        decision: datetime,
        book_view: FeatureBookView,
        price_cache: _PriceHistoryCache,
    ) -> SourceLineage:
        direct_sources: list[TimedValue[object]] = []
        window = decision - start
        price_start = min(
            start,
            decision
            - timedelta(seconds=self.settings.features.atr_period_seconds),
            decision
            - timedelta(seconds=self.settings.features.breakout_lookback_seconds),
        )
        trade_start = start - window
        direct_sources.extend(
            item
            for item in state.trades.items_between(trade_start, decision)
            if not price_cache.covers_source(
                item.value,
                start=price_start,
                end=decision,
            )
        )

        for dependency_item in (
            state.open_interest.latest_at_or_before(start),
            state.open_interest.latest_at_or_before(decision),
            state.funding_rates.latest_at_or_before(decision),
            state.index_prices.latest_at_or_before(decision),
        ):
            if dependency_item is not None:
                direct_sources.append(dependency_item)
        mark_price = state.mark_prices.latest_at_or_before(decision)
        if (
            mark_price is not None
            and not price_cache.covers_source(
                mark_price.value,
                start=price_start,
                end=decision,
            )
        ):
            direct_sources.append(mark_price)
        direct_sources.extend(
            state.liquidations.items_between(start, decision)
        )
        direct_sources.extend(state.health_sources_at_or_before(decision))

        return (
            price_cache.lineage(price_start, decision)
            .combine(book_view.lineage)
            .combine(self._direct_lineage(direct_sources))
        )

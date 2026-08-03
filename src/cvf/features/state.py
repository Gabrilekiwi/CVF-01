"""Bounded per-venue/symbol state used by both live and replay feature pipelines."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from cvf.config import FeaturesConfig
from cvf.features.lineage import SourceLineage
from cvf.features.rolling import (
    AppendStatus,
    BoundedTimeWindow,
    LateEventPolicy,
    TimedValue,
)
from cvf.models.enums import EventType, Exchange, HealthStatus
from cvf.models.market import (
    BestBidAsk,
    ExchangeHealth,
    FundingRate,
    IndexPrice,
    LiquidationEvent,
    MarkPrice,
    OpenInterest,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderBookUpdate,
    Trade,
)
from cvf.monitoring.health import StreamHealthSnapshot
from cvf.normalization.common import NormalizedMarketEvent

type StatefulMarketEvent = (
    Trade
    | BestBidAsk
    | OpenInterest
    | FundingRate
    | MarkPrice
    | IndexPrice
    | LiquidationEvent
)


class StateUpdateStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    LATE_DROPPED = "LATE_DROPPED"
    EXPIRED_DROPPED = "EXPIRED_DROPPED"
    STALE_GENERATION = "STALE_GENERATION"
    STALE_SEQUENCE = "STALE_SEQUENCE"
    BUFFERED_FOR_SNAPSHOT = "BUFFERED_FOR_SNAPSHOT"
    BOOK_INVALIDATED = "BOOK_INVALIDATED"


@dataclass(frozen=True, slots=True)
class StateUpdateResult:
    status: StateUpdateStatus
    exchange: Exchange
    symbol: str
    event_type: EventType
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class FeatureBookView:
    generation: int
    epoch: int
    sequence_id: int | None
    synchronized: bool
    synchronized_since: datetime | None
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    pending_updates: int
    last_error: str | None
    lineage: SourceLineage


@dataclass(frozen=True, slots=True)
class BookChange:
    bid_quantity_delta: Decimal
    ask_quantity_delta: Decimal
    added_bid_quantity: Decimal
    added_ask_quantity: Decimal
    removed_bid_quantity: Decimal
    removed_ask_quantity: Decimal
    order_flow_imbalance: Decimal


@dataclass(frozen=True, slots=True)
class FeatureBookCheckpoint:
    """Immutable event-time book state plus any state-changing source."""

    view: FeatureBookView
    source_event: OrderBookSnapshot | OrderBookUpdate | None
    change: BookChange | None


def _sequence(value: int | str | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


class FeatureOrderBookState:
    """Apply normalized absolute levels and expose a bounded immutable view."""

    def __init__(self, *, pending_capacity: int) -> None:
        if pending_capacity < 1:
            raise ValueError("pending_capacity must be positive")
        self._pending_capacity = pending_capacity
        self._pending: deque[OrderBookUpdate] = deque()
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self.generation = 0
        self.epoch = 0
        self.sequence_id: int | None = None
        self.synchronized = False
        self.synchronized_since: datetime | None = None
        self.last_error: str | None = None
        self.last_change: BookChange | None = None
        self._lineage = SourceLineage()

    def _reset(
        self,
        generation: int,
        reason: str | None = None,
        *,
        advance_epoch: bool = True,
    ) -> None:
        self._bids.clear()
        self._asks.clear()
        self._pending.clear()
        self.generation = generation
        if advance_epoch:
            self.epoch += 1
        self.sequence_id = None
        self.synchronized = False
        self.synchronized_since = None
        self.last_error = reason
        self.last_change = None
        self._lineage = SourceLineage()

    def _record_lineage(
        self,
        event: OrderBookSnapshot | OrderBookUpdate,
    ) -> None:
        self._lineage = self._lineage.combine(
            SourceLineage.from_source(event.exchange_timestamp, event)
        )

    @staticmethod
    def _apply_side(
        side: dict[Decimal, Decimal],
        levels: list[OrderBookLevel],
    ) -> None:
        for level in levels:
            if level.quantity == 0:
                side.pop(level.price, None)
            else:
                side[level.price] = level.quantity

    def _apply_levels(
        self,
        bids: list[OrderBookLevel],
        asks: list[OrderBookLevel],
    ) -> bool:
        self._apply_side(self._bids, bids)
        self._apply_side(self._asks, asks)
        if not self._bids or not self._asks:
            self.synchronized = False
            self.synchronized_since = None
            self.last_error = "order book side became empty"
            return False
        if max(self._bids) >= min(self._asks):
            self.synchronized = False
            self.synchronized_since = None
            self.last_error = "order book became crossed or locked"
            return False
        return True

    def _measure_change(self, update: OrderBookUpdate) -> BookChange:
        bid_deltas = [
            level.quantity - self._bids.get(level.price, Decimal(0))
            for level in update.bids
        ]
        ask_deltas = [
            level.quantity - self._asks.get(level.price, Decimal(0))
            for level in update.asks
        ]
        bid_delta = sum(bid_deltas, Decimal(0))
        ask_delta = sum(ask_deltas, Decimal(0))
        return BookChange(
            bid_quantity_delta=bid_delta,
            ask_quantity_delta=ask_delta,
            added_bid_quantity=sum(
                (delta for delta in bid_deltas if delta > 0),
                Decimal(0),
            ),
            added_ask_quantity=sum(
                (delta for delta in ask_deltas if delta > 0),
                Decimal(0),
            ),
            removed_bid_quantity=sum(
                (-delta for delta in bid_deltas if delta < 0),
                Decimal(0),
            ),
            removed_ask_quantity=sum(
                (-delta for delta in ask_deltas if delta < 0),
                Decimal(0),
            ),
            order_flow_imbalance=bid_delta - ask_delta,
        )

    def _buffer(self, update: OrderBookUpdate) -> StateUpdateStatus:
        if len(self._pending) >= self._pending_capacity:
            previous_lineage = self._lineage
            self._reset(update.generation, "pending order-book update capacity exceeded")
            self._lineage = previous_lineage
            self._record_lineage(update)
            return StateUpdateStatus.BOOK_INVALIDATED
        self._pending.append(update)
        self._record_lineage(update)
        return StateUpdateStatus.BUFFERED_FOR_SNAPSHOT

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> StateUpdateStatus:
        if snapshot.generation < self.generation:
            return StateUpdateStatus.STALE_GENERATION
        snapshot_sequence = _sequence(snapshot.sequence_id)
        if (
            snapshot.generation == self.generation
            and self.synchronized
            and snapshot_sequence is not None
            and self.sequence_id is not None
            and snapshot_sequence <= self.sequence_id
        ):
            return StateUpdateStatus.STALE_SEQUENCE
        pending = (
            list(self._pending)
            if snapshot.generation == self.generation
            else []
        )
        same_generation = snapshot.generation == self.generation
        previous_lineage = self._lineage
        continuing_synchronization = (
            same_generation and self.synchronized
        )
        synchronized_since = self.synchronized_since
        self._reset(
            snapshot.generation,
            advance_epoch=not continuing_synchronization,
        )
        if same_generation:
            self._lineage = previous_lineage
        self._record_lineage(snapshot)
        self._bids = {level.price: level.quantity for level in snapshot.bids}
        self._asks = {level.price: level.quantity for level in snapshot.asks}
        self.sequence_id = _sequence(snapshot.sequence_id)
        self.synchronized = bool(self._bids and self._asks)
        self.synchronized_since = (
            (
                synchronized_since
                if continuing_synchronization
                else snapshot.exchange_timestamp
            )
            if self.synchronized
            else None
        )
        if self.synchronized and max(self._bids) >= min(self._asks):
            self.synchronized = False
            self.synchronized_since = None
            self.last_error = "snapshot is crossed or locked"
            return StateUpdateStatus.BOOK_INVALIDATED
        for update in sorted(
            pending,
            key=lambda event: _sequence(event.sequence_id) or -1,
        ):
            sequence = _sequence(update.sequence_id)
            previous = _sequence(update.previous_sequence_id)
            if sequence is not None and self.sequence_id is not None:
                if sequence <= self.sequence_id:
                    continue
                if previous is not None and previous > self.sequence_id:
                    self.synchronized = False
                    self.synchronized_since = None
                    self.last_error = "pending update has a sequence gap"
                    return StateUpdateStatus.BOOK_INVALIDATED
            change = self._measure_change(update)
            if not self._apply_levels(update.bids, update.asks):
                return StateUpdateStatus.BOOK_INVALIDATED
            self.last_change = change
            self.sequence_id = sequence or self.sequence_id
        return (
            StateUpdateStatus.ACCEPTED
            if self.synchronized
            else StateUpdateStatus.BOOK_INVALIDATED
        )

    def apply_update(self, update: OrderBookUpdate) -> StateUpdateStatus:
        if update.generation < self.generation:
            return StateUpdateStatus.STALE_GENERATION
        if update.generation > self.generation:
            self._reset(update.generation, "book generation changed before snapshot")
            return self._buffer(update)
        if not self.synchronized:
            return self._buffer(update)
        sequence = _sequence(update.sequence_id)
        previous = _sequence(update.previous_sequence_id)
        if sequence is not None and self.sequence_id is not None:
            if sequence <= self.sequence_id:
                return StateUpdateStatus.STALE_SEQUENCE
            if previous is not None and previous != self.sequence_id:
                self.synchronized = False
                self.synchronized_since = None
                self.last_error = "order-book update sequence gap"
                self._pending.clear()
                return self._buffer(update)
        change = self._measure_change(update)
        self._record_lineage(update)
        if not self._apply_levels(update.bids, update.asks):
            return StateUpdateStatus.BOOK_INVALIDATED
        self.last_change = change
        self.sequence_id = sequence or self.sequence_id
        return StateUpdateStatus.ACCEPTED

    def view(self, *, depth: int) -> FeatureBookView:
        if depth < 1:
            raise ValueError("depth must be positive")
        bids = tuple(
            OrderBookLevel(price=price, quantity=self._bids[price])
            for price in sorted(self._bids, reverse=True)[:depth]
        )
        asks = tuple(
            OrderBookLevel(price=price, quantity=self._asks[price])
            for price in sorted(self._asks)[:depth]
        )
        return FeatureBookView(
            generation=self.generation,
            epoch=self.epoch,
            sequence_id=self.sequence_id,
            synchronized=self.synchronized,
            synchronized_since=self.synchronized_since,
            bids=bids,
            asks=asks,
            pending_updates=len(self._pending),
            last_error=self.last_error,
            lineage=self._lineage,
        )


@dataclass(slots=True)
class VenueSymbolState:
    exchange: Exchange
    symbol: str
    trades: BoundedTimeWindow[Trade]
    prices: BoundedTimeWindow[Trade | BestBidAsk | MarkPrice]
    open_interest: BoundedTimeWindow[OpenInterest]
    funding_rates: BoundedTimeWindow[FundingRate]
    mark_prices: BoundedTimeWindow[MarkPrice]
    index_prices: BoundedTimeWindow[IndexPrice]
    liquidations: BoundedTimeWindow[LiquidationEvent]
    book_updates: BoundedTimeWindow[OrderBookSnapshot | OrderBookUpdate]
    book_changes: BoundedTimeWindow[BookChange]
    book_history: BoundedTimeWindow[FeatureBookCheckpoint]
    exchange_health_events: BoundedTimeWindow[ExchangeHealth]
    health_events: BoundedTimeWindow[ExchangeHealth]
    order_book: FeatureOrderBookState
    latest_by_type: dict[EventType, StatefulMarketEvent] = field(default_factory=dict)
    health_by_channel: dict[str, HealthStatus] = field(default_factory=dict)
    accepted_events: int = 0
    rejected_events: int = 0

    def total_items(self) -> int:
        return sum(
            len(window)
            for window in (
                self.trades,
                self.prices,
                self.open_interest,
                self.funding_rates,
                self.mark_prices,
                self.index_prices,
                self.liquidations,
                self.book_updates,
                self.book_changes,
                self.book_history,
                self.health_events,
            )
        )

    def health_sources_at_or_before(
        self,
        boundary: datetime,
    ) -> tuple[TimedValue[ExchangeHealth], ...]:
        """Return the latest deterministic health event for each channel."""

        if boundary.tzinfo is None or boundary.utcoffset() is None:
            raise ValueError("health decision boundary must be timezone-aware")
        decision = boundary.astimezone(UTC)
        candidates = [
            (item.timestamp, scope, item.ordinal, item)
            for scope, window in (
                (0, self.exchange_health_events),
                (1, self.health_events),
            )
            for item in window
            if item.timestamp <= decision
        ]
        latest: dict[str, TimedValue[ExchangeHealth]] = {}
        for _timestamp, _scope, _ordinal, item in sorted(candidates):
            latest[item.value.channel] = item
        return tuple(latest[channel] for channel in sorted(latest))


class MarketStateStore:
    """Route normalized events into isolated, strictly bounded venue/symbol state."""

    def __init__(self, config: FeaturesConfig) -> None:
        self._config = config
        self._states: dict[tuple[Exchange, str], VenueSymbolState] = {}
        self._retention = timedelta(seconds=config.state_retention_seconds)
        self._maximum_lateness = timedelta(milliseconds=config.maximum_lateness_ms)
        self._late_policy = LateEventPolicy(config.late_event_policy)
        self._exchange_health_events = {
            exchange: self._health_window()
            for exchange in (Exchange.BINANCE, Exchange.OKX)
        }

    def _window(self) -> BoundedTimeWindow[object]:
        return BoundedTimeWindow(
            retention=self._retention,
            maximum_items=self._config.maximum_events_per_stream,
            late_event_policy=self._late_policy,
            maximum_lateness=self._maximum_lateness,
        )

    def _book_event_window(
        self,
    ) -> BoundedTimeWindow[OrderBookSnapshot | OrderBookUpdate]:
        """Keep the non-commutative book timeline strictly forward-only."""

        return BoundedTimeWindow(
            retention=self._retention,
            maximum_items=self._config.maximum_events_per_stream,
            late_event_policy=LateEventPolicy.DROP,
            maximum_lateness=timedelta(0),
        )

    def _book_history_window(self) -> BoundedTimeWindow[FeatureBookCheckpoint]:
        return BoundedTimeWindow(
            retention=self._retention,
            maximum_items=self._config.maximum_events_per_stream,
            late_event_policy=LateEventPolicy.INSERT,
            maximum_lateness=self._retention,
        )

    def _health_window(self) -> BoundedTimeWindow[ExchangeHealth]:
        return BoundedTimeWindow(
            retention=self._retention,
            maximum_items=self._config.maximum_events_per_stream,
            late_event_policy=self._late_policy,
            maximum_lateness=self._maximum_lateness,
        )

    def _create_state(self, exchange: Exchange, symbol: str) -> VenueSymbolState:
        return VenueSymbolState(
            exchange=exchange,
            symbol=symbol,
            trades=self._window(),  # type: ignore[arg-type]
            prices=self._window(),  # type: ignore[arg-type]
            open_interest=self._window(),  # type: ignore[arg-type]
            funding_rates=self._window(),  # type: ignore[arg-type]
            mark_prices=self._window(),  # type: ignore[arg-type]
            index_prices=self._window(),  # type: ignore[arg-type]
            liquidations=self._window(),  # type: ignore[arg-type]
            book_updates=self._book_event_window(),
            book_changes=self._window(),  # type: ignore[arg-type]
            book_history=self._book_history_window(),
            exchange_health_events=self._exchange_health_events[exchange],
            health_events=self._health_window(),
            order_book=FeatureOrderBookState(
                pending_capacity=self._config.book_pending_updates
            ),
        )

    def state(self, exchange: Exchange, symbol: str) -> VenueSymbolState:
        if exchange not in {Exchange.BINANCE, Exchange.OKX}:
            raise ValueError("feature market state requires a concrete exchange")
        key = (exchange, symbol)
        state = self._states.get(key)
        if state is None:
            state = self._create_state(exchange, symbol)
            self._states[key] = state
        return state

    @property
    def states(self) -> tuple[VenueSymbolState, ...]:
        return tuple(self._states.values())

    @property
    def retained_items(self) -> int:
        return (
            sum(state.total_items() for state in self._states.values())
            + sum(len(window) for window in self._exchange_health_events.values())
        )

    @staticmethod
    def _window_status(status: AppendStatus) -> StateUpdateStatus:
        if status is AppendStatus.LATE_DROPPED:
            return StateUpdateStatus.LATE_DROPPED
        if status is AppendStatus.EXPIRED_DROPPED:
            return StateUpdateStatus.EXPIRED_DROPPED
        return StateUpdateStatus.ACCEPTED

    def ingest(self, event: NormalizedMarketEvent) -> StateUpdateResult:
        if isinstance(event, ExchangeHealth):
            status = self.update_health(event)
            return StateUpdateResult(
                status=status,
                exchange=event.exchange,
                symbol=event.symbol,
                event_type=event.event_type,
                reason=None,
            )
        state = self.state(event.exchange, event.symbol)
        status = StateUpdateStatus.ACCEPTED
        event_at = event.exchange_timestamp
        book_change: BookChange | None = None
        if isinstance(event, OrderBookSnapshot):
            admissibility = self._window_status(
                state.book_updates.admissibility(event_at)
            )
            if admissibility is not StateUpdateStatus.ACCEPTED:
                state.rejected_events += 1
                return StateUpdateResult(
                    status=admissibility,
                    exchange=event.exchange,
                    symbol=event.symbol,
                    event_type=event.event_type,
                    reason="order-book event is outside the admissible event-time bound",
                )
            previous_generation = state.order_book.generation
            status = state.order_book.apply_snapshot(event)
            if state.order_book.generation != previous_generation:
                state.book_updates.clear()
                state.book_changes.clear()
            if status is StateUpdateStatus.ACCEPTED:
                state.book_updates.clear()
                window_status = state.book_updates.append(event_at, event)
                status = self._window_status(window_status)
        elif isinstance(event, OrderBookUpdate):
            admissibility = self._window_status(
                state.book_updates.admissibility(event_at)
            )
            if admissibility is not StateUpdateStatus.ACCEPTED:
                state.rejected_events += 1
                return StateUpdateResult(
                    status=admissibility,
                    exchange=event.exchange,
                    symbol=event.symbol,
                    event_type=event.event_type,
                    reason="order-book event is outside the admissible event-time bound",
                )
            previous_generation = state.order_book.generation
            status = state.order_book.apply_update(event)
            if state.order_book.generation != previous_generation:
                state.book_updates.clear()
                state.book_changes.clear()
            if status is StateUpdateStatus.ACCEPTED:
                status = self._window_status(state.book_updates.append(event_at, event))
                change = state.order_book.last_change
                if change is not None and status is StateUpdateStatus.ACCEPTED:
                    book_change = change
                    status = self._window_status(state.book_changes.append(event_at, change))
        elif isinstance(event, Trade):
            status = self._window_status(state.trades.append(event_at, event))
            if status is StateUpdateStatus.ACCEPTED:
                price_status = self._window_status(state.prices.append(event_at, event))
                if price_status is not StateUpdateStatus.ACCEPTED:
                    status = price_status
        elif isinstance(event, BestBidAsk):
            status = self._window_status(state.prices.append(event_at, event))
        elif isinstance(event, OpenInterest):
            status = self._window_status(state.open_interest.append(event_at, event))
        elif isinstance(event, FundingRate):
            status = self._window_status(state.funding_rates.append(event_at, event))
        elif isinstance(event, MarkPrice):
            status = self._window_status(state.mark_prices.append(event_at, event))
            if status is StateUpdateStatus.ACCEPTED:
                status = self._window_status(state.prices.append(event_at, event))
        elif isinstance(event, IndexPrice):
            status = self._window_status(state.index_prices.append(event_at, event))
        elif isinstance(event, LiquidationEvent):
            status = self._window_status(state.liquidations.append(event_at, event))

        if status in {
            StateUpdateStatus.STALE_GENERATION,
            StateUpdateStatus.STALE_SEQUENCE,
        }:
            state.rejected_events += 1
            return StateUpdateResult(
                status=status,
                exchange=event.exchange,
                symbol=event.symbol,
                event_type=event.event_type,
                reason=(
                    "order-book generation is stale"
                    if status is StateUpdateStatus.STALE_GENERATION
                    else "order-book sequence does not advance current state"
                ),
            )

        if isinstance(event, (OrderBookSnapshot, OrderBookUpdate)):
            history_status = self._window_status(
                state.book_history.append(
                    event_at,
                    FeatureBookCheckpoint(
                        view=state.order_book.view(
                            depth=self._config.order_book_depth
                        ),
                        source_event=(
                            event
                            if status
                            not in {
                                StateUpdateStatus.LATE_DROPPED,
                                StateUpdateStatus.EXPIRED_DROPPED,
                            }
                            else None
                        ),
                        change=(
                            book_change
                            if status is StateUpdateStatus.ACCEPTED
                            else None
                        ),
                    ),
                )
            )
            if (
                status is StateUpdateStatus.ACCEPTED
                and history_status is not StateUpdateStatus.ACCEPTED
            ):
                status = history_status

        if status is StateUpdateStatus.ACCEPTED:
            state.accepted_events += 1
            if isinstance(
                event,
                (
                    Trade,
                    BestBidAsk,
                    OpenInterest,
                    FundingRate,
                    MarkPrice,
                    IndexPrice,
                    LiquidationEvent,
                ),
            ):
                state.latest_by_type[event.event_type] = event
        else:
            state.rejected_events += 1
        return StateUpdateResult(
            status=status,
            exchange=event.exchange,
            symbol=event.symbol,
            event_type=event.event_type,
            reason=(
                state.order_book.last_error
                if status is not StateUpdateStatus.ACCEPTED
                else None
            ),
        )

    def update_health(self, health: ExchangeHealth) -> StateUpdateStatus:
        if health.exchange not in self._exchange_health_events:
            raise ValueError("feature health requires a concrete market-data exchange")
        event_at = health.exchange_timestamp
        if health.symbol == "*":
            status = self._window_status(
                self._exchange_health_events[health.exchange].append(
                    event_at,
                    health,
                )
            )
            if status is not StateUpdateStatus.ACCEPTED:
                return status
            for state in self._states.values():
                if state.exchange is health.exchange:
                    state.health_by_channel[health.channel] = health.status
            return status
        state = self.state(health.exchange, health.symbol)
        status = self._window_status(state.health_events.append(event_at, health))
        if status is StateUpdateStatus.ACCEPTED:
            state.health_by_channel[health.channel] = health.status
        return status

    def update_stream_health(self, health: StreamHealthSnapshot) -> None:
        if health.key.symbol == "*":
            for state in self._states.values():
                if state.exchange is health.key.exchange:
                    state.health_by_channel[health.key.channel] = health.status
            return
        state = self.state(health.key.exchange, health.key.symbol)
        state.health_by_channel[health.key.channel] = health.status

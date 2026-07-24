"""Bounded per-venue/symbol state used by both live and replay feature pipelines."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import timedelta
from decimal import Decimal
from enum import StrEnum

from cvf.config import FeaturesConfig
from cvf.features.rolling import AppendStatus, BoundedTimeWindow, LateEventPolicy
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
    sequence_id: int | None
    synchronized: bool
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    pending_updates: int
    last_error: str | None


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
        self.sequence_id: int | None = None
        self.synchronized = False
        self.last_error: str | None = None

    def _reset(self, generation: int, reason: str | None = None) -> None:
        self._bids.clear()
        self._asks.clear()
        self._pending.clear()
        self.generation = generation
        self.sequence_id = None
        self.synchronized = False
        self.last_error = reason

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
            self.last_error = "order book side became empty"
            return False
        if max(self._bids) >= min(self._asks):
            self.synchronized = False
            self.last_error = "order book became crossed or locked"
            return False
        return True

    def _buffer(self, update: OrderBookUpdate) -> StateUpdateStatus:
        if len(self._pending) >= self._pending_capacity:
            self._reset(update.generation, "pending order-book update capacity exceeded")
            return StateUpdateStatus.BOOK_INVALIDATED
        self._pending.append(update)
        return StateUpdateStatus.BUFFERED_FOR_SNAPSHOT

    def apply_snapshot(self, snapshot: OrderBookSnapshot) -> StateUpdateStatus:
        if snapshot.generation < self.generation:
            return StateUpdateStatus.STALE_GENERATION
        pending = (
            list(self._pending)
            if snapshot.generation == self.generation
            else []
        )
        self._reset(snapshot.generation)
        self._bids = {level.price: level.quantity for level in snapshot.bids}
        self._asks = {level.price: level.quantity for level in snapshot.asks}
        self.sequence_id = _sequence(snapshot.sequence_id)
        self.synchronized = bool(self._bids and self._asks)
        if self.synchronized and max(self._bids) >= min(self._asks):
            self.synchronized = False
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
                    self.last_error = "pending update has a sequence gap"
                    return StateUpdateStatus.BOOK_INVALIDATED
            if not self._apply_levels(update.bids, update.asks):
                return StateUpdateStatus.BOOK_INVALIDATED
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
                return StateUpdateStatus.STALE_GENERATION
            if previous is not None and previous != self.sequence_id:
                self.synchronized = False
                self.last_error = "order-book update sequence gap"
                self._pending.clear()
                return self._buffer(update)
        if not self._apply_levels(update.bids, update.asks):
            return StateUpdateStatus.BOOK_INVALIDATED
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
            sequence_id=self.sequence_id,
            synchronized=self.synchronized,
            bids=bids,
            asks=asks,
            pending_updates=len(self._pending),
            last_error=self.last_error,
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
            )
        )


class MarketStateStore:
    """Route normalized events into isolated, strictly bounded venue/symbol state."""

    def __init__(self, config: FeaturesConfig) -> None:
        self._config = config
        self._states: dict[tuple[Exchange, str], VenueSymbolState] = {}
        self._retention = timedelta(seconds=config.state_retention_seconds)
        self._maximum_lateness = timedelta(milliseconds=config.maximum_lateness_ms)
        self._late_policy = LateEventPolicy(config.late_event_policy)

    def _window(self) -> BoundedTimeWindow[object]:
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
            book_updates=self._window(),  # type: ignore[arg-type]
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

    @staticmethod
    def _window_status(status: AppendStatus) -> StateUpdateStatus:
        if status is AppendStatus.LATE_DROPPED:
            return StateUpdateStatus.LATE_DROPPED
        if status is AppendStatus.EXPIRED_DROPPED:
            return StateUpdateStatus.EXPIRED_DROPPED
        return StateUpdateStatus.ACCEPTED

    def ingest(self, event: NormalizedMarketEvent) -> StateUpdateResult:
        state = self.state(event.exchange, event.symbol)
        status = StateUpdateStatus.ACCEPTED
        event_at = event.exchange_timestamp
        if isinstance(event, OrderBookSnapshot):
            previous_generation = state.order_book.generation
            status = state.order_book.apply_snapshot(event)
            if event.generation != previous_generation:
                state.book_updates.clear()
            if status is StateUpdateStatus.ACCEPTED:
                state.book_updates.clear()
                window_status = state.book_updates.append(event_at, event)
                status = self._window_status(window_status)
        elif isinstance(event, OrderBookUpdate):
            previous_generation = state.order_book.generation
            status = state.order_book.apply_update(event)
            if event.generation != previous_generation:
                state.book_updates.clear()
            if status is StateUpdateStatus.ACCEPTED:
                status = self._window_status(state.book_updates.append(event_at, event))
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

    def update_health(self, health: ExchangeHealth) -> None:
        if health.symbol == "*":
            for state in self._states.values():
                if state.exchange is health.exchange:
                    state.health_by_channel[health.channel] = health.status
            return
        self.state(health.exchange, health.symbol).health_by_channel[health.channel] = health.status

    def update_stream_health(self, health: StreamHealthSnapshot) -> None:
        if health.key.symbol == "*":
            for state in self._states.values():
                if state.exchange is health.key.exchange:
                    state.health_by_channel[health.key.channel] = health.status
            return
        state = self.state(health.key.exchange, health.key.symbol)
        state.health_by_channel[health.key.channel] = health.status

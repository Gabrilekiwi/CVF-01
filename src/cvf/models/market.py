"""Normalized public market-data and feature models."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from cvf.models.common import (
    EventBase,
    FrozenModel,
    NonNegativeDecimal,
    PositiveDecimal,
    ensure_finite_number,
)
from cvf.models.enums import (
    AggressorSide,
    EventType,
    HealthStatus,
    LiquidatedPositionSide,
)


class OrderBookLevel(FrozenModel):
    price: PositiveDecimal
    quantity: NonNegativeDecimal


class Trade(EventBase):
    event_type: Literal[EventType.TRADE] = EventType.TRADE
    trade_id: str
    price: PositiveDecimal
    quantity: PositiveDecimal
    contract_quantity: PositiveDecimal | None = None
    aggressor_side: AggressorSide

    @property
    def notional(self) -> Decimal:
        return self.price * self.quantity

    @property
    def base_quantity(self) -> Decimal:
        """Normalized quantity in base-asset units."""

        return self.quantity

    @property
    def quote_notional(self) -> Decimal:
        """Trade notional in quote-asset units."""

        return self.notional


class OrderBookSnapshot(EventBase):
    event_type: Literal[EventType.ORDER_BOOK_SNAPSHOT] = EventType.ORDER_BOOK_SNAPSHOT
    bids: list[OrderBookLevel] = Field(min_length=1)
    asks: list[OrderBookLevel] = Field(min_length=1)
    depth: int = Field(ge=1)
    checksum: str | None = None
    generation: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_book(self) -> OrderBookSnapshot:
        if len(self.bids) > self.depth or len(self.asks) > self.depth:
            raise ValueError("book sides cannot contain more levels than depth")
        if any(
            left.price <= right.price for left, right in zip(self.bids, self.bids[1:], strict=False)
        ):
            raise ValueError("bids must be strictly descending by price")
        if any(
            left.price >= right.price for left, right in zip(self.asks, self.asks[1:], strict=False)
        ):
            raise ValueError("asks must be strictly ascending by price")
        if self.bids[0].price >= self.asks[0].price:
            raise ValueError("order book is crossed or locked")
        return self


class OrderBookUpdate(EventBase):
    event_type: Literal[EventType.ORDER_BOOK_UPDATE] = EventType.ORDER_BOOK_UPDATE
    bids: list[OrderBookLevel] = Field(default_factory=list)
    asks: list[OrderBookLevel] = Field(default_factory=list)
    previous_sequence_id: int | str | None = None
    checksum: str | None = None
    generation: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def update_is_not_empty(self) -> OrderBookUpdate:
        if not self.bids and not self.asks:
            raise ValueError("an order-book update must change at least one level")
        return self


class BestBidAsk(EventBase):
    event_type: Literal[EventType.BEST_BID_ASK] = EventType.BEST_BID_ASK
    bid_price: PositiveDecimal
    bid_quantity: NonNegativeDecimal
    ask_price: PositiveDecimal
    ask_quantity: NonNegativeDecimal

    @model_validator(mode="after")
    def quote_is_not_crossed(self) -> BestBidAsk:
        if self.bid_price >= self.ask_price:
            raise ValueError("best bid must be below best ask")
        return self

    @property
    def mid_price(self) -> Decimal:
        return (self.bid_price + self.ask_price) / Decimal(2)


class OpenInterest(EventBase):
    event_type: Literal[EventType.OPEN_INTEREST] = EventType.OPEN_INTEREST
    open_interest_contracts: NonNegativeDecimal
    open_interest_base: NonNegativeDecimal | None = None
    open_interest_quote: NonNegativeDecimal | None = None


class FundingRate(EventBase):
    event_type: Literal[EventType.FUNDING_RATE] = EventType.FUNDING_RATE
    funding_rate: Decimal
    next_funding_timestamp: datetime | None = None

    @field_validator("next_funding_timestamp")
    @classmethod
    def funding_timestamp_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        return EventBase.timestamp_is_timezone_aware(value)


class MarkPrice(EventBase):
    event_type: Literal[EventType.MARK_PRICE] = EventType.MARK_PRICE
    mark_price: PositiveDecimal


class IndexPrice(EventBase):
    event_type: Literal[EventType.INDEX_PRICE] = EventType.INDEX_PRICE
    index_price: PositiveDecimal


class LiquidationEvent(EventBase):
    event_type: Literal[EventType.LIQUIDATION] = EventType.LIQUIDATION
    liquidation_id: str | None = None
    position_side: LiquidatedPositionSide
    price: PositiveDecimal
    quantity: PositiveDecimal
    contract_quantity: PositiveDecimal | None = None

    @property
    def notional(self) -> Decimal:
        return self.price * self.quantity

    @property
    def base_quantity(self) -> Decimal:
        """Normalized liquidation quantity in base-asset units."""

        return self.quantity

    @property
    def quote_notional(self) -> Decimal:
        """Liquidated notional in quote-asset units."""

        return self.notional


class ExchangeHealth(EventBase):
    event_type: Literal[EventType.EXCHANGE_HEALTH] = EventType.EXCHANGE_HEALTH
    channel: str = "*"
    status: HealthStatus
    is_connected: bool
    last_event_timestamp: datetime | None = None
    last_latency_ms: float | None = None
    average_latency_ms: float | None = None
    maximum_latency_ms: float | None = None
    last_normalization_latency_ms: float | None = None
    clock_skew_ms: float | None = None
    messages_received: int = Field(default=0, ge=0)
    duplicate_events: int = Field(default=0, ge=0)
    sequence_gaps: int = Field(default=0, ge=0)
    checksum_failures: int = Field(default=0, ge=0)
    reconnects: int = Field(default=0, ge=0)
    resubscriptions: int = Field(default=0, ge=0)
    parse_errors: int = Field(default=0, ge=0)
    dropped_events: int = Field(default=0, ge=0)
    backpressure_events: int = Field(default=0, ge=0)
    sequence_gap_detected: bool = False
    resyncing: bool = False
    book_generation: int = Field(default=0, ge=0)
    rest_healthy: bool = True
    open_interest_stale: bool = False
    last_error: str | None = None
    details: dict[str, str | int | float | bool | None] = Field(default_factory=dict)

    @field_validator("last_event_timestamp")
    @classmethod
    def last_event_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        return EventBase.timestamp_is_timezone_aware(value)

    @field_validator("channel")
    @classmethod
    def channel_is_not_empty(cls, value: str) -> str:
        if not value or value.isspace():
            raise ValueError("channel cannot be empty")
        return value

    @model_validator(mode="after")
    def state_fields_are_consistent(self) -> ExchangeHealth:
        if self.status is HealthStatus.DISCONNECTED and self.is_connected:
            raise ValueError("DISCONNECTED health cannot report an active connection")
        if self.status is not HealthStatus.DISCONNECTED and not self.is_connected:
            raise ValueError(f"{self.status} health requires an active transport connection")
        if self.resyncing != (self.status is HealthStatus.RESYNCING):
            raise ValueError("resyncing must be true exactly when status is RESYNCING")
        return self


class MarketFeature(EventBase):
    event_type: Literal[EventType.MARKET_FEATURE] = EventType.MARKET_FEATURE
    feature_snapshot_id: UUID = Field(default_factory=uuid4)
    window_seconds: int = Field(gt=0)
    values: dict[str, float | int | Decimal | None] = Field(min_length=1)
    is_warm: bool
    source_event_count: int = Field(ge=0)

    @field_validator("values")
    @classmethod
    def values_are_finite(
        cls, values: dict[str, float | int | Decimal | None]
    ) -> dict[str, float | int | Decimal | None]:
        return {key: ensure_finite_number(value) for key, value in values.items()}

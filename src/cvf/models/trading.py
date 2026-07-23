"""Signal and paper-trading data models."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from cvf.models.common import EventBase, FrozenModel, NonNegativeDecimal, PositiveDecimal
from cvf.models.enums import (
    EventType,
    Exchange,
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    PositionStatus,
    SignalType,
    TradePurpose,
)


class SignalReason(FrozenModel):
    code: str
    message: str
    exchange: Exchange | None = None
    value: float | None = None
    threshold: float | None = None


class BlockingCondition(FrozenModel):
    code: str
    message: str
    exchange: Exchange | None = None


class TradingSignal(EventBase):
    event_type: Literal[EventType.TRADING_SIGNAL] = EventType.TRADING_SIGNAL
    signal_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime
    signal_type: SignalType
    binance_score: float
    okx_score: float
    combined_score: float
    confidence: float = Field(ge=0, le=1)
    suggested_exchange: Exchange | None = None
    suggested_entry_price: PositiveDecimal | None = None
    stop_loss: PositiveDecimal | None = None
    take_profit_1: PositiveDecimal | None = None
    take_profit_2: PositiveDecimal | None = None
    expires_at: datetime
    reasons: list[SignalReason] = Field(default_factory=list)
    blocking_conditions: list[BlockingCondition] = Field(default_factory=list)
    feature_snapshot_id: UUID
    strategy_version: str

    @field_validator("timestamp", "expires_at")
    @classmethod
    def signal_timestamp_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("signal timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_signal_shape(self) -> TradingSignal:
        if self.expires_at <= self.timestamp:
            raise ValueError("expires_at must be after timestamp")
        if self.suggested_exchange in {Exchange.CROSS_VENUE, Exchange.SIMULATED}:
            raise ValueError("suggested_exchange must be BINANCE, OKX, or null")

        entry_types = {SignalType.LONG_ENTRY, SignalType.SHORT_ENTRY}
        prices = (
            self.suggested_entry_price,
            self.stop_loss,
            self.take_profit_1,
            self.take_profit_2,
        )
        if self.signal_type in entry_types:
            if self.suggested_exchange is None or any(price is None for price in prices):
                raise ValueError("entry signals require an exchange and all entry/exit prices")
            entry, stop, target_1, target_2 = prices
            assert entry is not None and stop is not None
            assert target_1 is not None and target_2 is not None
            if self.signal_type is SignalType.LONG_ENTRY and not (
                stop < entry < target_1 < target_2
            ):
                raise ValueError("long entry prices must satisfy stop < entry < tp1 < tp2")
            if self.signal_type is SignalType.SHORT_ENTRY and not (
                target_2 < target_1 < entry < stop
            ):
                raise ValueError("short entry prices must satisfy tp2 < tp1 < entry < stop")
        return self


class SimulatedOrder(EventBase):
    event_type: Literal[EventType.SIMULATED_ORDER] = EventType.SIMULATED_ORDER
    order_id: UUID = Field(default_factory=uuid4)
    signal_id: UUID
    side: OrderSide
    position_side: PositionSide
    order_type: OrderType = OrderType.MARKET
    requested_quantity: PositiveDecimal
    filled_quantity: NonNegativeDecimal = Decimal(0)
    average_fill_price: PositiveDecimal | None = None
    fee: NonNegativeDecimal = Decimal(0)
    estimated_slippage_bps: float = Field(ge=0)
    status: OrderStatus = OrderStatus.CREATED
    created_at: datetime
    completed_at: datetime | None = None
    rejection_reason: str | None = None

    @field_validator("created_at", "completed_at")
    @classmethod
    def order_timestamp_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("order timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_fill(self) -> SimulatedOrder:
        if self.filled_quantity > self.requested_quantity:
            raise ValueError("filled quantity cannot exceed requested quantity")
        if self.average_fill_price is not None and self.filled_quantity == 0:
            raise ValueError("average fill price requires a positive fill quantity")
        if self.completed_at is not None and self.completed_at < self.created_at:
            raise ValueError("completed_at cannot precede created_at")
        return self


class SimulatedPosition(EventBase):
    event_type: Literal[EventType.SIMULATED_POSITION] = EventType.SIMULATED_POSITION
    position_id: UUID = Field(default_factory=uuid4)
    opening_signal_id: UUID
    side: PositionSide
    status: PositionStatus
    entry_price: PositiveDecimal
    original_quantity: PositiveDecimal
    remaining_quantity: NonNegativeDecimal
    stop_loss: PositiveDecimal
    take_profit_1: PositiveDecimal
    take_profit_2: PositiveDecimal
    opened_at: datetime
    closed_at: datetime | None = None
    realized_pnl: Decimal = Decimal(0)
    unrealized_pnl: Decimal = Decimal(0)
    fees_paid: NonNegativeDecimal = Decimal(0)
    funding_paid: Decimal = Decimal(0)

    @field_validator("opened_at", "closed_at")
    @classmethod
    def position_timestamp_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("position timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_position(self) -> SimulatedPosition:
        if self.remaining_quantity > self.original_quantity:
            raise ValueError("remaining quantity cannot exceed original quantity")
        if self.closed_at is not None and self.closed_at < self.opened_at:
            raise ValueError("closed_at cannot precede opened_at")
        if self.status is PositionStatus.CLOSED and self.remaining_quantity != 0:
            raise ValueError("closed positions must have zero remaining quantity")
        if self.side is PositionSide.LONG and not (
            self.stop_loss < self.entry_price < self.take_profit_1 < self.take_profit_2
        ):
            raise ValueError("long position levels are not ordered")
        if self.side is PositionSide.SHORT and not (
            self.take_profit_2 < self.take_profit_1 < self.entry_price < self.stop_loss
        ):
            raise ValueError("short position levels are not ordered")
        return self


class SimulatedTrade(EventBase):
    event_type: Literal[EventType.SIMULATED_TRADE] = EventType.SIMULATED_TRADE
    simulated_trade_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    position_id: UUID
    purpose: TradePurpose
    side: OrderSide
    price: PositiveDecimal
    quantity: PositiveDecimal
    fee: NonNegativeDecimal
    slippage_bps: float = Field(ge=0)
    realized_pnl: Decimal = Decimal(0)
    executed_at: datetime

    @field_validator("executed_at")
    @classmethod
    def execution_timestamp_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("executed_at must be timezone-aware")
        return value.astimezone(UTC)


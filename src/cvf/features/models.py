"""Versioned, typed feature snapshot contract for Phase 3 and later signals."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from cvf.models.common import EventBase, FrozenModel, ensure_finite_number
from cvf.models.enums import EventType


class FeatureUnavailableCode(StrEnum):
    NOT_WARM = "NOT_WARM"
    NO_TRADES = "NO_TRADES"
    BOOK_UNSYNCHRONIZED = "BOOK_UNSYNCHRONIZED"
    BOOK_GENERATION_WARMUP = "BOOK_GENERATION_WARMUP"
    OPEN_INTEREST_MISSING = "OPEN_INTEREST_MISSING"
    OPEN_INTEREST_STALE = "OPEN_INTEREST_STALE"
    HEALTH_BLOCKED = "HEALTH_BLOCKED"
    PIPELINE_BACKLOG = "PIPELINE_BACKLOG"
    EVENT_GAP = "EVENT_GAP"
    TIME_ALIGNMENT = "TIME_ALIGNMENT"


class FeatureUnavailableReason(FrozenModel):
    code: FeatureUnavailableCode
    detail: str
    channel: str | None = None


def _finite(value: float | None) -> float | None:
    result = ensure_finite_number(value)
    return None if result is None else float(result)


class TradeFlowFeatureValues(FrozenModel):
    aggressive_buy_notional: Decimal | None = None
    aggressive_sell_notional: Decimal | None = None
    taker_imbalance: float | None = None
    trade_notional_impulse: float | None = None
    trade_count_impulse: float | None = None
    average_trade_notional: Decimal | None = None
    large_trade_share: float | None = None

    @field_validator(
        "taker_imbalance",
        "trade_notional_impulse",
        "trade_count_impulse",
        "large_trade_share",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class OrderBookFeatureValues(FrozenModel):
    weighted_bid_depth: Decimal | None = None
    weighted_ask_depth: Decimal | None = None
    depth_imbalance: float | None = None
    spread: Decimal | None = None
    relative_spread: float | None = None
    mid_price: Decimal | None = None
    microprice: Decimal | None = None
    buy_slippage_bps: float | None = None
    sell_slippage_bps: float | None = None
    order_flow_imbalance: float | None = None
    order_flow_imbalance_zscore: float | None = None

    @field_validator(
        "depth_imbalance",
        "relative_spread",
        "buy_slippage_bps",
        "sell_slippage_bps",
        "order_flow_imbalance",
        "order_flow_imbalance_zscore",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class PriceFeatureValues(FrozenModel):
    return_value: float | None = None
    impulse_zscore: float | None = None
    realized_volatility: float | None = None
    atr_1m: Decimal | None = None
    trailing_high: Decimal | None = None
    trailing_low: Decimal | None = None
    recent_move_atr: float | None = None
    abnormal_jump: bool | None = None

    @field_validator(
        "return_value",
        "impulse_zscore",
        "realized_volatility",
        "recent_move_atr",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class OpenInterestFeatureValues(FrozenModel):
    change: Decimal | None = None
    percentage_change: float | None = None
    zscore: float | None = None
    data_age_ms: float | None = None

    @field_validator("percentage_change", "zscore", "data_age_ms")
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class CrowdingFeatureValues(FrozenModel):
    funding_rate: Decimal | None = None
    funding_zscore: float | None = None
    mark_index_premium: float | None = None
    premium_zscore: float | None = None

    @field_validator("funding_zscore", "mark_index_premium", "premium_zscore")
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class LiquidationFeatureValues(FrozenModel):
    public_sample_long_notional: Decimal | None = None
    public_sample_short_notional: Decimal | None = None
    public_sample_activity_zscore: float | None = None

    @field_validator("public_sample_activity_zscore")
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class FeatureSnapshot(EventBase):
    """No-lookahead snapshot with explicit absence and source boundaries."""

    event_type: Literal[EventType.MARKET_FEATURE] = EventType.MARKET_FEATURE
    feature_snapshot_id: UUID = Field(default_factory=uuid4)
    schema_version: Literal[1] = 1
    strategy_version: str
    calculation_timestamp: datetime
    decision_timestamp: datetime
    window_seconds: int = Field(gt=0)
    book_generation: int = Field(ge=0)
    source_sequence_id: int | str | None = None
    source_event_count: int = Field(ge=0)
    oldest_source_timestamp: datetime | None = None
    newest_source_timestamp: datetime | None = None
    data_age_ms: float = Field(ge=0)
    is_warm: bool
    is_healthy: bool
    unavailable_reasons: tuple[FeatureUnavailableReason, ...] = ()
    trade_flow: TradeFlowFeatureValues | None = None
    order_book: OrderBookFeatureValues | None = None
    price: PriceFeatureValues | None = None
    open_interest: OpenInterestFeatureValues | None = None
    crowding: CrowdingFeatureValues | None = None
    liquidation: LiquidationFeatureValues | None = None

    @field_validator(
        "calculation_timestamp",
        "decision_timestamp",
        "oldest_source_timestamp",
        "newest_source_timestamp",
    )
    @classmethod
    def feature_timestamp_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("feature timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_snapshot_boundaries(self) -> FeatureSnapshot:
        if self.calculation_timestamp < self.decision_timestamp:
            raise ValueError("calculation_timestamp cannot precede decision_timestamp")
        if self.source_event_count == 0:
            if self.oldest_source_timestamp is not None or self.newest_source_timestamp is not None:
                raise ValueError("empty snapshots cannot claim source timestamp bounds")
        elif self.oldest_source_timestamp is None or self.newest_source_timestamp is None:
            raise ValueError("nonempty snapshots require both source timestamp bounds")
        if (
            self.oldest_source_timestamp is not None
            and self.newest_source_timestamp is not None
        ):
            if self.oldest_source_timestamp > self.newest_source_timestamp:
                raise ValueError("source timestamp bounds are reversed")
            if self.newest_source_timestamp > self.decision_timestamp:
                raise ValueError("feature snapshot contains a future source event")
        if self.is_warm and self.is_healthy and self.unavailable_reasons:
            raise ValueError("available snapshots cannot have unavailable reasons")
        if (not self.is_warm or not self.is_healthy) and not self.unavailable_reasons:
            raise ValueError("unavailable snapshots require structured reasons")
        return self

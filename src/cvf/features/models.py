"""Versioned, typed feature snapshot contract for Phase 3 and later signals."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from cvf.models.common import EventBase, FrozenModel, ensure_finite_number
from cvf.models.enums import EventType, Exchange


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
    MISSING_BINANCE_SNAPSHOT = "MISSING_BINANCE_SNAPSHOT"
    MISSING_OKX_SNAPSHOT = "MISSING_OKX_SNAPSHOT"
    FUTURE_BINANCE_SNAPSHOT = "FUTURE_BINANCE_SNAPSHOT"
    FUTURE_OKX_SNAPSHOT = "FUTURE_OKX_SNAPSHOT"
    STALE_BINANCE_SNAPSHOT = "STALE_BINANCE_SNAPSHOT"
    STALE_OKX_SNAPSHOT = "STALE_OKX_SNAPSHOT"
    BINANCE_UNHEALTHY = "BINANCE_UNHEALTHY"
    OKX_UNHEALTHY = "OKX_UNHEALTHY"
    BINANCE_NOT_WARM = "BINANCE_NOT_WARM"
    OKX_NOT_WARM = "OKX_NOT_WARM"
    FEATURE_INPUT_MISSING = "FEATURE_INPUT_MISSING"
    ZERO_DENOMINATOR = "ZERO_DENOMINATOR"
    INSUFFICIENT_HISTORY = "INSUFFICIENT_HISTORY"
    LEAD_LAG_INSUFFICIENT = "LEAD_LAG_INSUFFICIENT"


class AlignmentStatus(StrEnum):
    ALIGNED = "ALIGNED"
    DEGRADED = "DEGRADED"
    STALE_BINANCE = "STALE_BINANCE"
    STALE_OKX = "STALE_OKX"
    UNAVAILABLE = "UNAVAILABLE"


class DirectionState(StrEnum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    FLAT = "FLAT"
    UNAVAILABLE = "UNAVAILABLE"


class DirectionAgreement(StrEnum):
    BOTH_POSITIVE = "BOTH_POSITIVE"
    BOTH_NEGATIVE = "BOTH_NEGATIVE"
    BOTH_FLAT = "BOTH_FLAT"
    DIVERGENT = "DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"


class StrengthAgreement(StrEnum):
    CONSISTENT = "CONSISTENT"
    DIVERGENT = "DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"


class ContextAgreement(StrEnum):
    MATCHED = "MATCHED"
    CONFLICT = "CONFLICT"
    UNAVAILABLE = "UNAVAILABLE"


class CrowdingAgreement(StrEnum):
    BOTH_LONG = "BOTH_LONG"
    BOTH_SHORT = "BOTH_SHORT"
    BOTH_MIXED = "BOTH_MIXED"
    ONE_SIDED = "ONE_SIDED"
    DIVERGENT = "DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"


class ActivityAgreement(StrEnum):
    BOTH_ELEVATED = "BOTH_ELEVATED"
    BOTH_NORMAL = "BOTH_NORMAL"
    ONE_SIDED = "ONE_SIDED"
    DIVERGENT = "DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"


class LiquidityDivergenceStatus(StrEnum):
    CONFIRMED_ADDITION = "CONFIRMED_ADDITION"
    CONFIRMED_REMOVAL = "CONFIRMED_REMOVAL"
    NEUTRAL = "NEUTRAL"
    DIVERGENT = "DIVERGENT"
    UNAVAILABLE = "UNAVAILABLE"


class LeadLagStatus(StrEnum):
    BINANCE_LEADS = "BINANCE_LEADS"
    OKX_LEADS = "OKX_LEADS"
    SIMULTANEOUS = "SIMULTANEOUS"
    UNAVAILABLE = "UNAVAILABLE"


class PriceOpenInterestState(StrEnum):
    PRICE_UP_OI_UP = "PRICE_UP_OI_UP"
    PRICE_UP_OI_DOWN = "PRICE_UP_OI_DOWN"
    PRICE_DOWN_OI_UP = "PRICE_DOWN_OI_UP"
    PRICE_DOWN_OI_DOWN = "PRICE_DOWN_OI_DOWN"
    FLAT = "FLAT"


class CrowdingState(StrEnum):
    CROWDED_LONG = "CROWDED_LONG"
    CROWDED_SHORT = "CROWDED_SHORT"
    MIXED = "MIXED"


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
    taker_imbalance_zscore: float | None = None
    trade_notional_impulse: float | None = None
    trade_count_impulse: float | None = None
    average_trade_notional: Decimal | None = None
    large_trade_share: float | None = None

    @field_validator(
        "taker_imbalance",
        "taker_imbalance_zscore",
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
    bid_liquidity_change: Decimal | None = None
    ask_liquidity_change: Decimal | None = None
    added_liquidity_quantity: Decimal | None = None
    removed_liquidity_quantity: Decimal | None = None
    liquidity_recovery_quantity_per_second: float | None = None
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
        "liquidity_recovery_quantity_per_second",
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
    price_oi_state: PriceOpenInterestState | None = None

    @field_validator("percentage_change", "zscore", "data_age_ms")
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class CrowdingFeatureValues(FrozenModel):
    funding_rate: Decimal | None = None
    funding_zscore: float | None = None
    mark_index_premium: float | None = None
    premium_zscore: float | None = None
    taker_bias: float | None = None
    joint_state: CrowdingState | None = None

    @field_validator(
        "funding_zscore",
        "mark_index_premium",
        "premium_zscore",
        "taker_bias",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class LiquidationFeatureValues(FrozenModel):
    public_sample_long_notional: Decimal | None = None
    public_sample_short_notional: Decimal | None = None
    public_sample_activity_zscore: float | None = None
    activity_with_oi_decline: bool | None = None

    @field_validator("public_sample_activity_zscore")
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class CrossVenueAlignmentResult(FrozenModel):
    decision_timestamp: datetime
    binance_snapshot_id: UUID | None = None
    okx_snapshot_id: UUID | None = None
    binance_source_timestamp: datetime | None = None
    okx_source_timestamp: datetime | None = None
    binance_data_age_ms: float | None = Field(default=None, ge=0)
    okx_data_age_ms: float | None = Field(default=None, ge=0)
    snapshot_time_difference_ms: float | None = Field(default=None, ge=0)
    status: AlignmentStatus
    quality: float = Field(ge=0, le=1)
    unavailable_reasons: tuple[FeatureUnavailableReason, ...] = ()

    @field_validator(
        "decision_timestamp",
        "binance_source_timestamp",
        "okx_source_timestamp",
    )
    @classmethod
    def alignment_timestamp_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("alignment timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator(
        "binance_data_age_ms",
        "okx_data_age_ms",
        "snapshot_time_difference_ms",
        "quality",
    )
    @classmethod
    def finite_alignment_float(cls, value: float | None) -> float | None:
        return _finite(value)

    @model_validator(mode="after")
    def sources_cannot_be_in_the_future(self) -> CrossVenueAlignmentResult:
        for source in (
            self.binance_source_timestamp,
            self.okx_source_timestamp,
        ):
            if source is not None and source > self.decision_timestamp:
                raise ValueError("alignment contains a future source timestamp")
        return self


class CrossVenuePriceFeatureValues(FrozenModel):
    binance_mid_price: Decimal | None = None
    okx_mid_price: Decimal | None = None
    mid_price_difference: Decimal | None = None
    mid_price_absolute_spread: Decimal | None = None
    percentage_spread_denominator: Decimal | None = None
    mid_price_percentage_spread: float | None = None
    mid_price_spread_zscore: float | None = None
    return_direction_agreement: DirectionAgreement = DirectionAgreement.UNAVAILABLE
    price_impulse_direction_agreement: DirectionAgreement = (
        DirectionAgreement.UNAVAILABLE
    )
    price_impulse_strength_difference: float | None = None
    realized_volatility_difference: float | None = None
    relative_spread_divergence: float | None = None

    @field_validator(
        "mid_price_percentage_spread",
        "mid_price_spread_zscore",
        "price_impulse_strength_difference",
        "realized_volatility_difference",
        "relative_spread_divergence",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class CrossVenueOrderFlowFeatureValues(FrozenModel):
    taker_flow_direction_agreement: DirectionAgreement = DirectionAgreement.UNAVAILABLE
    taker_imbalance_difference: float | None = None
    taker_imbalance_strength_agreement: StrengthAgreement = (
        StrengthAgreement.UNAVAILABLE
    )
    ofi_direction_agreement: DirectionAgreement = DirectionAgreement.UNAVAILABLE
    ofi_difference: float | None = None
    depth_imbalance_difference: float | None = None
    liquidity_addition_difference: Decimal | None = None
    liquidity_removal_difference: Decimal | None = None
    order_book_recovery_speed_difference: float | None = None
    liquidity_divergence_status: LiquidityDivergenceStatus = (
        LiquidityDivergenceStatus.UNAVAILABLE
    )

    @field_validator(
        "taker_imbalance_difference",
        "ofi_difference",
        "depth_imbalance_difference",
        "order_book_recovery_speed_difference",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class CrossVenuePositioningFeatureValues(FrozenModel):
    price_oi_state_agreement: ContextAgreement = ContextAgreement.UNAVAILABLE
    binance_oi_change_direction: DirectionState = DirectionState.UNAVAILABLE
    okx_oi_change_direction: DirectionState = DirectionState.UNAVAILABLE
    oi_context_conflict: bool | None = None
    funding_direction_agreement: DirectionAgreement = DirectionAgreement.UNAVAILABLE
    binance_funding_abnormality: float | None = None
    okx_funding_abnormality: float | None = None
    funding_abnormality_difference: float | None = None
    mark_index_premium_difference: float | None = None
    crowding_direction_agreement: CrowdingAgreement = CrowdingAgreement.UNAVAILABLE
    one_sided_crowding_unconfirmed: bool | None = None
    liquidation_activity_agreement: ActivityAgreement = ActivityAgreement.UNAVAILABLE

    @field_validator(
        "binance_funding_abnormality",
        "okx_funding_abnormality",
        "funding_abnormality_difference",
        "mark_index_premium_difference",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class CrossVenueConfirmationFeatureValues(FrozenModel):
    price_direction_agreement: DirectionAgreement
    price_impulse_agreement: DirectionAgreement
    taker_flow_agreement: DirectionAgreement
    ofi_agreement: DirectionAgreement
    oi_context_conflict: bool | None = None
    crowding_agreement: CrowdingAgreement
    liquidation_activity_agreement: ActivityAgreement
    cross_venue_confirmation: float | None = Field(default=None, ge=-1, le=1)
    divergence_penalty_input: float | None = Field(default=None, ge=0, le=1)
    alignment_quality: float = Field(ge=0, le=1)
    research_only: Literal[True] = True

    @field_validator(
        "cross_venue_confirmation",
        "divergence_penalty_input",
        "alignment_quality",
    )
    @classmethod
    def finite_float(cls, value: float | None) -> float | None:
        return _finite(value)


class LeadLagResearchFeatureValues(FrozenModel):
    price_impulse_status: LeadLagStatus = LeadLagStatus.UNAVAILABLE
    taker_flow_status: LeadLagStatus = LeadLagStatus.UNAVAILABLE
    ofi_status: LeadLagStatus = LeadLagStatus.UNAVAILABLE
    alignment_quality: float = Field(ge=0, le=1)
    research_only: Literal[True] = True
    unavailable_reasons: tuple[FeatureUnavailableReason, ...] = ()

    @field_validator("alignment_quality")
    @classmethod
    def finite_float(cls, value: float) -> float:
        result = _finite(value)
        assert result is not None
        return float(result)


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


class CrossVenueFeatureSnapshot(EventBase):
    """Deterministic research snapshot joining Binance and OKX observations."""

    exchange: Literal[Exchange.CROSS_VENUE] = Exchange.CROSS_VENUE
    event_type: Literal[EventType.MARKET_FEATURE] = EventType.MARKET_FEATURE
    feature_snapshot_id: UUID
    schema_version: Literal[1] = 1
    strategy_version: str
    code_version: str
    config_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    calculation_timestamp: datetime
    decision_timestamp: datetime
    window_seconds: int = Field(gt=0)
    binance_book_generation: int | None = Field(default=None, ge=0)
    okx_book_generation: int | None = Field(default=None, ge=0)
    source_snapshot_ids: tuple[UUID, ...] = ()
    source_event_count: int = Field(ge=0)
    oldest_source_timestamp: datetime | None = None
    newest_source_timestamp: datetime | None = None
    data_age_ms: float = Field(ge=0)
    is_warm: bool
    is_healthy: bool
    alignment: CrossVenueAlignmentResult
    unavailable_reasons: tuple[FeatureUnavailableReason, ...] = ()
    price: CrossVenuePriceFeatureValues | None = None
    order_flow: CrossVenueOrderFlowFeatureValues | None = None
    positioning: CrossVenuePositioningFeatureValues | None = None
    confirmation: CrossVenueConfirmationFeatureValues | None = None
    lead_lag: LeadLagResearchFeatureValues

    @field_validator(
        "calculation_timestamp",
        "decision_timestamp",
        "oldest_source_timestamp",
        "newest_source_timestamp",
    )
    @classmethod
    def cross_venue_timestamp_is_aware(
        cls,
        value: datetime | None,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("cross-venue timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("data_age_ms")
    @classmethod
    def finite_data_age(cls, value: float) -> float:
        result = _finite(value)
        assert result is not None
        return float(result)

    @model_validator(mode="after")
    def validate_cross_venue_boundaries(self) -> CrossVenueFeatureSnapshot:
        if self.calculation_timestamp < self.decision_timestamp:
            raise ValueError("calculation_timestamp cannot precede decision_timestamp")
        if len(self.source_snapshot_ids) > 2 or len(set(self.source_snapshot_ids)) != len(
            self.source_snapshot_ids
        ):
            raise ValueError("cross-venue source snapshot IDs must be unique and bounded")
        expected_ids = tuple(
            value
            for value in (
                self.alignment.binance_snapshot_id,
                self.alignment.okx_snapshot_id,
            )
            if value is not None
        )
        if self.source_snapshot_ids != expected_ids:
            raise ValueError("source_snapshot_ids must match the alignment result")
        if self.source_event_count == 0:
            if self.oldest_source_timestamp is not None or self.newest_source_timestamp is not None:
                raise ValueError("empty cross-venue snapshots cannot claim source bounds")
        elif self.oldest_source_timestamp is None or self.newest_source_timestamp is None:
            raise ValueError("nonempty cross-venue snapshots require source bounds")
        if (
            self.oldest_source_timestamp is not None
            and self.newest_source_timestamp is not None
        ):
            if self.oldest_source_timestamp > self.newest_source_timestamp:
                raise ValueError("cross-venue source timestamp bounds are reversed")
            if self.newest_source_timestamp > self.decision_timestamp:
                raise ValueError("cross-venue snapshot contains a future source event")
        if self.alignment.decision_timestamp != self.decision_timestamp:
            raise ValueError("alignment and snapshot decision timestamps must match")
        if self.alignment.status is AlignmentStatus.UNAVAILABLE and any(
            group is not None
            for group in (
                self.price,
                self.order_flow,
                self.positioning,
                self.confirmation,
            )
        ):
            raise ValueError("unavailable alignments cannot expose paired feature groups")
        if self.is_healthy and self.alignment.status is not AlignmentStatus.ALIGNED:
            raise ValueError("only aligned snapshots can be healthy")
        return self

"""Phase 3C cross-venue formulas, boundaries, and determinism."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import fmean, pstdev
from typing import Any
from uuid import UUID

import pytest

from cvf.config import Settings, load_settings
from cvf.features import (
    ActivityAgreement,
    AlignmentStatus,
    ContextAgreement,
    CrossVenueFeatureEngine,
    CrossVenueFeatureSnapshot,
    CrowdingAgreement,
    CrowdingState,
    DirectionAgreement,
    DirectionState,
    FeatureUnavailableCode,
    LeadLagStatus,
    LiquidityDivergenceStatus,
    PriceOpenInterestState,
    StrengthAgreement,
)
from cvf.features.models import (
    CrowdingFeatureValues,
    FeatureSnapshot,
    FeatureUnavailableReason,
    LiquidationFeatureValues,
    OpenInterestFeatureValues,
    OrderBookFeatureValues,
    PriceFeatureValues,
    TradeFlowFeatureValues,
)
from cvf.models.enums import Exchange

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
SYMBOL = "BTC-USDT-PERP"


def settings(**feature_updates: object) -> Settings:
    base = load_settings(environ={})
    updates = {
        "cross_venue_max_snapshot_age_ms": 2_000,
        "cross_venue_max_time_difference_ms": 500,
        "cross_venue_zscore_minimum_samples": 2,
        **feature_updates,
    }
    return base.model_copy(
        update={"features": base.features.model_copy(update=updates)}
    )


def snapshot(
    exchange: Exchange,
    *,
    at: datetime = NOW,
    source_at: datetime | None = None,
    identifier: int | None = None,
    symbol: str = SYMBOL,
    window_seconds: int = 5,
    mid: str | None = "100",
    price_return: float | None = 0.01,
    impulse: float | None = 1.0,
    volatility: float | None = 0.2,
    relative_spread: float | None = 0.001,
    taker: float | None = 0.4,
    ofi: float | None = 0.3,
    depth: float | None = 0.2,
    added: str | None = "10",
    removed: str | None = "4",
    recovery: float | None = 3.0,
    oi_change: float | None = 0.02,
    oi_absolute_change: str | None = "10",
    price_oi_state: PriceOpenInterestState | None = (
        PriceOpenInterestState.PRICE_UP_OI_UP
    ),
    funding: str | None = "0.001",
    funding_zscore: float | None = 1.5,
    premium: float | None = 0.002,
    crowding: CrowdingState | None = CrowdingState.CROWDED_LONG,
    liquidation_zscore: float | None = 1.0,
    warm: bool = True,
    healthy: bool = True,
    book_generation: int = 1,
) -> FeatureSnapshot:
    source = at if source_at is None else source_at
    unavailable_reasons: tuple[FeatureUnavailableReason, ...] = ()
    if not warm or not healthy:
        unavailable_reasons = (
            FeatureUnavailableReason(
                code=(
                    FeatureUnavailableCode.NOT_WARM
                    if not warm
                    else FeatureUnavailableCode.HEALTH_BLOCKED
                ),
                detail="test fixture availability state",
            ),
        )
    if identifier is None:
        identifier = (
            int(at.timestamp()) * 10
            + (1 if exchange is Exchange.BINANCE else 2)
            + window_seconds
        )
    order_book = (
        None
        if mid is None
        else OrderBookFeatureValues(
            mid_price=Decimal(mid),
            relative_spread=relative_spread,
            order_flow_imbalance=ofi,
            depth_imbalance=depth,
            added_liquidity_quantity=(
                None if added is None else Decimal(added)
            ),
            removed_liquidity_quantity=(
                None if removed is None else Decimal(removed)
            ),
            liquidity_recovery_quantity_per_second=recovery,
        )
    )
    return FeatureSnapshot(
        exchange=exchange,
        symbol=symbol,
        exchange_timestamp=at,
        local_receive_timestamp=at,
        normalization_timestamp=at,
        sequence_id=identifier,
        feature_snapshot_id=UUID(int=identifier),
        strategy_version="0.2.1",
        calculation_timestamp=at,
        decision_timestamp=at,
        window_seconds=window_seconds,
        book_generation=book_generation,
        source_sequence_id=identifier,
        source_event_count=1,
        oldest_source_timestamp=source,
        newest_source_timestamp=source,
        data_age_ms=max(0.0, (at - source).total_seconds() * 1000),
        is_warm=warm,
        is_healthy=healthy,
        unavailable_reasons=unavailable_reasons,
        trade_flow=TradeFlowFeatureValues(taker_imbalance=taker),
        order_book=order_book,
        price=PriceFeatureValues(
            return_value=price_return,
            impulse_zscore=impulse,
            realized_volatility=volatility,
        ),
        open_interest=OpenInterestFeatureValues(
            change=(
                None
                if oi_absolute_change is None
                else Decimal(oi_absolute_change)
            ),
            percentage_change=oi_change,
            price_oi_state=price_oi_state,
        ),
        crowding=CrowdingFeatureValues(
            funding_rate=None if funding is None else Decimal(funding),
            funding_zscore=funding_zscore,
            mark_index_premium=premium,
            joint_state=crowding,
        ),
        liquidation=LiquidationFeatureValues(
            public_sample_activity_zscore=liquidation_zscore
        ),
    )


def pair(
    *,
    binance: dict[str, Any] | None = None,
    okx: dict[str, Any] | None = None,
) -> list[FeatureSnapshot]:
    return [
        snapshot(Exchange.BINANCE, **(binance or {})),
        snapshot(Exchange.OKX, **(okx or {})),
    ]


def calculate(
    values: list[FeatureSnapshot],
    *,
    config: Settings | None = None,
    symbol: str = SYMBOL,
    decision: datetime = NOW,
    window_seconds: int = 5,
) -> CrossVenueFeatureSnapshot:
    return CrossVenueFeatureEngine(config or settings()).calculate(
        values,
        symbol=symbol,
        decision_timestamp=decision,
        window_seconds=window_seconds,
    )


def test_same_price_and_flow_directions_are_typed_confirmations() -> None:
    result = calculate(pair())

    assert result.price is not None
    assert result.order_flow is not None
    assert (
        result.price.return_direction_agreement
        is DirectionAgreement.BOTH_POSITIVE
    )
    assert (
        result.order_flow.taker_flow_direction_agreement
        is DirectionAgreement.BOTH_POSITIVE
    )
    assert result.order_flow.ofi_direction_agreement is DirectionAgreement.BOTH_POSITIVE


def test_opposing_taker_flow_is_divergent() -> None:
    result = calculate(pair(okx={"taker": -0.2}))

    assert result.order_flow is not None
    assert (
        result.order_flow.taker_flow_direction_agreement
        is DirectionAgreement.DIVERGENT
    )
    assert result.order_flow.taker_imbalance_difference == pytest.approx(0.6)


def test_opposing_ofi_is_divergent() -> None:
    result = calculate(pair(okx={"ofi": -0.1}))

    assert result.order_flow is not None
    assert result.order_flow.ofi_direction_agreement is DirectionAgreement.DIVERGENT
    assert result.order_flow.ofi_difference == pytest.approx(0.4)


def test_missing_binance_snapshot_is_structurally_unavailable() -> None:
    result = calculate([snapshot(Exchange.OKX)])

    assert result.alignment.status is AlignmentStatus.UNAVAILABLE
    assert result.price is None
    assert result.alignment.binance_data_age_ms is None
    assert FeatureUnavailableCode.MISSING_BINANCE_SNAPSHOT in {
        reason.code for reason in result.unavailable_reasons
    }


def test_missing_okx_snapshot_is_structurally_unavailable() -> None:
    result = calculate([snapshot(Exchange.BINANCE)])

    assert result.alignment.status is AlignmentStatus.UNAVAILABLE
    assert result.order_flow is None
    assert FeatureUnavailableCode.MISSING_OKX_SNAPSHOT in {
        reason.code for reason in result.unavailable_reasons
    }


def test_stale_binance_snapshot_has_explicit_status() -> None:
    values = pair(
        binance={"source_at": NOW - timedelta(milliseconds=2_001)},
        okx={"source_at": NOW},
    )
    result = calculate(values)

    assert result.alignment.status is AlignmentStatus.STALE_BINANCE
    assert FeatureUnavailableCode.STALE_BINANCE_SNAPSHOT in {
        reason.code for reason in result.unavailable_reasons
    }


def test_stale_okx_snapshot_has_explicit_status() -> None:
    values = pair(
        binance={"source_at": NOW},
        okx={"source_at": NOW - timedelta(milliseconds=2_001)},
    )
    result = calculate(values)

    assert result.alignment.status is AlignmentStatus.STALE_OKX
    assert FeatureUnavailableCode.STALE_OKX_SNAPSHOT in {
        reason.code for reason in result.unavailable_reasons
    }


def test_age_and_time_difference_equal_to_threshold_are_accepted() -> None:
    values = pair(
        binance={"source_at": NOW - timedelta(milliseconds=2_000)},
        okx={"source_at": NOW - timedelta(milliseconds=1_500)},
    )
    result = calculate(values)

    assert result.alignment.status is AlignmentStatus.ALIGNED
    assert result.alignment.binance_data_age_ms == pytest.approx(2_000)
    assert result.alignment.snapshot_time_difference_ms == pytest.approx(500)


def test_time_difference_over_threshold_is_degraded() -> None:
    values = pair(
        binance={"source_at": NOW},
        okx={"source_at": NOW - timedelta(milliseconds=501)},
    )
    result = calculate(values)

    assert result.alignment.status is AlignmentStatus.DEGRADED
    assert FeatureUnavailableCode.TIME_ALIGNMENT in {
        reason.code for reason in result.unavailable_reasons
    }


def test_future_snapshot_is_never_selected() -> None:
    future = snapshot(
        Exchange.BINANCE,
        at=NOW + timedelta(seconds=1),
        source_at=NOW + timedelta(seconds=1),
    )
    result = calculate([future, snapshot(Exchange.OKX)])

    assert result.alignment.status is AlignmentStatus.UNAVAILABLE
    assert result.alignment.binance_snapshot_id is None
    assert FeatureUnavailableCode.FUTURE_BINANCE_SNAPSHOT in {
        reason.code for reason in result.unavailable_reasons
    }


def test_generation_warmup_degrades_without_hiding_features() -> None:
    result = calculate(pair(binance={"warm": False, "book_generation": 2}))

    assert result.alignment.status is AlignmentStatus.DEGRADED
    assert result.price is not None
    assert result.is_warm is False
    assert FeatureUnavailableCode.BINANCE_NOT_WARM in {
        reason.code for reason in result.unavailable_reasons
    }


def test_unhealthy_venue_degrades_alignment() -> None:
    result = calculate(pair(okx={"healthy": False}))

    assert result.alignment.status is AlignmentStatus.DEGRADED
    assert result.is_healthy is False
    assert FeatureUnavailableCode.OKX_UNHEALTHY in {
        reason.code for reason in result.unavailable_reasons
    }


def test_insufficient_zscore_history_is_not_zero_masked() -> None:
    result = calculate(pair())

    assert result.price is not None
    assert result.price.mid_price_spread_zscore is None
    assert result.is_warm is False
    assert FeatureUnavailableCode.INSUFFICIENT_HISTORY in {
        reason.code for reason in result.unavailable_reasons
    }


def test_cross_venue_spread_zscore_matches_hand_calculation() -> None:
    first_at = NOW - timedelta(seconds=2)
    second_at = NOW - timedelta(seconds=1)
    values = [
        snapshot(Exchange.BINANCE, at=first_at, mid="100"),
        snapshot(Exchange.OKX, at=first_at, mid="100"),
        snapshot(Exchange.BINANCE, at=second_at, mid="102"),
        snapshot(Exchange.OKX, at=second_at, mid="100"),
        snapshot(Exchange.BINANCE, mid="104"),
        snapshot(Exchange.OKX, mid="100"),
    ]
    result = calculate(values)
    history = [0.0, 2 / 101]
    current = 4 / 102
    expected = (current - fmean(history)) / pstdev(history)

    assert result.price is not None
    assert result.price.mid_price_spread_zscore == pytest.approx(expected)
    assert result.is_warm is True


def test_zero_midpoint_denominator_is_explicitly_unavailable() -> None:
    result = calculate(pair(binance={"mid": "0"}, okx={"mid": "0"}))

    assert result.price is not None
    assert result.price.percentage_spread_denominator == 0
    assert result.price.mid_price_percentage_spread is None
    assert FeatureUnavailableCode.ZERO_DENOMINATOR in {
        reason.code for reason in result.unavailable_reasons
    }


def test_repeat_calculation_has_identical_id_and_payload() -> None:
    values = pair()
    first = calculate(values)
    second = calculate(values)

    assert first.feature_snapshot_id == second.feature_snapshot_id
    assert first.model_dump(mode="json") == second.model_dump(mode="json")


def test_input_event_order_does_not_change_output() -> None:
    values = pair()
    forward = calculate(values)
    reverse = calculate(list(reversed(values)))

    assert forward.model_dump(mode="json") == reverse.model_dump(mode="json")


def test_independent_live_and_replay_engines_are_consistent() -> None:
    values = pair()
    live = CrossVenueFeatureEngine(settings()).calculate(
        values,
        symbol=SYMBOL,
        decision_timestamp=NOW,
        window_seconds=5,
    )
    replay = CrossVenueFeatureEngine(settings()).calculate(
        tuple(reversed(values)),
        symbol=SYMBOL,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert live.model_dump(mode="json") == replay.model_dump(mode="json")


def test_symbol_history_is_isolated() -> None:
    other = "ETH-USDT-PERP"
    historical = [
        snapshot(Exchange.BINANCE, at=NOW - timedelta(seconds=2), symbol=other),
        snapshot(Exchange.OKX, at=NOW - timedelta(seconds=2), symbol=other),
        snapshot(Exchange.BINANCE, at=NOW - timedelta(seconds=1), symbol=other),
        snapshot(Exchange.OKX, at=NOW - timedelta(seconds=1), symbol=other),
    ]
    result = calculate([*historical, *pair()])

    assert result.price is not None
    assert result.price.mid_price_spread_zscore is None


def test_window_history_is_isolated() -> None:
    historical = [
        snapshot(Exchange.BINANCE, at=NOW - timedelta(seconds=2), window_seconds=15),
        snapshot(Exchange.OKX, at=NOW - timedelta(seconds=2), window_seconds=15),
        snapshot(Exchange.BINANCE, at=NOW - timedelta(seconds=1), window_seconds=15),
        snapshot(Exchange.OKX, at=NOW - timedelta(seconds=1), window_seconds=15),
    ]
    result = calculate([*historical, *pair()])

    assert result.price is not None
    assert result.price.mid_price_spread_zscore is None


def test_totally_unavailable_snapshot_does_not_report_zero_age() -> None:
    result = calculate([])

    assert result.data_age_ms is None
    assert result.alignment.binance_data_age_ms is None
    assert result.alignment.okx_data_age_ms is None


def test_mid_price_spread_formula_uses_symmetric_denominator() -> None:
    result = calculate(pair(binance={"mid": "102"}, okx={"mid": "98"}))

    assert result.price is not None
    assert result.price.mid_price_difference == Decimal("4")
    assert result.price.mid_price_absolute_spread == Decimal("4")
    assert result.price.percentage_spread_denominator == Decimal("100")
    assert result.price.mid_price_percentage_spread == pytest.approx(0.04)


def test_order_flow_and_liquidity_formulas_are_hand_verifiable() -> None:
    values = pair(
        binance={
            "taker": 0.5,
            "ofi": 0.4,
            "depth": 0.25,
            "added": "12",
            "removed": "3",
            "recovery": 4.5,
        },
        okx={
            "taker": 0.2,
            "ofi": 0.1,
            "depth": -0.05,
            "added": "8",
            "removed": "2",
            "recovery": 1.5,
        },
    )
    result = calculate(values)

    assert result.order_flow is not None
    assert result.order_flow.taker_imbalance_difference == pytest.approx(0.3)
    assert (
        result.order_flow.taker_imbalance_strength_agreement
        is StrengthAgreement.DIVERGENT
    )
    assert result.order_flow.ofi_difference == pytest.approx(0.3)
    assert result.order_flow.depth_imbalance_difference == pytest.approx(0.3)
    assert result.order_flow.liquidity_addition_difference == Decimal("4")
    assert result.order_flow.liquidity_removal_difference == Decimal("1")
    assert result.order_flow.order_book_recovery_speed_difference == pytest.approx(3)
    assert (
        result.order_flow.liquidity_divergence_status
        is LiquidityDivergenceStatus.CONFIRMED_ADDITION
    )


def test_open_interest_compares_percentage_direction_not_absolute_size() -> None:
    values = pair(
        binance={"oi_change": 0.01, "oi_absolute_change": "1000000"},
        okx={"oi_change": -0.02, "oi_absolute_change": "-1"},
    )
    result = calculate(values)

    assert result.positioning is not None
    assert (
        result.positioning.binance_oi_change_direction
        is DirectionState.POSITIVE
    )
    assert result.positioning.okx_oi_change_direction is DirectionState.NEGATIVE


def test_price_oi_state_conflict_is_typed() -> None:
    result = calculate(
        pair(
            okx={
                "price_oi_state": PriceOpenInterestState.PRICE_UP_OI_DOWN,
            }
        )
    )

    assert result.positioning is not None
    assert (
        result.positioning.price_oi_state_agreement
        is ContextAgreement.CONFLICT
    )
    assert result.positioning.oi_context_conflict is True


def test_one_sided_crowding_is_unconfirmed() -> None:
    result = calculate(pair(okx={"crowding": CrowdingState.MIXED}))

    assert result.positioning is not None
    assert (
        result.positioning.crowding_direction_agreement
        is CrowdingAgreement.ONE_SIDED
    )
    assert result.positioning.one_sided_crowding_unconfirmed is True


def test_liquidation_activity_can_confirm_both_venues() -> None:
    result = calculate(
        pair(
            binance={"liquidation_zscore": 5.0},
            okx={"liquidation_zscore": 4.5},
        )
    )

    assert result.positioning is not None
    assert (
        result.positioning.liquidation_activity_agreement
        is ActivityAgreement.BOTH_ELEVATED
    )


def test_both_stale_sources_make_pair_unavailable() -> None:
    stale_at = NOW - timedelta(milliseconds=2_001)
    result = calculate(
        pair(
            binance={"source_at": stale_at},
            okx={"source_at": stale_at},
        )
    )

    assert result.alignment.status is AlignmentStatus.UNAVAILABLE
    assert result.price is None
    assert result.confirmation is None


def test_lead_lag_never_uses_local_arrival_order() -> None:
    result = calculate(pair())

    assert result.lead_lag.price_impulse_status is LeadLagStatus.UNAVAILABLE
    assert result.lead_lag.taker_flow_status is LeadLagStatus.UNAVAILABLE
    assert result.lead_lag.ofi_status is LeadLagStatus.UNAVAILABLE
    assert result.lead_lag.research_only is True
    assert result.lead_lag.unavailable_reasons[0].code is (
        FeatureUnavailableCode.LEAD_LAG_INSUFFICIENT
    )


def test_confirmation_is_research_only_and_not_a_signal() -> None:
    result = calculate(pair())

    assert result.confirmation is not None
    assert result.confirmation.research_only is True
    assert "signal" not in result.model_dump(mode="json")


def test_config_hash_changes_when_cross_venue_threshold_changes() -> None:
    first = calculate(pair(), config=settings(cross_venue_max_snapshot_age_ms=2_000))
    second = calculate(pair(), config=settings(cross_venue_max_snapshot_age_ms=2_001))

    assert first.config_hash != second.config_hash
    assert first.feature_snapshot_id != second.feature_snapshot_id

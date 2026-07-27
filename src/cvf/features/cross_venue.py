"""Deterministic, no-lookahead Binance/OKX research features."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from statistics import fmean, pstdev
from uuid import NAMESPACE_URL, UUID, uuid5

from cvf import __version__
from cvf.config import Settings
from cvf.features.models import (
    ActivityAgreement,
    AlignmentStatus,
    ContextAgreement,
    CrossVenueAlignmentResult,
    CrossVenueConfirmationFeatureValues,
    CrossVenueFeatureSnapshot,
    CrossVenueOrderFlowFeatureValues,
    CrossVenuePositioningFeatureValues,
    CrossVenuePriceFeatureValues,
    CrowdingAgreement,
    CrowdingState,
    DirectionAgreement,
    DirectionState,
    FeatureSnapshot,
    FeatureUnavailableCode,
    FeatureUnavailableReason,
    LeadLagResearchFeatureValues,
    LiquidityDivergenceStatus,
    StrengthAgreement,
)
from cvf.models.enums import Exchange
from cvf.utils.fingerprint import settings_fingerprint


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("cross-venue decision timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _direction(value: float | Decimal | None, *, epsilon: float) -> DirectionState:
    if value is None:
        return DirectionState.UNAVAILABLE
    numeric = float(value)
    if numeric > epsilon:
        return DirectionState.POSITIVE
    if numeric < -epsilon:
        return DirectionState.NEGATIVE
    return DirectionState.FLAT


def _direction_agreement(
    left: float | Decimal | None,
    right: float | Decimal | None,
    *,
    epsilon: float,
) -> DirectionAgreement:
    left_direction = _direction(left, epsilon=epsilon)
    right_direction = _direction(right, epsilon=epsilon)
    if DirectionState.UNAVAILABLE in (left_direction, right_direction):
        return DirectionAgreement.UNAVAILABLE
    if left_direction is not right_direction:
        return DirectionAgreement.DIVERGENT
    return {
        DirectionState.POSITIVE: DirectionAgreement.BOTH_POSITIVE,
        DirectionState.NEGATIVE: DirectionAgreement.BOTH_NEGATIVE,
        DirectionState.FLAT: DirectionAgreement.BOTH_FLAT,
    }[left_direction]


def _difference(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return left - right


def _decimal_difference(
    left: Decimal | None,
    right: Decimal | None,
) -> Decimal | None:
    if left is None or right is None:
        return None
    return left - right


def _agreement_value(value: DirectionAgreement) -> float | None:
    if value is DirectionAgreement.UNAVAILABLE:
        return None
    if value is DirectionAgreement.DIVERGENT:
        return -1.0
    if value is DirectionAgreement.BOTH_FLAT:
        return 0.0
    return 1.0


def _context_value(value: ContextAgreement) -> float | None:
    if value is ContextAgreement.UNAVAILABLE:
        return None
    return 1.0 if value is ContextAgreement.MATCHED else -1.0


def _crowding_value(value: CrowdingAgreement) -> float | None:
    if value is CrowdingAgreement.UNAVAILABLE:
        return None
    if value is CrowdingAgreement.DIVERGENT:
        return -1.0
    if value is CrowdingAgreement.ONE_SIDED:
        return 0.0
    return 1.0


def _activity_value(value: ActivityAgreement) -> float | None:
    if value is ActivityAgreement.UNAVAILABLE:
        return None
    if value is ActivityAgreement.DIVERGENT:
        return -1.0
    if value is ActivityAgreement.ONE_SIDED:
        return 0.0
    return 1.0


def _reason(
    code: FeatureUnavailableCode,
    detail: str,
    *,
    channel: str | None = None,
) -> FeatureUnavailableReason:
    return FeatureUnavailableReason(code=code, detail=detail, channel=channel)


def _unique_reasons(
    reasons: Iterable[FeatureUnavailableReason],
) -> tuple[FeatureUnavailableReason, ...]:
    return tuple(dict.fromkeys(reasons))


class CrossVenueFeatureEngine:
    """Join typed venue snapshots without creating execution or signal semantics."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.config_hash = settings_fingerprint(settings)

    def calculate(
        self,
        snapshots: Iterable[FeatureSnapshot],
        *,
        symbol: str,
        decision_timestamp: datetime,
        window_seconds: int,
    ) -> CrossVenueFeatureSnapshot:
        decision = _utc(decision_timestamp)
        if window_seconds not in self.settings.timing.feature_windows_seconds:
            raise ValueError("window_seconds is not a configured feature window")

        candidates = tuple(
            sorted(
                (
                    snapshot
                    for snapshot in snapshots
                    if snapshot.symbol == symbol
                    and snapshot.window_seconds == window_seconds
                    and snapshot.exchange in (Exchange.BINANCE, Exchange.OKX)
                ),
                key=lambda item: (
                    item.decision_timestamp,
                    item.exchange.value,
                    item.feature_snapshot_id.hex,
                ),
            )
        )
        binance, binance_reason = self._select_snapshot(
            candidates,
            exchange=Exchange.BINANCE,
            decision=decision,
        )
        okx, okx_reason = self._select_snapshot(
            candidates,
            exchange=Exchange.OKX,
            decision=decision,
        )
        alignment = self._align(
            binance,
            okx,
            decision=decision,
            selection_reasons=(binance_reason, okx_reason),
        )
        feature_id = self._feature_id(
            symbol=symbol,
            decision=decision,
            window_seconds=window_seconds,
            binance=binance,
            okx=okx,
            status=alignment.status,
        )
        lead_lag = LeadLagResearchFeatureValues(
            alignment_quality=alignment.quality,
            unavailable_reasons=(
                _reason(
                    FeatureUnavailableCode.LEAD_LAG_INSUFFICIENT,
                    (
                        "lead/lag requires independently validated event-time history; "
                        "local arrival order is never used"
                    ),
                    channel="lead_lag",
                ),
            ),
        )
        sources = tuple(
            snapshot for snapshot in (binance, okx) if snapshot is not None
        )
        source_ids = tuple(snapshot.feature_snapshot_id for snapshot in sources)
        source_timestamps = tuple(
            timestamp
            for snapshot in sources
            for timestamp in (
                snapshot.oldest_source_timestamp,
                snapshot.newest_source_timestamp,
            )
            if timestamp is not None
        )
        data_ages = tuple(
            age
            for age in (
                alignment.binance_data_age_ms,
                alignment.okx_data_age_ms,
            )
            if age is not None
        )

        if alignment.status is AlignmentStatus.UNAVAILABLE:
            return CrossVenueFeatureSnapshot(
                symbol=symbol,
                exchange_timestamp=decision,
                local_receive_timestamp=decision,
                normalization_timestamp=decision,
                sequence_id=feature_id.hex,
                feature_snapshot_id=feature_id,
                strategy_version=self.settings.app.strategy_version,
                code_version=__version__,
                config_hash=self.config_hash,
                calculation_timestamp=decision,
                decision_timestamp=decision,
                window_seconds=window_seconds,
                binance_book_generation=(
                    None if binance is None else binance.book_generation
                ),
                okx_book_generation=None if okx is None else okx.book_generation,
                source_snapshot_ids=source_ids,
                source_event_count=sum(
                    snapshot.source_event_count for snapshot in sources
                ),
                oldest_source_timestamp=(
                    min(source_timestamps) if source_timestamps else None
                ),
                newest_source_timestamp=(
                    max(source_timestamps) if source_timestamps else None
                ),
                data_age_ms=max(data_ages) if data_ages else None,
                is_warm=False,
                is_healthy=False,
                alignment=alignment,
                unavailable_reasons=alignment.unavailable_reasons,
                lead_lag=lead_lag,
            )

        assert binance is not None
        assert okx is not None
        reasons = list(alignment.unavailable_reasons)
        price = self._price_features(
            candidates,
            binance,
            okx,
            decision=decision,
            window_seconds=window_seconds,
            reasons=reasons,
        )
        order_flow = self._order_flow_features(binance, okx, reasons=reasons)
        positioning = self._positioning_features(binance, okx, reasons=reasons)
        confirmation = self._confirmation_features(
            price,
            order_flow,
            positioning,
            alignment_quality=alignment.quality,
        )
        zscore_warm = price.mid_price_spread_zscore is not None
        if not zscore_warm:
            reasons.append(
                _reason(
                    FeatureUnavailableCode.INSUFFICIENT_HISTORY,
                    (
                        "cross-venue spread z-score has fewer than "
                        f"{self.settings.features.cross_venue_zscore_minimum_samples} "
                        "prior paired observations or zero variance"
                    ),
                    channel="price.mid_price_spread_zscore",
                )
            )
        is_warm = binance.is_warm and okx.is_warm and zscore_warm
        is_healthy = (
            alignment.status is AlignmentStatus.ALIGNED
            and binance.is_healthy
            and okx.is_healthy
        )
        return CrossVenueFeatureSnapshot(
            symbol=symbol,
            exchange_timestamp=decision,
            local_receive_timestamp=decision,
            normalization_timestamp=decision,
            sequence_id=feature_id.hex,
            feature_snapshot_id=feature_id,
            strategy_version=self.settings.app.strategy_version,
            code_version=__version__,
            config_hash=self.config_hash,
            calculation_timestamp=decision,
            decision_timestamp=decision,
            window_seconds=window_seconds,
            binance_book_generation=binance.book_generation,
            okx_book_generation=okx.book_generation,
            source_snapshot_ids=source_ids,
            source_event_count=sum(
                snapshot.source_event_count for snapshot in sources
            ),
            oldest_source_timestamp=(
                min(source_timestamps) if source_timestamps else None
            ),
            newest_source_timestamp=(
                max(source_timestamps) if source_timestamps else None
            ),
            data_age_ms=max(data_ages) if data_ages else None,
            is_warm=is_warm,
            is_healthy=is_healthy,
            alignment=alignment,
            unavailable_reasons=_unique_reasons(reasons),
            price=price,
            order_flow=order_flow,
            positioning=positioning,
            confirmation=confirmation,
            lead_lag=lead_lag,
        )

    def calculate_all(
        self,
        snapshots: Iterable[FeatureSnapshot],
        *,
        decision_timestamp: datetime,
    ) -> list[CrossVenueFeatureSnapshot]:
        materialized = tuple(snapshots)
        return [
            self.calculate(
                materialized,
                symbol=symbol,
                decision_timestamp=decision_timestamp,
                window_seconds=window_seconds,
            )
            for symbol in self.settings.markets.canonical_symbols
            for window_seconds in self.settings.timing.feature_windows_seconds
        ]

    @staticmethod
    def _select_snapshot(
        snapshots: tuple[FeatureSnapshot, ...],
        *,
        exchange: Exchange,
        decision: datetime,
    ) -> tuple[FeatureSnapshot | None, FeatureUnavailableReason | None]:
        venue = tuple(item for item in snapshots if item.exchange is exchange)
        eligible = tuple(item for item in venue if item.decision_timestamp <= decision)
        if eligible:
            return max(
                eligible,
                key=lambda item: (
                    item.decision_timestamp,
                    item.feature_snapshot_id.hex,
                ),
            ), None
        label = exchange.value.lower()
        if venue:
            code = (
                FeatureUnavailableCode.FUTURE_BINANCE_SNAPSHOT
                if exchange is Exchange.BINANCE
                else FeatureUnavailableCode.FUTURE_OKX_SNAPSHOT
            )
            return None, _reason(
                code,
                f"only future {label} snapshots exist at the decision boundary",
                channel="alignment",
            )
        code = (
            FeatureUnavailableCode.MISSING_BINANCE_SNAPSHOT
            if exchange is Exchange.BINANCE
            else FeatureUnavailableCode.MISSING_OKX_SNAPSHOT
        )
        return None, _reason(
            code,
            f"no {label} snapshot exists for symbol/window",
            channel="alignment",
        )

    def _align(
        self,
        binance: FeatureSnapshot | None,
        okx: FeatureSnapshot | None,
        *,
        decision: datetime,
        selection_reasons: tuple[
            FeatureUnavailableReason | None,
            FeatureUnavailableReason | None,
        ],
    ) -> CrossVenueAlignmentResult:
        reasons = [reason for reason in selection_reasons if reason is not None]
        binance_source = (
            None if binance is None else binance.newest_source_timestamp
        )
        okx_source = None if okx is None else okx.newest_source_timestamp
        binance_age = (
            None
            if binance_source is None
            else max(0.0, (decision - binance_source).total_seconds() * 1000.0)
        )
        okx_age = (
            None
            if okx_source is None
            else max(0.0, (decision - okx_source).total_seconds() * 1000.0)
        )
        time_difference = (
            None
            if binance_source is None or okx_source is None
            else abs((binance_source - okx_source).total_seconds() * 1000.0)
        )
        data_age_difference = (
            None
            if binance_age is None or okx_age is None
            else abs(binance_age - okx_age)
        )
        if binance is None or okx is None:
            status = AlignmentStatus.UNAVAILABLE
        else:
            if binance_source is None:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.FEATURE_INPUT_MISSING,
                        "Binance snapshot has no source timestamp",
                        channel="alignment",
                    )
                )
            if okx_source is None:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.FEATURE_INPUT_MISSING,
                        "OKX snapshot has no source timestamp",
                        channel="alignment",
                    )
                )
            binance_stale = (
                binance_age is not None
                and binance_age
                > self.settings.features.cross_venue_max_snapshot_age_ms
            )
            okx_stale = (
                okx_age is not None
                and okx_age
                > self.settings.features.cross_venue_max_snapshot_age_ms
            )
            if binance_stale:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.STALE_BINANCE_SNAPSHOT,
                        "Binance source age exceeds the configured threshold",
                        channel="alignment",
                    )
                )
            if okx_stale:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.STALE_OKX_SNAPSHOT,
                        "OKX source age exceeds the configured threshold",
                        channel="alignment",
                    )
                )
            if not binance.is_warm:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.BINANCE_NOT_WARM,
                        "Binance feature snapshot is not warm",
                        channel="alignment",
                    )
                )
            if not okx.is_warm:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.OKX_NOT_WARM,
                        "OKX feature snapshot is not warm",
                        channel="alignment",
                    )
                )
            if not binance.is_healthy:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.BINANCE_UNHEALTHY,
                        "Binance feature snapshot is unhealthy",
                        channel="alignment",
                    )
                )
            if not okx.is_healthy:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.OKX_UNHEALTHY,
                        "OKX feature snapshot is unhealthy",
                        channel="alignment",
                    )
                )
            bad_time_alignment = (
                time_difference is not None
                and time_difference
                > self.settings.features.cross_venue_max_time_difference_ms
            )
            if bad_time_alignment:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.TIME_ALIGNMENT,
                        "venue source timestamps exceed the configured separation",
                        channel="alignment",
                    )
                )
            if binance_stale and okx_stale:
                status = AlignmentStatus.UNAVAILABLE
            elif binance_stale:
                status = AlignmentStatus.STALE_BINANCE
            elif okx_stale:
                status = AlignmentStatus.STALE_OKX
            elif (
                binance_source is None
                or okx_source is None
                or bad_time_alignment
                or not binance.is_warm
                or not okx.is_warm
                or not binance.is_healthy
                or not okx.is_healthy
            ):
                status = AlignmentStatus.DEGRADED
            else:
                status = AlignmentStatus.ALIGNED

        quality = self._alignment_quality(
            binance_age,
            okx_age,
            time_difference,
        )
        return CrossVenueAlignmentResult(
            decision_timestamp=decision,
            binance_snapshot_id=(
                None if binance is None else binance.feature_snapshot_id
            ),
            okx_snapshot_id=None if okx is None else okx.feature_snapshot_id,
            binance_source_timestamp=binance_source,
            okx_source_timestamp=okx_source,
            binance_data_age_ms=binance_age,
            okx_data_age_ms=okx_age,
            data_age_difference_ms=data_age_difference,
            snapshot_time_difference_ms=time_difference,
            status=status,
            quality=quality,
            unavailable_reasons=_unique_reasons(reasons),
        )

    def _alignment_quality(
        self,
        binance_age: float | None,
        okx_age: float | None,
        time_difference: float | None,
    ) -> float:
        if binance_age is None or okx_age is None or time_difference is None:
            return 0.0
        age_limit = self.settings.features.cross_venue_max_snapshot_age_ms
        difference_limit = (
            self.settings.features.cross_venue_max_time_difference_ms
        )
        worst_ratio = max(
            binance_age / age_limit,
            okx_age / age_limit,
            time_difference / difference_limit,
        )
        return 1.0 / (1.0 + worst_ratio)

    def _price_features(
        self,
        snapshots: tuple[FeatureSnapshot, ...],
        binance: FeatureSnapshot,
        okx: FeatureSnapshot,
        *,
        decision: datetime,
        window_seconds: int,
        reasons: list[FeatureUnavailableReason],
    ) -> CrossVenuePriceFeatureValues:
        binance_mid = (
            None if binance.order_book is None else binance.order_book.mid_price
        )
        okx_mid = None if okx.order_book is None else okx.order_book.mid_price
        midpoint_difference = _decimal_difference(binance_mid, okx_mid)
        absolute_spread = (
            None if midpoint_difference is None else abs(midpoint_difference)
        )
        denominator = (
            None
            if binance_mid is None or okx_mid is None
            else (abs(binance_mid) + abs(okx_mid)) / Decimal(2)
        )
        percentage_spread = self._percentage_spread(
            binance_mid,
            okx_mid,
            reasons=reasons,
            channel="price.mid_price_percentage_spread",
        )
        spread_zscore = self._spread_zscore(
            snapshots,
            symbol=binance.symbol,
            decision=decision,
            window_seconds=window_seconds,
            value=percentage_spread,
        )
        binance_return = None if binance.price is None else binance.price.return_value
        okx_return = None if okx.price is None else okx.price.return_value
        binance_impulse = (
            None if binance.price is None else binance.price.impulse_zscore
        )
        okx_impulse = None if okx.price is None else okx.price.impulse_zscore
        binance_volatility = (
            None if binance.price is None else binance.price.realized_volatility
        )
        okx_volatility = (
            None if okx.price is None else okx.price.realized_volatility
        )
        binance_relative_spread = (
            None if binance.order_book is None else binance.order_book.relative_spread
        )
        okx_relative_spread = (
            None if okx.order_book is None else okx.order_book.relative_spread
        )
        self._note_missing_pair(
            reasons,
            "price mid prices",
            binance_mid,
            okx_mid,
            channel="price",
        )
        self._note_missing_pair(
            reasons,
            "venue returns",
            binance_return,
            okx_return,
            channel="price.return_direction_agreement",
        )
        return CrossVenuePriceFeatureValues(
            binance_mid_price=binance_mid,
            okx_mid_price=okx_mid,
            mid_price_difference=midpoint_difference,
            mid_price_absolute_spread=absolute_spread,
            percentage_spread_denominator=denominator,
            mid_price_percentage_spread=percentage_spread,
            mid_price_spread_zscore=spread_zscore,
            return_direction_agreement=_direction_agreement(
                binance_return,
                okx_return,
                epsilon=self.settings.features.cross_venue_direction_epsilon,
            ),
            price_impulse_direction_agreement=_direction_agreement(
                binance_impulse,
                okx_impulse,
                epsilon=self.settings.features.cross_venue_direction_epsilon,
            ),
            price_impulse_strength_difference=_difference(
                binance_impulse,
                okx_impulse,
            ),
            realized_volatility_difference=_difference(
                binance_volatility,
                okx_volatility,
            ),
            relative_spread_divergence=_difference(
                binance_relative_spread,
                okx_relative_spread,
            ),
        )

    def _order_flow_features(
        self,
        binance: FeatureSnapshot,
        okx: FeatureSnapshot,
        *,
        reasons: list[FeatureUnavailableReason],
    ) -> CrossVenueOrderFlowFeatureValues:
        binance_taker = (
            None if binance.trade_flow is None else binance.trade_flow.taker_imbalance
        )
        okx_taker = (
            None if okx.trade_flow is None else okx.trade_flow.taker_imbalance
        )
        binance_ofi = (
            None
            if binance.order_book is None
            else binance.order_book.order_flow_imbalance
        )
        okx_ofi = (
            None if okx.order_book is None else okx.order_book.order_flow_imbalance
        )
        binance_depth = (
            None if binance.order_book is None else binance.order_book.depth_imbalance
        )
        okx_depth = (
            None if okx.order_book is None else okx.order_book.depth_imbalance
        )
        binance_add = (
            None
            if binance.order_book is None
            else binance.order_book.added_liquidity_quantity
        )
        okx_add = (
            None
            if okx.order_book is None
            else okx.order_book.added_liquidity_quantity
        )
        binance_remove = (
            None
            if binance.order_book is None
            else binance.order_book.removed_liquidity_quantity
        )
        okx_remove = (
            None
            if okx.order_book is None
            else okx.order_book.removed_liquidity_quantity
        )
        binance_recovery = (
            None
            if binance.order_book is None
            else binance.order_book.liquidity_recovery_quantity_per_second
        )
        okx_recovery = (
            None
            if okx.order_book is None
            else okx.order_book.liquidity_recovery_quantity_per_second
        )
        self._note_missing_pair(
            reasons,
            "taker imbalances",
            binance_taker,
            okx_taker,
            channel="order_flow.taker",
        )
        self._note_missing_pair(
            reasons,
            "order-flow imbalances",
            binance_ofi,
            okx_ofi,
            channel="order_flow.ofi",
        )
        strength = StrengthAgreement.UNAVAILABLE
        if binance_taker is not None and okx_taker is not None:
            difference = abs(abs(binance_taker) - abs(okx_taker))
            strength = (
                StrengthAgreement.CONSISTENT
                if difference
                <= self.settings.features.cross_venue_strength_tolerance
                else StrengthAgreement.DIVERGENT
            )
        return CrossVenueOrderFlowFeatureValues(
            taker_flow_direction_agreement=_direction_agreement(
                binance_taker,
                okx_taker,
                epsilon=self.settings.features.cross_venue_direction_epsilon,
            ),
            taker_imbalance_difference=_difference(binance_taker, okx_taker),
            taker_imbalance_strength_agreement=strength,
            ofi_direction_agreement=_direction_agreement(
                binance_ofi,
                okx_ofi,
                epsilon=self.settings.features.cross_venue_direction_epsilon,
            ),
            ofi_difference=_difference(binance_ofi, okx_ofi),
            depth_imbalance_difference=_difference(binance_depth, okx_depth),
            liquidity_addition_difference=_decimal_difference(
                binance_add,
                okx_add,
            ),
            liquidity_removal_difference=_decimal_difference(
                binance_remove,
                okx_remove,
            ),
            order_book_recovery_speed_difference=_difference(
                binance_recovery,
                okx_recovery,
            ),
            liquidity_divergence_status=self._liquidity_status(
                binance_add,
                binance_remove,
                okx_add,
                okx_remove,
            ),
        )

    def _positioning_features(
        self,
        binance: FeatureSnapshot,
        okx: FeatureSnapshot,
        *,
        reasons: list[FeatureUnavailableReason],
    ) -> CrossVenuePositioningFeatureValues:
        binance_oi_change = (
            None
            if binance.open_interest is None
            else binance.open_interest.percentage_change
        )
        okx_oi_change = (
            None
            if okx.open_interest is None
            else okx.open_interest.percentage_change
        )
        binance_oi_direction = _direction(
            binance_oi_change,
            epsilon=self.settings.features.cross_venue_direction_epsilon,
        )
        okx_oi_direction = _direction(
            okx_oi_change,
            epsilon=self.settings.features.cross_venue_direction_epsilon,
        )
        binance_state = (
            None
            if binance.open_interest is None
            else binance.open_interest.price_oi_state
        )
        okx_state = (
            None if okx.open_interest is None else okx.open_interest.price_oi_state
        )
        price_oi_agreement = ContextAgreement.UNAVAILABLE
        if binance_state is not None and okx_state is not None:
            price_oi_agreement = (
                ContextAgreement.MATCHED
                if binance_state is okx_state
                else ContextAgreement.CONFLICT
            )
        binance_funding = (
            None if binance.crowding is None else binance.crowding.funding_rate
        )
        okx_funding = None if okx.crowding is None else okx.crowding.funding_rate
        binance_funding_zscore = (
            None if binance.crowding is None else binance.crowding.funding_zscore
        )
        okx_funding_zscore = (
            None if okx.crowding is None else okx.crowding.funding_zscore
        )
        binance_abnormality = (
            None
            if binance_funding_zscore is None
            else abs(binance_funding_zscore)
        )
        okx_abnormality = (
            None if okx_funding_zscore is None else abs(okx_funding_zscore)
        )
        binance_premium = (
            None
            if binance.crowding is None
            else binance.crowding.mark_index_premium
        )
        okx_premium = (
            None if okx.crowding is None else okx.crowding.mark_index_premium
        )
        binance_crowding = (
            None if binance.crowding is None else binance.crowding.joint_state
        )
        okx_crowding = (
            None if okx.crowding is None else okx.crowding.joint_state
        )
        crowding_agreement = self._crowding_agreement(
            binance_crowding,
            okx_crowding,
        )
        liquidation_agreement = self._liquidation_agreement(binance, okx)
        self._note_missing_pair(
            reasons,
            "percentage open-interest changes",
            binance_oi_change,
            okx_oi_change,
            channel="positioning.open_interest",
        )
        self._note_missing_pair(
            reasons,
            "funding rates",
            binance_funding,
            okx_funding,
            channel="positioning.funding",
        )
        return CrossVenuePositioningFeatureValues(
            price_oi_state_agreement=price_oi_agreement,
            binance_oi_change_direction=binance_oi_direction,
            okx_oi_change_direction=okx_oi_direction,
            oi_context_conflict=(
                None
                if price_oi_agreement is ContextAgreement.UNAVAILABLE
                else price_oi_agreement is ContextAgreement.CONFLICT
            ),
            funding_direction_agreement=_direction_agreement(
                binance_funding,
                okx_funding,
                epsilon=self.settings.features.cross_venue_direction_epsilon,
            ),
            binance_funding_abnormality=binance_abnormality,
            okx_funding_abnormality=okx_abnormality,
            funding_abnormality_difference=_difference(
                binance_abnormality,
                okx_abnormality,
            ),
            mark_index_premium_difference=_difference(
                binance_premium,
                okx_premium,
            ),
            crowding_direction_agreement=crowding_agreement,
            one_sided_crowding_unconfirmed=(
                None
                if crowding_agreement is CrowdingAgreement.UNAVAILABLE
                else crowding_agreement is CrowdingAgreement.ONE_SIDED
            ),
            liquidation_activity_agreement=liquidation_agreement,
        )

    @staticmethod
    def _confirmation_features(
        price: CrossVenuePriceFeatureValues,
        order_flow: CrossVenueOrderFlowFeatureValues,
        positioning: CrossVenuePositioningFeatureValues,
        *,
        alignment_quality: float,
    ) -> CrossVenueConfirmationFeatureValues:
        values = tuple(
            value
            for value in (
                _agreement_value(price.return_direction_agreement),
                _agreement_value(price.price_impulse_direction_agreement),
                _agreement_value(order_flow.taker_flow_direction_agreement),
                _agreement_value(order_flow.ofi_direction_agreement),
                _context_value(positioning.price_oi_state_agreement),
                _crowding_value(positioning.crowding_direction_agreement),
                _activity_value(positioning.liquidation_activity_agreement),
            )
            if value is not None
        )
        confirmation = fmean(values) if values else None
        divergence_penalty = (
            None
            if not values
            else sum(1 for value in values if value < 0) / len(values)
        )
        return CrossVenueConfirmationFeatureValues(
            price_direction_agreement=price.return_direction_agreement,
            price_impulse_agreement=price.price_impulse_direction_agreement,
            taker_flow_agreement=order_flow.taker_flow_direction_agreement,
            ofi_agreement=order_flow.ofi_direction_agreement,
            oi_context_conflict=positioning.oi_context_conflict,
            crowding_agreement=positioning.crowding_direction_agreement,
            liquidation_activity_agreement=(
                positioning.liquidation_activity_agreement
            ),
            cross_venue_confirmation=confirmation,
            divergence_penalty_input=divergence_penalty,
            alignment_quality=alignment_quality,
        )

    def _spread_zscore(
        self,
        snapshots: tuple[FeatureSnapshot, ...],
        *,
        symbol: str,
        decision: datetime,
        window_seconds: int,
        value: float | None,
    ) -> float | None:
        if value is None:
            return None
        boundary = decision - timedelta(
            seconds=self.settings.features.zscore_lookback_seconds
        )
        paired: dict[
            datetime,
            dict[Exchange, FeatureSnapshot],
        ] = {}
        for snapshot in snapshots:
            if (
                snapshot.symbol != symbol
                or snapshot.window_seconds != window_seconds
                or not boundary < snapshot.decision_timestamp < decision
            ):
                continue
            venue_at_time = paired.setdefault(snapshot.decision_timestamp, {})
            existing = venue_at_time.get(snapshot.exchange)
            if (
                existing is None
                or snapshot.feature_snapshot_id.hex
                > existing.feature_snapshot_id.hex
            ):
                venue_at_time[snapshot.exchange] = snapshot
        history: list[float] = []
        for timestamp in sorted(paired):
            pair = paired[timestamp]
            binance = pair.get(Exchange.BINANCE)
            okx = pair.get(Exchange.OKX)
            if binance is None or okx is None:
                continue
            binance_mid = (
                None
                if binance.order_book is None
                else binance.order_book.mid_price
            )
            okx_mid = None if okx.order_book is None else okx.order_book.mid_price
            spread = self._percentage_spread(binance_mid, okx_mid)
            if spread is not None:
                history.append(spread)
        minimum = self.settings.features.cross_venue_zscore_minimum_samples
        if len(history) < minimum:
            return None
        deviation = pstdev(history)
        if deviation == 0:
            return 0.0 if value == history[-1] else None
        return (value - fmean(history)) / deviation

    @staticmethod
    def _percentage_spread(
        binance_mid: Decimal | None,
        okx_mid: Decimal | None,
        *,
        reasons: list[FeatureUnavailableReason] | None = None,
        channel: str | None = None,
    ) -> float | None:
        if binance_mid is None or okx_mid is None:
            return None
        denominator = (abs(binance_mid) + abs(okx_mid)) / Decimal(2)
        if denominator == 0:
            if reasons is not None:
                reasons.append(
                    _reason(
                        FeatureUnavailableCode.ZERO_DENOMINATOR,
                        "symmetric mid-price denominator is zero",
                        channel=channel,
                    )
                )
            return None
        return float((binance_mid - okx_mid) / denominator)

    @staticmethod
    def _note_missing_pair(
        reasons: list[FeatureUnavailableReason],
        label: str,
        left: object | None,
        right: object | None,
        *,
        channel: str,
    ) -> None:
        if left is not None and right is not None:
            return
        missing = []
        if left is None:
            missing.append("Binance")
        if right is None:
            missing.append("OKX")
        reasons.append(
            _reason(
                FeatureUnavailableCode.FEATURE_INPUT_MISSING,
                f"{' and '.join(missing)} missing {label}",
                channel=channel,
            )
        )

    def _liquidity_status(
        self,
        binance_add: Decimal | None,
        binance_remove: Decimal | None,
        okx_add: Decimal | None,
        okx_remove: Decimal | None,
    ) -> LiquidityDivergenceStatus:
        if None in (
            binance_add,
            binance_remove,
            okx_add,
            okx_remove,
        ):
            return LiquidityDivergenceStatus.UNAVAILABLE
        assert binance_add is not None
        assert binance_remove is not None
        assert okx_add is not None
        assert okx_remove is not None
        binance_direction = _direction(
            binance_add - binance_remove,
            epsilon=self.settings.features.cross_venue_direction_epsilon,
        )
        okx_direction = _direction(
            okx_add - okx_remove,
            epsilon=self.settings.features.cross_venue_direction_epsilon,
        )
        if binance_direction is not okx_direction:
            return LiquidityDivergenceStatus.DIVERGENT
        return {
            DirectionState.POSITIVE: LiquidityDivergenceStatus.CONFIRMED_ADDITION,
            DirectionState.NEGATIVE: LiquidityDivergenceStatus.CONFIRMED_REMOVAL,
            DirectionState.FLAT: LiquidityDivergenceStatus.NEUTRAL,
        }[binance_direction]

    @staticmethod
    def _crowding_agreement(
        binance: CrowdingState | None,
        okx: CrowdingState | None,
    ) -> CrowdingAgreement:
        if binance is None or okx is None:
            return CrowdingAgreement.UNAVAILABLE
        if binance is okx:
            return {
                CrowdingState.CROWDED_LONG: CrowdingAgreement.BOTH_LONG,
                CrowdingState.CROWDED_SHORT: CrowdingAgreement.BOTH_SHORT,
                CrowdingState.MIXED: CrowdingAgreement.BOTH_MIXED,
            }[binance]
        if CrowdingState.MIXED in (binance, okx):
            return CrowdingAgreement.ONE_SIDED
        return CrowdingAgreement.DIVERGENT

    def _liquidation_agreement(
        self,
        binance: FeatureSnapshot,
        okx: FeatureSnapshot,
    ) -> ActivityAgreement:
        binance_zscore = (
            None
            if binance.liquidation is None
            else binance.liquidation.public_sample_activity_zscore
        )
        okx_zscore = (
            None
            if okx.liquidation is None
            else okx.liquidation.public_sample_activity_zscore
        )
        if binance_zscore is None or okx_zscore is None:
            return ActivityAgreement.UNAVAILABLE
        threshold = self.settings.features.abnormal_jump_zscore
        binance_elevated = binance_zscore >= threshold
        okx_elevated = okx_zscore >= threshold
        if binance_elevated and okx_elevated:
            return ActivityAgreement.BOTH_ELEVATED
        if (
            (binance_zscore >= threshold
            and okx_zscore <= -threshold)
            or (okx_zscore >= threshold
            and binance_zscore <= -threshold)
        ):
            return ActivityAgreement.DIVERGENT
        if binance_elevated or okx_elevated:
            return ActivityAgreement.ONE_SIDED
        return ActivityAgreement.BOTH_NORMAL

    def _feature_id(
        self,
        *,
        symbol: str,
        decision: datetime,
        window_seconds: int,
        binance: FeatureSnapshot | None,
        okx: FeatureSnapshot | None,
        status: AlignmentStatus,
    ) -> UUID:
        binance_id = "missing" if binance is None else binance.feature_snapshot_id.hex
        okx_id = "missing" if okx is None else okx.feature_snapshot_id.hex
        return uuid5(
            NAMESPACE_URL,
            (
                f"cvf:cross-venue:{self.settings.app.strategy_version}:"
                f"{__version__}:{self.config_hash}:{symbol}:{decision.isoformat()}:"
                f"{window_seconds}:{binance_id}:{okx_id}:{status.value}"
            ),
        )

"""Feature health and warmup gates over bounded market state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from cvf.features.models import FeatureUnavailableCode, FeatureUnavailableReason
from cvf.features.state import VenueSymbolState
from cvf.models.enums import HealthStatus


@dataclass(frozen=True, slots=True)
class FeatureAvailability:
    is_warm: bool
    is_healthy: bool
    reasons: tuple[FeatureUnavailableReason, ...]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("decision timestamp must be timezone-aware")
    return value.astimezone(UTC)


def evaluate_availability(
    state: VenueSymbolState,
    *,
    decision_timestamp: datetime,
    warmup: timedelta,
    open_interest_stale_after: timedelta,
    blocked_health_statuses: frozenset[HealthStatus],
    pipeline_backlogged: bool = False,
) -> FeatureAvailability:
    """Return structured blockers; missing is never represented as numeric zero."""

    decision_at = _utc(decision_timestamp)
    if warmup <= timedelta(0):
        raise ValueError("warmup must be positive")
    if open_interest_stale_after <= timedelta(0):
        raise ValueError("open_interest_stale_after must be positive")
    reasons: list[FeatureUnavailableReason] = []

    if not any(item.timestamp <= decision_at for item in state.trades):
        reasons.append(
            FeatureUnavailableReason(
                code=FeatureUnavailableCode.NO_TRADES,
                detail="no accepted trades are available",
            )
        )
    book = state.order_book.view(depth=1)
    if not book.synchronized:
        reasons.append(
            FeatureUnavailableReason(
                code=FeatureUnavailableCode.BOOK_UNSYNCHRONIZED,
                detail=book.last_error or "local feature book has no valid snapshot",
                channel="order_book",
            )
        )
    book_items = list(state.book_updates)
    if not book_items or book_items[0].timestamp > decision_at - warmup:
        reasons.append(
            FeatureUnavailableReason(
                code=FeatureUnavailableCode.BOOK_GENERATION_WARMUP,
                detail="current book generation has not covered the required warmup",
                channel="order_book",
            )
        )
    latest_oi = next(
        (
            item.value
            for item in reversed(list(state.open_interest))
            if item.timestamp <= decision_at
        ),
        None,
    )
    if latest_oi is None:
        reasons.append(
            FeatureUnavailableReason(
                code=FeatureUnavailableCode.OPEN_INTEREST_MISSING,
                detail="open interest has not been observed",
                channel="open_interest",
            )
        )
    elif decision_at - latest_oi.exchange_timestamp > open_interest_stale_after:
        reasons.append(
            FeatureUnavailableReason(
                code=FeatureUnavailableCode.OPEN_INTEREST_STALE,
                detail="latest open interest exceeds the configured freshness limit",
                channel="open_interest",
            )
        )
    for channel, status in state.health_by_channel.items():
        if status in blocked_health_statuses:
            reasons.append(
                FeatureUnavailableReason(
                    code=FeatureUnavailableCode.HEALTH_BLOCKED,
                    detail=f"{state.exchange.value} {channel} is {status.value}",
                    channel=channel,
                )
            )
    if pipeline_backlogged:
        reasons.append(
            FeatureUnavailableReason(
                code=FeatureUnavailableCode.PIPELINE_BACKLOG,
                detail="feature consumer backlog makes the snapshot stale",
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
    return FeatureAvailability(
        is_warm=not any(reason.code in warm_codes for reason in reasons),
        is_healthy=not any(reason.code in health_codes for reason in reasons),
        reasons=tuple(reasons),
    )

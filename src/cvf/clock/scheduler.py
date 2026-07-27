"""Deterministic feature and signal decision boundaries."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum


class TickKind(StrEnum):
    FEATURE = "FEATURE"
    SIGNAL = "SIGNAL"


@dataclass(frozen=True, slots=True)
class DecisionTick:
    timestamp: datetime
    kind: TickKind


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("scheduler timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _next_boundary(after: datetime, interval: timedelta) -> datetime:
    interval_us = interval // timedelta(microseconds=1)
    if interval_us < 1:
        raise ValueError("scheduler intervals must be at least one microsecond")
    epoch_us = (after - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)
    next_us = ((epoch_us // interval_us) + 1) * interval_us
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=next_us)


class DecisionScheduler:
    """Advance through fixed UTC boundaries without depending on task interleaving."""

    def __init__(
        self,
        *,
        start: datetime,
        feature_interval: timedelta,
        signal_interval: timedelta,
    ) -> None:
        self._cursor = _utc(start)
        if feature_interval <= timedelta(0) or signal_interval <= timedelta(0):
            raise ValueError("scheduler intervals must be positive")
        self._feature_interval = feature_interval
        self._signal_interval = signal_interval
        self._next_feature = _next_boundary(self._cursor, feature_interval)
        self._next_signal = _next_boundary(self._cursor, signal_interval)

    @property
    def cursor(self) -> datetime:
        return self._cursor

    def has_due_tick(self, target: datetime) -> bool:
        """Return whether advancing to target would emit a decision tick."""

        end = _utc(target)
        if end < self._cursor:
            raise ValueError("decision scheduler cannot move backwards")
        return min(self._next_feature, self._next_signal) <= end

    def advance_to(self, target: datetime) -> list[DecisionTick]:
        """Return every due tick in stable timestamp/feature-before-signal order."""

        end = _utc(target)
        if end < self._cursor:
            raise ValueError("decision scheduler cannot move backwards")
        ticks: list[DecisionTick] = []
        while min(self._next_feature, self._next_signal) <= end:
            boundary = min(self._next_feature, self._next_signal)
            if self._next_feature == boundary:
                ticks.append(DecisionTick(boundary, TickKind.FEATURE))
                self._next_feature += self._feature_interval
            if self._next_signal == boundary:
                ticks.append(DecisionTick(boundary, TickKind.SIGNAL))
                self._next_signal += self._signal_interval
        self._cursor = end
        return ticks

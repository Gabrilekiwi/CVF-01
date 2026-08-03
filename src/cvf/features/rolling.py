"""Strictly bounded event-time windows with explicit late-event behavior."""

from __future__ import annotations

from bisect import bisect_right
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum


class LateEventPolicy(StrEnum):
    DROP = "drop"
    INSERT = "insert"


class AppendStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    LATE_DROPPED = "LATE_DROPPED"
    EXPIRED_DROPPED = "EXPIRED_DROPPED"


@dataclass(frozen=True, slots=True)
class TimedValue[T]:
    timestamp: datetime
    value: T
    ordinal: int


@dataclass(frozen=True, slots=True)
class WindowStats:
    size: int
    accepted: int
    late_dropped: int
    expired_dropped: int
    capacity_evictions: int
    watermark: datetime | None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("window timestamps must be timezone-aware")
    return value.astimezone(UTC)


class BoundedTimeWindow[T]:
    """Keep ``(watermark-retention, watermark]`` with a hard item cap."""

    def __init__(
        self,
        *,
        retention: timedelta,
        maximum_items: int,
        late_event_policy: LateEventPolicy = LateEventPolicy.DROP,
        maximum_lateness: timedelta = timedelta(0),
    ) -> None:
        if retention <= timedelta(0):
            raise ValueError("retention must be positive")
        if maximum_items < 1:
            raise ValueError("maximum_items must be positive")
        if maximum_lateness < timedelta(0):
            raise ValueError("maximum_lateness cannot be negative")
        self.retention = retention
        self.maximum_items = maximum_items
        self.late_event_policy = late_event_policy
        self.maximum_lateness = maximum_lateness
        self._items: deque[TimedValue[T]] = deque()
        self._watermark: datetime | None = None
        self._ordinal = 0
        self._accepted = 0
        self._late_dropped = 0
        self._expired_dropped = 0
        self._capacity_evictions = 0

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[TimedValue[T]]:
        return iter(self._items)

    @property
    def earliest(self) -> TimedValue[T] | None:
        return None if not self._items else self._items[0]

    @property
    def latest(self) -> TimedValue[T] | None:
        return None if not self._items else self._items[-1]

    @property
    def latest_ordinal(self) -> int:
        """Return the monotonically increasing append ordinal."""

        return self._ordinal

    @property
    def stats(self) -> WindowStats:
        return WindowStats(
            size=len(self),
            accepted=self._accepted,
            late_dropped=self._late_dropped,
            expired_dropped=self._expired_dropped,
            capacity_evictions=self._capacity_evictions,
            watermark=self._watermark,
        )

    def _prune(self) -> None:
        if self._watermark is None:
            return
        cutoff = self._watermark - self.retention
        while self._items and self._items[0].timestamp <= cutoff:
            self._items.popleft()
        while len(self._items) > self.maximum_items:
            self._items.popleft()
            self._capacity_evictions += 1

    def admissibility(self, timestamp: datetime) -> AppendStatus:
        """Classify an append without mutating items, counters, or watermark."""

        event_at = _utc(timestamp)
        watermark = self._watermark
        if watermark is not None and event_at < watermark:
            if event_at <= watermark - self.retention:
                return AppendStatus.EXPIRED_DROPPED
            if (
                self.late_event_policy is LateEventPolicy.DROP
                or watermark - event_at > self.maximum_lateness
            ):
                return AppendStatus.LATE_DROPPED
        return AppendStatus.ACCEPTED

    def append(self, timestamp: datetime, value: T) -> AppendStatus:
        event_at = _utc(timestamp)
        watermark = self._watermark
        status = self.admissibility(event_at)
        if status is AppendStatus.EXPIRED_DROPPED:
            self._expired_dropped += 1
            return status
        if status is AppendStatus.LATE_DROPPED:
            self._late_dropped += 1
            return status
        self._ordinal += 1
        item = TimedValue(event_at, value, self._ordinal)
        if watermark is None or event_at >= watermark:
            self._items.append(item)
            self._watermark = event_at
        else:
            items = list(self._items)
            keys = [(entry.timestamp, entry.ordinal) for entry in items]
            items.insert(bisect_right(keys, (event_at, self._ordinal)), item)
            self._items = deque(items)
        self._accepted += 1
        self._prune()
        return AppendStatus.ACCEPTED

    def items_between(
        self,
        start_exclusive: datetime,
        end_inclusive: datetime,
    ) -> list[TimedValue[T]]:
        """Return timestamped items in ``(start, end]`` without scanning older history."""

        start = _utc(start_exclusive)
        end = _utc(end_inclusive)
        if end < start:
            raise ValueError("window end cannot precede start")
        result: list[TimedValue[T]] = []
        for item in reversed(self._items):
            if item.timestamp > end:
                continue
            if item.timestamp <= start:
                break
            result.append(item)
        result.reverse()
        return result

    def values_between(self, start_exclusive: datetime, end_inclusive: datetime) -> list[T]:
        """Return values in the explicit trailing interval ``(start, end]``."""

        return [
            item.value
            for item in self.items_between(start_exclusive, end_inclusive)
        ]

    def latest_at_or_before(self, boundary: datetime) -> TimedValue[T] | None:
        """Return the newest item at or before a deterministic decision boundary."""

        end = _utc(boundary)
        for item in reversed(self._items):
            if item.timestamp <= end:
                return item
        return None

    def items_appended_after(self, ordinal: int) -> list[TimedValue[T]]:
        """Return retained items appended after ``ordinal`` in append order."""

        if ordinal < 0:
            raise ValueError("window ordinal cannot be negative")
        if ordinal >= self._ordinal:
            return []
        if self.late_event_policy is LateEventPolicy.DROP:
            result: list[TimedValue[T]] = []
            for item in reversed(self._items):
                if item.ordinal <= ordinal:
                    break
                result.append(item)
            result.reverse()
            return result
        return sorted(
            (item for item in self._items if item.ordinal > ordinal),
            key=lambda item: item.ordinal,
        )

    def clear(self) -> None:
        self._items.clear()
        self._watermark = None

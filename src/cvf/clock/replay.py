"""Explicit event-time clock for deterministic replay."""

from __future__ import annotations

from datetime import UTC, datetime


class ReplayClock:
    def __init__(self, start: datetime) -> None:
        self._current = self._validated(start)

    @staticmethod
    def _validated(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("replay clock timestamps must be timezone-aware")
        return value.astimezone(UTC)

    def now(self) -> datetime:
        return self._current

    def advance_to(self, target: datetime) -> None:
        normalized = self._validated(target)
        if normalized < self._current:
            raise ValueError("replay clock cannot move backwards")
        self._current = normalized

    async def sleep_until(self, target: datetime) -> None:
        self.advance_to(target)

"""Bounded time-to-live deduplication for long-running market-data sessions."""

from __future__ import annotations

import time
from collections import OrderedDict
from collections.abc import Callable, Hashable


class BoundedTTLDeduplicator:
    """Remember recently accepted event identities without unbounded growth."""

    def __init__(
        self,
        *,
        capacity: int,
        ttl_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._capacity = capacity
        self._ttl_seconds = ttl_seconds
        self._monotonic = monotonic
        self._entries: OrderedDict[Hashable, float] = OrderedDict()
        self._last_now: float | None = None

    @property
    def size(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()
        self._last_now = None

    def _validated_now(self, now: float | None) -> float:
        current = self._monotonic() if now is None else now
        if self._last_now is not None and current < self._last_now:
            raise ValueError("monotonic time cannot move backwards")
        self._last_now = current
        return current

    def _expire(self, now: float) -> None:
        while self._entries:
            _, last_seen = next(iter(self._entries.items()))
            if now - last_seen < self._ttl_seconds:
                break
            self._entries.popitem(last=False)

    def seen_or_add(self, identity: Hashable, *, now: float | None = None) -> bool:
        """Return true for a duplicate and refresh its bounded TTL entry."""

        current = self._validated_now(now)
        self._expire(current)
        if identity in self._entries:
            self._entries[identity] = current
            self._entries.move_to_end(identity)
            return True
        self._entries[identity] = current
        if len(self._entries) > self._capacity:
            self._entries.popitem(last=False)
        return False

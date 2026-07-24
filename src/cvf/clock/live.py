"""UTC wall clock with monotonic waiting."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime


class LiveClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep_until(self, target: datetime) -> None:
        if target.tzinfo is None or target.utcoffset() is None:
            raise ValueError("target must be timezone-aware")
        while True:
            remaining = (target.astimezone(UTC) - self.now()).total_seconds()
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

"""Clock contract shared by live processing and deterministic replay."""

from __future__ import annotations

from datetime import datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime:
        """Return the current timezone-aware UTC time."""

    async def sleep_until(self, target: datetime) -> None:
        """Wait or advance until ``target``."""

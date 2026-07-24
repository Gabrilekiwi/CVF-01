"""Venue-neutral local order-book interface and observable state."""

from __future__ import annotations

from enum import StrEnum
from typing import Protocol

from pydantic import Field, computed_field

from cvf.models.common import FrozenModel
from cvf.models.enums import Exchange
from cvf.models.market import OrderBookLevel


class BookStatus(StrEnum):
    """Synchronization state of one exchange/symbol/channel book."""

    BUFFERING = "BUFFERING"
    LIVE = "LIVE"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"


class BookTransition(StrEnum):
    """Result of applying one snapshot or update."""

    BUFFERED = "BUFFERED"
    SNAPSHOT_APPLIED = "SNAPSHOT_APPLIED"
    UPDATE_APPLIED = "UPDATE_APPLIED"
    STALE_IGNORED = "STALE_IGNORED"
    RETRY_SNAPSHOT = "RETRY_SNAPSHOT"
    SEQUENCE_GAP = "SEQUENCE_GAP"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    BUFFER_OVERFLOW = "BUFFER_OVERFLOW"
    RESYNC_STARTED = "RESYNC_STARTED"


class BookApplyResult(FrozenModel):
    """Explicit state-machine transition returned to connector orchestration."""

    transition: BookTransition
    status: BookStatus
    generation: int = Field(ge=0)
    sequence_id: int | None = None
    buffered_events: int = Field(ge=0)
    needs_snapshot: bool
    reason: str | None = None


class BookView(FrozenModel):
    """Venue-neutral, normalized, immutable view of a local book."""

    exchange: Exchange
    symbol: str
    channel: str
    generation: int = Field(ge=0)
    sequence_id: int | None = None
    synchronized: bool
    bids: list[OrderBookLevel] = Field(default_factory=list)
    asks: list[OrderBookLevel] = Field(default_factory=list)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def best_bid(self) -> OrderBookLevel | None:
        return self.bids[0] if self.bids else None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def best_ask(self) -> OrderBookLevel | None:
        return self.asks[0] if self.asks else None


class StatefulOrderBook(Protocol):
    """Common read/reset contract shared by exchange-specific state machines."""

    @property
    def status(self) -> BookStatus: ...

    @property
    def generation(self) -> int: ...

    @property
    def sequence_id(self) -> int | None: ...

    def begin_resync(self, reason: str) -> BookApplyResult: ...

    def view(self, *, depth: int | None = None) -> BookView: ...

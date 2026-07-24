"""Binance USDⓈ-M local order book using the documented snapshot/diff procedure."""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from cvf.exchanges.symbols import to_canonical_symbol
from cvf.models.enums import Exchange
from cvf.models.market import OrderBookLevel
from cvf.normalization.binance import (
    BinanceDepthSnapshotPayload,
    BinanceDepthUpdatePayload,
    BookLevelText,
)
from cvf.normalization.common import decimal_from_text
from cvf.orderbook.base import (
    BookApplyResult,
    BookStatus,
    BookTransition,
    BookView,
)


class BinanceLocalOrderBook:
    """One symbol's bounded-buffer Binance depth state machine."""

    def __init__(self, venue_symbol: str, *, max_buffer_events: int = 10_000) -> None:
        if max_buffer_events < 1:
            raise ValueError("max_buffer_events must be positive")
        self.venue_symbol = venue_symbol.upper()
        self.canonical_symbol = to_canonical_symbol(Exchange.BINANCE, self.venue_symbol)
        self.max_buffer_events = max_buffer_events
        self._status = BookStatus.BUFFERING
        self._generation = 0
        self._sequence_id: int | None = None
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._buffer: deque[BinanceDepthUpdatePayload] = deque()
        self._pending_snapshot: BinanceDepthSnapshotPayload | None = None

    @property
    def status(self) -> BookStatus:
        return self._status

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def sequence_id(self) -> int | None:
        return self._sequence_id

    @property
    def buffered_events(self) -> int:
        return len(self._buffer)

    def _result(
        self,
        transition: BookTransition,
        *,
        needs_snapshot: bool,
        reason: str | None = None,
    ) -> BookApplyResult:
        return BookApplyResult(
            transition=transition,
            status=self._status,
            generation=self._generation,
            sequence_id=self._sequence_id,
            buffered_events=len(self._buffer),
            needs_snapshot=needs_snapshot,
            reason=reason,
        )

    def begin_resync(self, reason: str) -> BookApplyResult:
        """Invalidate the current generation and request a fresh REST snapshot."""

        self._generation += 1
        self._status = BookStatus.RESYNC_REQUIRED
        self._sequence_id = None
        self._bids.clear()
        self._asks.clear()
        self._buffer.clear()
        self._pending_snapshot = None
        return self._result(
            BookTransition.RESYNC_STARTED,
            needs_snapshot=True,
            reason=reason,
        )

    def _overflow(self, update: BinanceDepthUpdatePayload) -> BookApplyResult:
        self._generation += 1
        self._status = BookStatus.RESYNC_REQUIRED
        self._sequence_id = None
        self._bids.clear()
        self._asks.clear()
        self._pending_snapshot = None
        self._buffer.clear()
        self._buffer.append(update)
        return self._result(
            BookTransition.BUFFER_OVERFLOW,
            needs_snapshot=True,
            reason=f"buffer exceeded {self.max_buffer_events} events",
        )

    def _buffer_update(self, update: BinanceDepthUpdatePayload) -> BookApplyResult:
        if len(self._buffer) >= self.max_buffer_events:
            return self._overflow(update)
        self._buffer.append(update)
        if self._pending_snapshot is not None:
            activation = self._try_activate_snapshot()
            if activation is not None:
                return activation
        return self._result(
            BookTransition.BUFFERED,
            needs_snapshot=self._pending_snapshot is None,
        )

    def ingest(self, update: BinanceDepthUpdatePayload) -> BookApplyResult:
        """Buffer or apply a diff event with exact `pu == previous u` continuity."""

        if update.venue_symbol.upper() != self.venue_symbol:
            raise ValueError(f"book {self.venue_symbol} cannot ingest {update.venue_symbol}")
        if self._status is not BookStatus.LIVE:
            return self._buffer_update(update)

        if self._sequence_id is None:
            raise RuntimeError("LIVE Binance book is missing its sequence id")
        if update.final_update_id <= self._sequence_id:
            return self._result(
                BookTransition.STALE_IGNORED,
                needs_snapshot=False,
            )
        if update.previous_final_update_id != self._sequence_id:
            previous = self._sequence_id
            self._generation += 1
            self._status = BookStatus.RESYNC_REQUIRED
            self._sequence_id = None
            self._bids.clear()
            self._asks.clear()
            self._pending_snapshot = None
            self._buffer.clear()
            self._buffer.append(update)
            return self._result(
                BookTransition.SEQUENCE_GAP,
                needs_snapshot=True,
                reason=(f"expected pu={previous}, received pu={update.previous_final_update_id}"),
            )

        self._apply_update(update)
        return self._result(
            BookTransition.UPDATE_APPLIED,
            needs_snapshot=False,
        )

    def install_snapshot(
        self,
        snapshot: BinanceDepthSnapshotPayload,
    ) -> BookApplyResult:
        """Install a REST snapshot and bridge it to the buffered diff stream."""

        self._pending_snapshot = snapshot
        self._sequence_id = snapshot.last_update_id
        self._bids = self._snapshot_side(snapshot.bids)
        self._asks = self._snapshot_side(snapshot.asks)
        self._status = BookStatus.BUFFERING
        activation = self._try_activate_snapshot()
        if activation is not None:
            return activation
        return self._result(
            BookTransition.BUFFERED,
            needs_snapshot=False,
            reason="waiting for a diff event that overlaps lastUpdateId",
        )

    def _try_activate_snapshot(self) -> BookApplyResult | None:
        snapshot = self._pending_snapshot
        if snapshot is None:
            return None

        while self._buffer and self._buffer[0].final_update_id < snapshot.last_update_id:
            self._buffer.popleft()
        if not self._buffer:
            return None

        first_index: int | None = None
        for index, update in enumerate(self._buffer):
            if (
                update.first_update_id <= snapshot.last_update_id
                and update.final_update_id >= snapshot.last_update_id
            ):
                first_index = index
                break
        if first_index is None:
            first = self._buffer[0]
            if first.first_update_id > snapshot.last_update_id:
                self._status = BookStatus.RESYNC_REQUIRED
                return self._result(
                    BookTransition.RETRY_SNAPSHOT,
                    needs_snapshot=True,
                    reason=(
                        f"snapshot lastUpdateId={snapshot.last_update_id} "
                        f"precedes buffered U={first.first_update_id}"
                    ),
                )
            return None

        for _ in range(first_index):
            self._buffer.popleft()
        first = self._buffer.popleft()
        self._apply_update(first)

        while self._buffer:
            update = self._buffer.popleft()
            if self._sequence_id is None:
                raise RuntimeError("snapshot activation lost its sequence id")
            if update.final_update_id <= self._sequence_id:
                continue
            if update.previous_final_update_id != self._sequence_id:
                previous = self._sequence_id
                remaining = [update, *self._buffer]
                self._generation += 1
                self._status = BookStatus.RESYNC_REQUIRED
                self._sequence_id = None
                self._bids.clear()
                self._asks.clear()
                self._pending_snapshot = None
                self._buffer.clear()
                self._buffer.extend(remaining)
                return self._result(
                    BookTransition.SEQUENCE_GAP,
                    needs_snapshot=True,
                    reason=(
                        f"buffered chain expected pu={previous}, received "
                        f"pu={update.previous_final_update_id}"
                    ),
                )
            self._apply_update(update)

        self._pending_snapshot = None
        self._status = BookStatus.LIVE
        return self._result(
            BookTransition.SNAPSHOT_APPLIED,
            needs_snapshot=False,
        )

    @staticmethod
    def _snapshot_side(levels: list[BookLevelText]) -> dict[Decimal, Decimal]:
        side: dict[Decimal, Decimal] = {}
        for price_text, quantity_text in levels:
            price = decimal_from_text(price_text, field_name="price")
            quantity = decimal_from_text(quantity_text, field_name="quantity")
            if quantity > 0:
                side[price] = quantity
        return side

    @staticmethod
    def _apply_side(
        side: dict[Decimal, Decimal],
        levels: list[BookLevelText],
    ) -> None:
        for price_text, quantity_text in levels:
            price = decimal_from_text(price_text, field_name="price")
            quantity = decimal_from_text(quantity_text, field_name="quantity")
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def _apply_update(self, update: BinanceDepthUpdatePayload) -> None:
        self._apply_side(self._bids, update.bids)
        self._apply_side(self._asks, update.asks)
        self._sequence_id = update.final_update_id

    def view(self, *, depth: int | None = None) -> BookView:
        """Return a sorted immutable normalized view."""

        if depth is not None and depth < 1:
            raise ValueError("depth must be positive")
        bids = sorted(self._bids.items(), reverse=True)
        asks = sorted(self._asks.items())
        if depth is not None:
            bids = bids[:depth]
            asks = asks[:depth]
        return BookView(
            exchange=Exchange.BINANCE,
            symbol=self.canonical_symbol,
            channel="depth",
            generation=self._generation,
            sequence_id=self._sequence_id,
            synchronized=self._status is BookStatus.LIVE,
            bids=[OrderBookLevel(price=price, quantity=quantity) for price, quantity in bids],
            asks=[OrderBookLevel(price=price, quantity=quantity) for price, quantity in asks],
        )


class BinanceOrderBookManager:
    """Maintain isolated bounded state for multiple configured Binance symbols."""

    def __init__(self, *, max_buffer_events: int = 10_000) -> None:
        self.max_buffer_events = max_buffer_events
        self._books: dict[str, BinanceLocalOrderBook] = {}

    def book(self, venue_symbol: str) -> BinanceLocalOrderBook:
        normalized = venue_symbol.upper()
        if normalized not in self._books:
            self._books[normalized] = BinanceLocalOrderBook(
                normalized,
                max_buffer_events=self.max_buffer_events,
            )
        return self._books[normalized]

    def ingest(self, update: BinanceDepthUpdatePayload) -> BookApplyResult:
        return self.book(update.venue_symbol).ingest(update)

    def install_snapshot(
        self,
        venue_symbol: str,
        snapshot: BinanceDepthSnapshotPayload,
    ) -> BookApplyResult:
        return self.book(venue_symbol).install_snapshot(snapshot)

    def views(self, *, depth: int | None = None) -> dict[str, BookView]:
        return {venue_symbol: book.view(depth=depth) for venue_symbol, book in self._books.items()}

"""OKX v5 `books` and isolated `books5` local order-book state machines."""

from __future__ import annotations

import zlib
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from cvf.models.enums import Exchange
from cvf.models.market import OrderBookLevel
from cvf.normalization.common import decimal_from_text
from cvf.normalization.instruments import ContractSpecification
from cvf.normalization.okx import (
    OkxBookData,
    OkxBookLevelText,
    OkxBooks5Data,
    OkxBooks5Message,
    OkxBooksMessage,
)
from cvf.orderbook.base import (
    BookApplyResult,
    BookStatus,
    BookTransition,
    BookView,
)


@dataclass(frozen=True, slots=True)
class _RawLevel:
    price: Decimal
    quantity_contracts: Decimal
    price_text: str
    quantity_text: str


def _raw_side(levels: list[OkxBookLevelText]) -> dict[Decimal, _RawLevel]:
    side: dict[Decimal, _RawLevel] = {}
    for price_text, quantity_text, _deprecated, _order_count in levels:
        price = decimal_from_text(price_text, field_name="price")
        quantity = decimal_from_text(
            quantity_text,
            field_name="quantity_contracts",
        )
        if quantity > 0:
            side[price] = _RawLevel(
                price=price,
                quantity_contracts=quantity,
                price_text=price_text,
                quantity_text=quantity_text,
            )
    return side


def _apply_raw_side(
    side: dict[Decimal, _RawLevel],
    levels: list[OkxBookLevelText],
) -> None:
    for price_text, quantity_text, _deprecated, _order_count in levels:
        price = decimal_from_text(price_text, field_name="price")
        quantity = decimal_from_text(
            quantity_text,
            field_name="quantity_contracts",
        )
        if quantity == 0:
            side.pop(price, None)
        else:
            side[price] = _RawLevel(
                price=price,
                quantity_contracts=quantity,
                price_text=price_text,
                quantity_text=quantity_text,
            )


def _signed_crc32(payload: str) -> int:
    checksum = zlib.crc32(payload.encode("utf-8"))
    return checksum if checksum < 2**31 else checksum - 2**32


def calculate_okx_checksum(
    bids: list[OkxBookLevelText],
    asks: list[OkxBookLevelText],
) -> int:
    """Calculate the historical signed CRC32 checksum over interleaved top 25."""

    return _checksum_from_sides(_raw_side(bids), _raw_side(asks))


def _checksum_from_sides(
    bids: dict[Decimal, _RawLevel],
    asks: dict[Decimal, _RawLevel],
) -> int:
    sorted_bids = sorted(bids.values(), key=lambda level: level.price, reverse=True)[:25]
    sorted_asks = sorted(asks.values(), key=lambda level: level.price)[:25]
    values: list[str] = []
    for index in range(max(len(sorted_bids), len(sorted_asks))):
        if index < len(sorted_bids):
            bid = sorted_bids[index]
            values.extend((bid.price_text, bid.quantity_text))
        if index < len(sorted_asks):
            ask = sorted_asks[index]
            values.extend((ask.price_text, ask.quantity_text))
    return _signed_crc32(":".join(values))


class OkxLocalOrderBook:
    """Incremental `books` state with sequence and legacy-checksum validation."""

    def __init__(
        self,
        specification: ContractSpecification,
        *,
        validate_nonzero_checksum: bool = True,
    ) -> None:
        if specification.exchange is not Exchange.OKX:
            raise ValueError("OKX book requires an OKX contract specification")
        self.specification = specification
        self.validate_nonzero_checksum = validate_nonzero_checksum
        self.venue_symbol = specification.venue_symbol
        self.canonical_symbol = specification.canonical_symbol
        self._status = BookStatus.BUFFERING
        self._generation = 0
        self._sequence_id: int | None = None
        self._bids: dict[Decimal, _RawLevel] = {}
        self._asks: dict[Decimal, _RawLevel] = {}

    @property
    def status(self) -> BookStatus:
        return self._status

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def sequence_id(self) -> int | None:
        return self._sequence_id

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
            buffered_events=0,
            needs_snapshot=needs_snapshot,
            reason=reason,
        )

    def begin_resync(self, reason: str) -> BookApplyResult:
        self._generation += 1
        self._status = BookStatus.RESYNC_REQUIRED
        self._sequence_id = None
        self._bids.clear()
        self._asks.clear()
        return self._result(
            BookTransition.RESYNC_STARTED,
            needs_snapshot=True,
            reason=reason,
        )

    def _checksum_valid(self, checksum: int) -> bool:
        if checksum == 0:
            return True
        if not self.validate_nonzero_checksum:
            return True
        return _checksum_from_sides(self._bids, self._asks) == checksum

    def _checksum_failure(self, checksum: int) -> BookApplyResult:
        actual = _checksum_from_sides(self._bids, self._asks)
        self._generation += 1
        self._status = BookStatus.RESYNC_REQUIRED
        self._sequence_id = None
        self._bids.clear()
        self._asks.clear()
        return self._result(
            BookTransition.CHECKSUM_MISMATCH,
            needs_snapshot=True,
            reason=f"expected historical checksum={checksum}, calculated={actual}",
        )

    def ingest(
        self,
        action: Literal["snapshot", "update"],
        data: OkxBookData,
    ) -> BookApplyResult:
        """Apply one `books` snapshot/update or request unsubscribe/resubscribe."""

        if action == "snapshot":
            self._bids = _raw_side(data.bids)
            self._asks = _raw_side(data.asks)
            self._sequence_id = data.sequence_id
            if not self._checksum_valid(data.checksum):
                return self._checksum_failure(data.checksum)
            self._status = BookStatus.LIVE
            return self._result(
                BookTransition.SNAPSHOT_APPLIED,
                needs_snapshot=False,
            )

        if self._status is not BookStatus.LIVE or self._sequence_id is None:
            self._status = BookStatus.RESYNC_REQUIRED
            return self._result(
                BookTransition.SEQUENCE_GAP,
                needs_snapshot=True,
                reason="received incremental books data before a valid snapshot",
            )
        if data.previous_sequence_id != self._sequence_id:
            previous = self._sequence_id
            self._generation += 1
            self._status = BookStatus.RESYNC_REQUIRED
            self._sequence_id = None
            self._bids.clear()
            self._asks.clear()
            return self._result(
                BookTransition.SEQUENCE_GAP,
                needs_snapshot=True,
                reason=(
                    f"expected prevSeqId={previous}, received prevSeqId={data.previous_sequence_id}"
                ),
            )

        _apply_raw_side(self._bids, data.bids)
        _apply_raw_side(self._asks, data.asks)
        self._sequence_id = data.sequence_id
        if not self._checksum_valid(data.checksum):
            return self._checksum_failure(data.checksum)
        return self._result(
            BookTransition.UPDATE_APPLIED,
            needs_snapshot=False,
        )

    def view(self, *, depth: int | None = None) -> BookView:
        if depth is not None and depth < 1:
            raise ValueError("depth must be positive")
        bids = sorted(self._bids.values(), key=lambda level: level.price, reverse=True)
        asks = sorted(self._asks.values(), key=lambda level: level.price)
        if depth is not None:
            bids = bids[:depth]
            asks = asks[:depth]
        return BookView(
            exchange=Exchange.OKX,
            symbol=self.canonical_symbol,
            channel="books",
            generation=self._generation,
            sequence_id=self._sequence_id,
            synchronized=self._status is BookStatus.LIVE,
            bids=[self._normalized_level(level) for level in bids],
            asks=[self._normalized_level(level) for level in asks],
        )

    def _normalized_level(self, level: _RawLevel) -> OrderBookLevel:
        return OrderBookLevel(
            price=level.price,
            quantity=self.specification.contracts_to_base(level.quantity_contracts),
        )


class OkxBooks5OrderBook:
    """Isolated full-snapshot `books5` view with no incremental assumptions."""

    def __init__(self, specification: ContractSpecification) -> None:
        if specification.exchange is not Exchange.OKX:
            raise ValueError("OKX books5 requires an OKX contract specification")
        self.specification = specification
        self.venue_symbol = specification.venue_symbol
        self.canonical_symbol = specification.canonical_symbol
        self._status = BookStatus.BUFFERING
        self._generation = 0
        self._sequence_id: int | None = None
        self._bids: dict[Decimal, _RawLevel] = {}
        self._asks: dict[Decimal, _RawLevel] = {}

    @property
    def status(self) -> BookStatus:
        return self._status

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def sequence_id(self) -> int | None:
        return self._sequence_id

    def begin_resync(self, reason: str) -> BookApplyResult:
        self._generation += 1
        self._status = BookStatus.RESYNC_REQUIRED
        self._sequence_id = None
        self._bids.clear()
        self._asks.clear()
        return BookApplyResult(
            transition=BookTransition.RESYNC_STARTED,
            status=self._status,
            generation=self._generation,
            sequence_id=None,
            buffered_events=0,
            needs_snapshot=True,
            reason=reason,
        )

    def ingest(self, data: OkxBooks5Data) -> BookApplyResult:
        if data.instrument_id != self.venue_symbol:
            raise ValueError(f"books5 {self.venue_symbol} cannot ingest {data.instrument_id}")
        self._bids = _raw_side(data.bids)
        self._asks = _raw_side(data.asks)
        self._sequence_id = data.sequence_id
        self._status = BookStatus.LIVE
        return BookApplyResult(
            transition=BookTransition.SNAPSHOT_APPLIED,
            status=self._status,
            generation=self._generation,
            sequence_id=self._sequence_id,
            buffered_events=0,
            needs_snapshot=False,
        )

    def view(self, *, depth: int | None = None) -> BookView:
        if depth is not None and depth < 1:
            raise ValueError("depth must be positive")
        bids = sorted(self._bids.values(), key=lambda level: level.price, reverse=True)
        asks = sorted(self._asks.values(), key=lambda level: level.price)
        if depth is not None:
            bids = bids[:depth]
            asks = asks[:depth]
        return BookView(
            exchange=Exchange.OKX,
            symbol=self.canonical_symbol,
            channel="books5",
            generation=self._generation,
            sequence_id=self._sequence_id,
            synchronized=self._status is BookStatus.LIVE,
            bids=[self._normalized_level(level) for level in bids],
            asks=[self._normalized_level(level) for level in asks],
        )

    def _normalized_level(self, level: _RawLevel) -> OrderBookLevel:
        return OrderBookLevel(
            price=level.price,
            quantity=self.specification.contracts_to_base(level.quantity_contracts),
        )


class OkxOrderBookManager:
    """Keep `books` and `books5` states isolated per venue symbol."""

    def __init__(
        self,
        specifications: dict[str, ContractSpecification],
        *,
        validate_nonzero_checksum: bool = True,
    ) -> None:
        self._books = {
            venue_symbol: OkxLocalOrderBook(
                specification,
                validate_nonzero_checksum=validate_nonzero_checksum,
            )
            for venue_symbol, specification in specifications.items()
        }
        self._books5 = {
            venue_symbol: OkxBooks5OrderBook(specification)
            for venue_symbol, specification in specifications.items()
        }

    def ingest_books(self, message: OkxBooksMessage) -> list[BookApplyResult]:
        venue_symbol = message.argument.instrument_id
        if venue_symbol is None:
            raise ValueError("OKX books message is missing instId")
        try:
            book = self._books[venue_symbol]
        except KeyError as exc:
            raise ValueError(f"no OKX books state for {venue_symbol}") from exc
        return [book.ingest(message.action, data) for data in message.data]

    def ingest_books5(self, message: OkxBooks5Message) -> list[BookApplyResult]:
        results: list[BookApplyResult] = []
        for data in message.data:
            try:
                book = self._books5[data.instrument_id]
            except KeyError as exc:
                raise ValueError(f"no OKX books5 state for {data.instrument_id}") from exc
            results.append(book.ingest(data))
        return results

    def book(
        self,
        venue_symbol: str,
        *,
        channel: Literal["books", "books5"] = "books",
    ) -> OkxLocalOrderBook | OkxBooks5OrderBook:
        if channel == "books":
            return self._books[venue_symbol]
        return self._books5[venue_symbol]

"""Deterministic tests for the Binance documented local-book procedure."""

from __future__ import annotations

from decimal import Decimal

from cvf.normalization.binance import (
    BinanceDepthSnapshotPayload,
    BinanceDepthUpdatePayload,
)
from cvf.orderbook import (
    BinanceLocalOrderBook,
    BinanceOrderBookManager,
    BookStatus,
    BookTransition,
)


def snapshot(
    *,
    last_update_id: int = 100,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> BinanceDepthSnapshotPayload:
    return BinanceDepthSnapshotPayload(
        last_update_id=last_update_id,
        event_time_ms=1_784_851_580_000,
        transaction_time_ms=1_784_851_579_999,
        bids=bids or [("100", "1"), ("99", "2")],
        asks=asks or [("101", "1"), ("102", "2")],
    )


def update(
    *,
    symbol: str = "BTCUSDT",
    first: int,
    final: int,
    previous: int,
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
) -> BinanceDepthUpdatePayload:
    return BinanceDepthUpdatePayload(
        event_type="depthUpdate",
        event_time_ms=1_784_851_580_000,
        transaction_time_ms=1_784_851_579_999,
        venue_symbol=symbol,
        first_update_id=first,
        final_update_id=final,
        previous_final_update_id=previous,
        bids=bids or [],
        asks=asks or [],
    )


def synchronized_book() -> BinanceLocalOrderBook:
    book = BinanceLocalOrderBook("BTCUSDT", max_buffer_events=10)
    result = book.ingest(
        update(
            first=99,
            final=101,
            previous=98,
            bids=[("100", "3"), ("99", "0"), ("98", "4")],
            asks=[("101", "2")],
        )
    )
    assert result.transition is BookTransition.BUFFERED
    result = book.install_snapshot(snapshot())
    assert result.transition is BookTransition.SNAPSHOT_APPLIED
    return book


def test_buffers_before_snapshot_and_finds_exact_overlap() -> None:
    book = synchronized_book()
    view = book.view()

    assert book.status is BookStatus.LIVE
    assert view.sequence_id == 101
    assert view.generation == 0
    assert [level.price for level in view.bids] == [
        Decimal("100"),
        Decimal("98"),
    ]
    assert view.bids[0].quantity == Decimal("3")
    assert view.asks[0].quantity == Decimal("2")


def test_updates_are_absolute_and_zero_deletes_levels() -> None:
    book = synchronized_book()
    result = book.ingest(
        update(
            first=102,
            final=102,
            previous=101,
            bids=[("100", "1")],
            asks=[("101", "0"), ("103", "5")],
        )
    )

    assert result.transition is BookTransition.UPDATE_APPLIED
    view = book.view()
    assert view.bids[0].quantity == Decimal("1")
    assert [level.price for level in view.asks] == [
        Decimal("102"),
        Decimal("103"),
    ]


def test_stale_update_is_ignored_without_changing_generation() -> None:
    book = synchronized_book()
    before = book.view()

    result = book.ingest(
        update(
            first=100,
            final=101,
            previous=99,
            bids=[("100", "999")],
        )
    )

    assert result.transition is BookTransition.STALE_IGNORED
    assert book.view() == before
    assert book.generation == 0


def test_pu_gap_invalidates_book_and_requests_fresh_snapshot() -> None:
    book = synchronized_book()

    result = book.ingest(
        update(
            first=102,
            final=103,
            previous=999,
            bids=[("100", "5")],
        )
    )

    assert result.transition is BookTransition.SEQUENCE_GAP
    assert result.needs_snapshot
    assert book.status is BookStatus.RESYNC_REQUIRED
    assert book.generation == 1
    assert book.sequence_id is None
    assert not book.view().synchronized
    assert book.buffered_events == 1


def test_snapshot_that_precedes_first_buffered_u_must_be_retried() -> None:
    book = BinanceLocalOrderBook("BTCUSDT")
    book.ingest(update(first=100, final=101, previous=99))

    result = book.install_snapshot(snapshot(last_update_id=90))

    assert result.transition is BookTransition.RETRY_SNAPSHOT
    assert result.needs_snapshot
    assert book.status is BookStatus.RESYNC_REQUIRED


def test_pending_snapshot_activates_when_bridge_event_arrives() -> None:
    book = BinanceLocalOrderBook("BTCUSDT")
    result = book.install_snapshot(snapshot(last_update_id=100))
    assert result.transition is BookTransition.BUFFERED
    assert not result.needs_snapshot

    result = book.ingest(update(first=100, final=101, previous=99))

    assert result.transition is BookTransition.SNAPSHOT_APPLIED
    assert book.status is BookStatus.LIVE
    assert book.sequence_id == 101


def test_bounded_buffer_overflow_is_explicit_and_starts_new_generation() -> None:
    book = BinanceLocalOrderBook("BTCUSDT", max_buffer_events=2)
    book.ingest(update(first=1, final=1, previous=0))
    book.ingest(update(first=2, final=2, previous=1))

    result = book.ingest(update(first=3, final=3, previous=2))

    assert result.transition is BookTransition.BUFFER_OVERFLOW
    assert result.needs_snapshot
    assert book.generation == 1
    assert book.buffered_events == 1


def test_gap_inside_buffered_chain_invalidates_partial_activation() -> None:
    book = BinanceLocalOrderBook("BTCUSDT")
    book.ingest(update(first=99, final=101, previous=98))
    book.ingest(update(first=102, final=103, previous=777))

    result = book.install_snapshot(snapshot(last_update_id=100))

    assert result.transition is BookTransition.SEQUENCE_GAP
    assert book.status is BookStatus.RESYNC_REQUIRED
    assert book.generation == 1
    assert book.view().bids == []


def test_explicit_reconnect_reset_increments_rebuild_generation() -> None:
    book = synchronized_book()

    result = book.begin_resync("transport reconnected")

    assert result.transition is BookTransition.RESYNC_STARTED
    assert book.generation == 1
    assert book.sequence_id is None
    assert book.view().bids == []


def test_multi_symbol_manager_keeps_books_isolated() -> None:
    manager = BinanceOrderBookManager(max_buffer_events=10)
    manager.ingest(update(symbol="BTCUSDT", first=99, final=101, previous=98))
    manager.ingest(update(symbol="ETHUSDT", first=199, final=201, previous=198))
    manager.install_snapshot("BTCUSDT", snapshot(last_update_id=100))
    manager.install_snapshot(
        "ETHUSDT",
        snapshot(
            last_update_id=200,
            bids=[("3500", "2")],
            asks=[("3501", "3")],
        ),
    )

    views = manager.views(depth=1)
    assert views["BTCUSDT"].symbol == "BTC-USDT-PERP"
    assert views["ETHUSDT"].symbol == "ETH-USDT-PERP"
    assert views["BTCUSDT"].sequence_id == 101
    assert views["ETHUSDT"].sequence_id == 201

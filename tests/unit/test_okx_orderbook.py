"""Deterministic OKX books/books5 sequence, checksum, and rebuild tests."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from cvf.normalization import contract_specifications_from_response, parse_okx_payload
from cvf.normalization.okx import (
    OkxBookData,
    OkxBooks5Message,
    OkxBooksMessage,
)
from cvf.orderbook import (
    BookStatus,
    BookTransition,
    OkxBooks5OrderBook,
    OkxLocalOrderBook,
    OkxOrderBookManager,
    calculate_okx_checksum,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "okx"


def load_fixture(filename: str) -> object:
    return json.loads((FIXTURES / filename).read_text(encoding="utf-8"))


def specification():
    return contract_specifications_from_response(load_fixture("instrument_btc_live.json"))[
        "BTC-USDT-SWAP"
    ]


def books_message(filename: str) -> OkxBooksMessage:
    parsed = parse_okx_payload(load_fixture(filename))
    assert isinstance(parsed, OkxBooksMessage)
    return parsed


def books5_message() -> OkxBooks5Message:
    parsed = parse_okx_payload(load_fixture("books5_live.json"))
    assert isinstance(parsed, OkxBooks5Message)
    return parsed


def test_current_zero_checksum_snapshot_uses_sequence_integrity() -> None:
    book = OkxLocalOrderBook(specification())
    message = books_message("books_snapshot_official.json")

    result = book.ingest(message.action, message.data[0])

    assert result.transition is BookTransition.SNAPSHOT_APPLIED
    assert book.status is BookStatus.LIVE
    assert book.sequence_id == 331282913977
    assert book.view().bids[0].quantity == Decimal("15.8681")


def test_incremental_update_checks_prev_seq_and_applies_absolute_sizes() -> None:
    book = OkxLocalOrderBook(specification())
    snapshot = books_message("books_snapshot_official.json")
    update = books_message("books_update_official.json")
    book.ingest(snapshot.action, snapshot.data[0])

    result = book.ingest(update.action, update.data[0])

    assert result.transition is BookTransition.UPDATE_APPLIED
    view = book.view()
    assert view.sequence_id == 331282914100
    assert view.bids[0].price == Decimal("64930.4")
    assert view.bids[0].quantity == Decimal("15.0000")
    assert Decimal("64930.3") not in {level.price for level in view.bids}
    assert Decimal("64930.8") not in {level.price for level in view.asks}


def test_prev_seq_gap_requires_resubscribe_and_new_snapshot() -> None:
    book = OkxLocalOrderBook(specification())
    snapshot = books_message("books_snapshot_official.json")
    update = books_message("books_update_official.json")
    book.ingest(snapshot.action, snapshot.data[0])
    broken = update.data[0].model_copy(update={"previous_sequence_id": 99})

    result = book.ingest("update", broken)

    assert result.transition is BookTransition.SEQUENCE_GAP
    assert result.needs_snapshot
    assert book.status is BookStatus.RESYNC_REQUIRED
    assert book.generation == 1
    assert book.sequence_id is None
    assert book.view().bids == []


def test_historical_nonzero_checksum_has_fixed_regression_value() -> None:
    message = books_message("books_snapshot_historical_checksum.json")
    data = message.data[0]

    assert calculate_okx_checksum(data.bids, data.asks) == -198771539

    book = OkxLocalOrderBook(specification())
    result = book.ingest(message.action, data)
    assert result.transition is BookTransition.SNAPSHOT_APPLIED
    assert book.status is BookStatus.LIVE


def test_historical_checksum_mismatch_invalidates_generation() -> None:
    message = books_message("books_snapshot_historical_checksum.json")
    invalid = message.data[0].model_copy(update={"checksum": -198771538})
    book = OkxLocalOrderBook(specification())

    result = book.ingest("snapshot", invalid)

    assert result.transition is BookTransition.CHECKSUM_MISMATCH
    assert result.needs_snapshot
    assert book.status is BookStatus.RESYNC_REQUIRED
    assert book.generation == 1
    assert book.view().asks == []


def test_sequence_keepalive_and_documented_maintenance_reset_are_valid() -> None:
    book = OkxLocalOrderBook(specification())
    snapshot = books_message("books_snapshot_official.json")
    book.ingest(snapshot.action, snapshot.data[0])
    current = snapshot.data[0].sequence_id
    keepalive = OkxBookData(
        asks=[],
        bids=[],
        timestamp_ms="1784851712000",
        checksum=0,
        sequence_id=current,
        previous_sequence_id=current,
    )
    reset = keepalive.model_copy(
        update={
            "timestamp_ms": "1784851713000",
            "sequence_id": 3,
            "previous_sequence_id": current,
        }
    )
    after_reset = keepalive.model_copy(
        update={
            "timestamp_ms": "1784851714000",
            "sequence_id": 5,
            "previous_sequence_id": 3,
        }
    )

    assert book.ingest("update", keepalive).transition is BookTransition.UPDATE_APPLIED
    assert book.ingest("update", reset).transition is BookTransition.UPDATE_APPLIED
    assert book.sequence_id == 3
    assert book.ingest("update", after_reset).transition is BookTransition.UPDATE_APPLIED
    assert book.sequence_id == 5


def test_explicit_resync_rebuilds_with_incremented_generation() -> None:
    book = OkxLocalOrderBook(specification())
    snapshot = books_message("books_snapshot_official.json")
    book.ingest(snapshot.action, snapshot.data[0])

    book.begin_resync("service notice")
    rebuilt = book.ingest(snapshot.action, snapshot.data[0])

    assert rebuilt.transition is BookTransition.SNAPSHOT_APPLIED
    assert book.status is BookStatus.LIVE
    assert book.generation == 1
    assert book.view().generation == 1


def test_books5_replaces_full_snapshot_without_incremental_assumptions() -> None:
    book = OkxBooks5OrderBook(specification())
    message = books5_message()
    first = message.data[0]

    result = book.ingest(first)
    replacement = first.model_copy(
        update={
            "sequence_id": first.sequence_id - 100,
            "bids": [
                ("64930.4", "10", "0", "1"),
                ("64930.3", "5", "0", "1"),
            ],
        }
    )
    result = book.ingest(replacement)

    assert result.transition is BookTransition.SNAPSHOT_APPLIED
    assert book.sequence_id == first.sequence_id - 100
    assert book.view().bids[0].quantity == Decimal("0.10")


def test_manager_keeps_books_and_books5_channels_isolated() -> None:
    specs = {"BTC-USDT-SWAP": specification()}
    manager = OkxOrderBookManager(specs)
    snapshot = books_message("books_snapshot_official.json")
    update = books_message("books_update_official.json")
    manager.ingest_books(snapshot)
    manager.ingest_books5(books5_message())
    books5_before = manager.book("BTC-USDT-SWAP", channel="books5").view()

    manager.ingest_books(update)

    books_view = manager.book("BTC-USDT-SWAP", channel="books").view()
    books5_after = manager.book("BTC-USDT-SWAP", channel="books5").view()
    assert books_view.sequence_id == 331282914100
    assert books5_after == books5_before
    assert books_view.channel == "books"
    assert books5_after.channel == "books5"

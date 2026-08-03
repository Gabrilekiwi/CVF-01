"""Fixture-backed tests for venue parsing, units, sides, and deterministic timing."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from pydantic import ValidationError

from cvf.models import (
    AggressorSide,
    BestBidAsk,
    FundingRate,
    IndexPrice,
    LiquidatedPositionSide,
    LiquidationEvent,
    MarkPrice,
    OpenInterest,
    OrderBookSnapshot,
    OrderBookUpdate,
    Trade,
)
from cvf.normalization import (
    BinanceNormalizer,
    NormalizationContext,
    OkxNormalizer,
    contract_specifications_from_response,
    parse_binance_payload,
    parse_okx_payload,
)
from cvf.normalization.binance import (
    BinanceAggTradePayload,
    BinanceDepthUpdatePayload,
)
from cvf.normalization.okx import OkxBooksMessage, OkxTradeMessage
from cvf.replay import RawRecordNormalizer
from cvf.storage.raw import (
    NORMALIZED_EVENT_JOURNAL_CHANNEL,
    normalized_event_journal_record,
)

FIXTURES = Path(__file__).parents[1] / "fixtures"
RECEIVED_AT = datetime(2026, 7, 24, 0, 7, tzinfo=UTC)
NORMALIZED_AT = RECEIVED_AT + timedelta(milliseconds=3)


def load_fixture(exchange: str, filename: str) -> object:
    path = FIXTURES / exchange / filename
    return json.loads(path.read_text(encoding="utf-8"))


def context(*, canonical_symbol: str | None = None) -> NormalizationContext:
    return NormalizationContext(
        local_receive_timestamp=RECEIVED_AT,
        normalization_timestamp=NORMALIZED_AT,
        raw_payload_reference="raw/test/payload.json",
        canonical_symbol=canonical_symbol,
    )


def okx_normalizer() -> OkxNormalizer:
    specifications = {}
    for filename in ("instrument_btc_live.json", "instrument_eth_live.json"):
        specifications.update(contract_specifications_from_response(load_fixture("okx", filename)))
    return OkxNormalizer(specifications)


def test_post_dedup_normalized_event_journal_round_trips_exactly() -> None:
    event = BinanceNormalizer().normalize(
        load_fixture("binance", "agg_trade_live.json"),
        context=context(),
    )[0]
    record = normalized_event_journal_record(event)

    assert record.channel == NORMALIZED_EVENT_JOURNAL_CHANNEL
    assert record.normalization_timestamp == NORMALIZED_AT
    assert RawRecordNormalizer().normalize(record) == [event]

    mismatched = record.model_copy(update={"symbol": "ETH-USDT-PERP"})
    with pytest.raises(ValueError, match="metadata mismatch"):
        RawRecordNormalizer().normalize(mismatched)


def test_binance_parses_combined_stream_and_preserves_decimal_units() -> None:
    payload = load_fixture("binance", "agg_trade_live.json")
    parsed = parse_binance_payload(payload)
    assert isinstance(parsed, BinanceAggTradePayload)

    events = BinanceNormalizer().normalize(payload, context=context())
    assert len(events) == 1
    event = events[0]
    assert isinstance(event, Trade)
    assert event.symbol == "BTC-USDT-PERP"
    assert event.price == Decimal("64955.50")
    assert event.base_quantity == Decimal("0.002")
    assert event.quote_notional == Decimal("129.91100")
    assert event.aggressor_side is AggressorSide.SELL
    assert event.normalization_latency_ms == 3


def test_binance_depth_ids_and_deletions_are_typed() -> None:
    payload = load_fixture("binance", "depth_update_live.json")
    parsed = parse_binance_payload(payload)
    assert isinstance(parsed, BinanceDepthUpdatePayload)
    assert parsed.first_update_id == 11118701985066
    assert parsed.previous_final_update_id == 11118701984795

    event = BinanceNormalizer().normalize(payload, context=context())[0]
    assert isinstance(event, OrderBookUpdate)
    assert event.sequence_id == 11118701999173
    assert event.previous_sequence_id == 11118701984795
    assert any(level.quantity == 0 for level in [*event.bids, *event.asks])


def test_binance_mark_stream_emits_mark_index_and_funding() -> None:
    events = BinanceNormalizer().normalize(
        load_fixture("binance", "mark_price_live.json"),
        context=context(),
    )
    assert [type(event) for event in events] == [MarkPrice, IndexPrice, FundingRate]
    mark, index, funding = events
    assert isinstance(mark, MarkPrice)
    assert isinstance(index, IndexPrice)
    assert isinstance(funding, FundingRate)
    assert mark.mark_price == Decimal("64955.50000000")
    assert index.index_price == Decimal("64977.42695652")
    assert funding.funding_rate == Decimal("0.00007414")
    assert funding.next_funding_timestamp is not None
    assert funding.next_funding_timestamp.tzinfo is UTC


def test_binance_book_ticker_normalizes_best_bid_and_ask() -> None:
    event = BinanceNormalizer().normalize(
        load_fixture("binance", "book_ticker_official.json"),
        context=context(),
    )[0]
    assert isinstance(event, BestBidAsk)
    assert event.bid_price == Decimal("64954.90")
    assert event.ask_price == Decimal("64955.00")
    assert event.sequence_id == 400900217


def test_binance_snapshot_and_force_order_normalize_with_context() -> None:
    snapshot = BinanceNormalizer().normalize(
        load_fixture("binance", "depth_snapshot_official.json"),
        context=context(canonical_symbol="BTC-USDT-PERP"),
    )[0]
    assert isinstance(snapshot, OrderBookSnapshot)
    assert snapshot.sequence_id == 1027024
    assert snapshot.bids[0].price == Decimal("64954.90")

    liquidation = BinanceNormalizer().normalize(
        load_fixture("binance", "force_order_official.json"),
        context=context(),
    )[0]
    assert isinstance(liquidation, LiquidationEvent)
    assert liquidation.position_side is LiquidatedPositionSide.LONG
    assert liquidation.base_quantity == Decimal("0.014")
    assert liquidation.quote_notional == Decimal("138.740")


def test_parsers_ignore_additive_fields_but_reject_missing_required_fields() -> None:
    payload = load_fixture("binance", "agg_trade_live.json")
    assert isinstance(payload, dict)
    data = payload["data"]
    assert isinstance(data, dict)
    data["futureField"] = {"nested": True}
    assert isinstance(parse_binance_payload(payload), BinanceAggTradePayload)

    del data["q"]
    with pytest.raises(ValidationError):
        parse_binance_payload(payload)


def test_okx_public_instruments_drive_contract_conversion() -> None:
    btc = contract_specifications_from_response(load_fixture("okx", "instrument_btc_live.json"))[
        "BTC-USDT-SWAP"
    ]
    eth = contract_specifications_from_response(load_fixture("okx", "instrument_eth_live.json"))[
        "ETH-USDT-SWAP"
    ]

    assert btc.base_units_per_contract == Decimal("0.01")
    assert eth.base_units_per_contract == Decimal("0.1")
    assert btc.contracts_to_base(Decimal("19.7")) == Decimal("0.197")


def test_okx_trade_uses_taker_side_and_outputs_base_and_quote_units() -> None:
    payload = load_fixture("okx", "trade_live.json")
    assert isinstance(parse_okx_payload(payload), OkxTradeMessage)

    event = okx_normalizer().normalize(payload, context=context())[0]
    assert isinstance(event, Trade)
    assert event.contract_quantity == Decimal("19.7")
    assert event.base_quantity == Decimal("0.197")
    assert event.quote_notional == Decimal("12791.2888")
    assert event.aggressor_side is AggressorSide.BUY
    assert event.sequence_id == 331282913128


def test_okx_books_and_books5_convert_contract_quantities() -> None:
    normalizer = okx_normalizer()
    payload = load_fixture("okx", "books_snapshot_official.json")
    assert isinstance(parse_okx_payload(payload), OkxBooksMessage)

    snapshot = normalizer.normalize(payload, context=context())[0]
    assert isinstance(snapshot, OrderBookSnapshot)
    assert snapshot.checksum == "0"
    assert snapshot.bids[0].quantity == Decimal("15.8681")

    update = normalizer.normalize(
        load_fixture("okx", "books_update_official.json"),
        context=context(),
    )[0]
    assert isinstance(update, OrderBookUpdate)
    assert update.previous_sequence_id == 331282913977
    assert any(level.quantity == 0 for level in [*update.bids, *update.asks])

    books5 = normalizer.normalize(
        load_fixture("okx", "books5_live.json"),
        context=context(),
    )[0]
    assert isinstance(books5, OrderBookSnapshot)
    assert books5.depth == 5


def test_okx_open_interest_funding_mark_and_liquidation() -> None:
    normalizer = okx_normalizer()

    open_interest = normalizer.normalize(
        load_fixture("okx", "open_interest_live.json"),
        context=context(),
    )[0]
    assert isinstance(open_interest, OpenInterest)
    assert open_interest.open_interest_contracts == Decimal("3093520.20000001838")
    assert open_interest.open_interest_base == Decimal("30935.2020000001838")

    funding = normalizer.normalize(
        load_fixture("okx", "funding_rate_live.json"),
        context=context(),
    )[0]
    assert isinstance(funding, FundingRate)
    assert funding.funding_rate == Decimal("-0.0000011159895977")

    mark = normalizer.normalize(
        load_fixture("okx", "mark_price_live.json"),
        context=context(),
    )[0]
    assert isinstance(mark, MarkPrice)
    assert mark.mark_price == Decimal("64927.3")

    liquidation = normalizer.normalize(
        load_fixture("okx", "liquidation_official.json"),
        context=context(),
    )[0]
    assert isinstance(liquidation, LiquidationEvent)
    assert liquidation.position_side is LiquidatedPositionSide.SHORT
    assert liquidation.contract_quantity == Decimal("13")
    assert liquidation.base_quantity == Decimal("0.13")

    index = normalizer.normalize(
        load_fixture("okx", "index_ticker_official.json"),
        context=context(),
    )[0]
    assert isinstance(index, IndexPrice)
    assert index.symbol == "BTC-USDT-PERP"
    assert index.index_price == Decimal("64977.4")


def test_replay_with_fixed_context_is_deterministic() -> None:
    payload = load_fixture("okx", "trade_live.json")
    normalizer = okx_normalizer()

    first = normalizer.normalize(payload, context=context())
    second = normalizer.normalize(payload, context=context())

    assert first == second
    assert first[0].normalization_timestamp == NORMALIZED_AT

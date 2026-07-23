"""Validation tests for the phase-1 normalized model contract."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cvf.models import (
    AggressorSide,
    BestBidAsk,
    Exchange,
    ExchangeHealth,
    FundingRate,
    HealthStatus,
    IndexPrice,
    LiquidatedPositionSide,
    LiquidationEvent,
    MarketFeature,
    MarkPrice,
    OpenInterest,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderBookUpdate,
    SignalType,
    SimulatedOrder,
    SimulatedPosition,
    SimulatedTrade,
    Trade,
    TradingSignal,
)

NOW = datetime(2026, 7, 23, 8, 0, tzinfo=UTC)


def metadata(exchange: Exchange = Exchange.BINANCE) -> dict[str, object]:
    return {
        "exchange": exchange,
        "symbol": "BTC-USDT-PERP",
        "exchange_timestamp": NOW,
        "local_receive_timestamp": NOW + timedelta(milliseconds=25),
        "sequence_id": 42,
        "raw_payload_reference": "raw/2026-07-23/binance/trades.parquet#42",
    }


def test_trade_normalizes_timezone_and_calculates_notional() -> None:
    event = Trade(
        **metadata(),
        trade_id="9001",
        price=Decimal("65000.5"),
        quantity=Decimal("0.20"),
        aggressor_side=AggressorSide.BUY,
    )

    assert event.notional == Decimal("13000.100")
    assert event.receive_latency_ms == 25
    assert event.exchange_timestamp.tzinfo is UTC

    plus_eight = timezone(timedelta(hours=8))
    shifted = event.model_copy(
        update={
            "exchange_timestamp": datetime(2026, 7, 23, 16, 0, tzinfo=plus_eight),
        }
    )
    # model_copy is intentionally non-validating; round-trip validation must normalize.
    normalized = Trade.model_validate(shifted.model_dump(exclude={"receive_latency_ms"}))
    assert normalized.exchange_timestamp == NOW
    assert normalized.exchange_timestamp.tzinfo is UTC


def test_naive_timestamps_are_rejected() -> None:
    invalid = metadata()
    invalid["exchange_timestamp"] = datetime(2026, 7, 23, 8, 0)

    with pytest.raises(ValidationError, match="timezone-aware"):
        Trade(
            **invalid,
            trade_id="9001",
            price=Decimal("65000"),
            quantity=Decimal("1"),
            aggressor_side=AggressorSide.SELL,
        )


def test_symbol_wildcard_is_reserved_for_exchange_health() -> None:
    invalid = metadata()
    invalid["symbol"] = "*"

    with pytest.raises(ValidationError, match="reserved for exchange-wide health"):
        Trade(
            **invalid,
            trade_id="9001",
            price=Decimal("65000"),
            quantity=Decimal("1"),
            aggressor_side=AggressorSide.BUY,
        )


def test_order_book_snapshot_enforces_sorting_and_non_crossing() -> None:
    snapshot = OrderBookSnapshot(
        **metadata(),
        bids=[
            OrderBookLevel(price=Decimal("100"), quantity=Decimal("2")),
            OrderBookLevel(price=Decimal("99"), quantity=Decimal("3")),
        ],
        asks=[
            OrderBookLevel(price=Decimal("101"), quantity=Decimal("4")),
            OrderBookLevel(price=Decimal("102"), quantity=Decimal("5")),
        ],
        depth=5,
    )
    assert snapshot.bids[0].price == Decimal("100")

    with pytest.raises(ValidationError, match="strictly descending"):
        OrderBookSnapshot(
            **metadata(),
            bids=[
                OrderBookLevel(price=Decimal("99"), quantity=Decimal("2")),
                OrderBookLevel(price=Decimal("100"), quantity=Decimal("3")),
            ],
            asks=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("4"))],
            depth=5,
        )

    with pytest.raises(ValidationError, match="crossed or locked"):
        OrderBookSnapshot(
            **metadata(),
            bids=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("2"))],
            asks=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("4"))],
            depth=5,
        )


def test_long_entry_signal_requires_consistent_price_levels() -> None:
    signal = TradingSignal(
        **metadata(Exchange.CROSS_VENUE),
        timestamp=NOW,
        signal_type=SignalType.LONG_ENTRY,
        binance_score=2.1,
        okx_score=1.9,
        combined_score=2.0,
        confidence=0.8,
        suggested_exchange=Exchange.BINANCE,
        suggested_entry_price=Decimal("100"),
        stop_loss=Decimal("99"),
        take_profit_1=Decimal("101"),
        take_profit_2=Decimal("102"),
        expires_at=NOW + timedelta(seconds=5),
        reasons=[{"code": "FLOW_CONFIRMED", "message": "Both venues confirm flow"}],
        blocking_conditions=[],
        feature_snapshot_id=uuid4(),
        strategy_version="0.1.0",
    )

    assert signal.signal_type is SignalType.LONG_ENTRY

    with pytest.raises(ValidationError, match="stop < entry < tp1 < tp2"):
        TradingSignal.model_validate(
            {
                **signal.model_dump(exclude={"receive_latency_ms"}),
                "stop_loss": Decimal("101"),
            }
        )


def test_all_normalized_models_expose_required_event_metadata() -> None:
    model_types = [
        Trade,
        OrderBookSnapshot,
        OrderBookUpdate,
        BestBidAsk,
        OpenInterest,
        FundingRate,
        MarkPrice,
        IndexPrice,
        LiquidationEvent,
        ExchangeHealth,
        MarketFeature,
        TradingSignal,
        SimulatedOrder,
        SimulatedPosition,
        SimulatedTrade,
    ]
    required = {
        "exchange",
        "symbol",
        "exchange_timestamp",
        "local_receive_timestamp",
        "event_type",
        "sequence_id",
        "raw_payload_reference",
    }

    for model_type in model_types:
        assert required <= set(model_type.model_fields)


def test_health_status_can_represent_disconnected_exchange_scope() -> None:
    health = ExchangeHealth(
        exchange=Exchange.OKX,
        symbol="*",
        exchange_timestamp=NOW,
        local_receive_timestamp=NOW,
        status=HealthStatus.DISCONNECTED,
        is_connected=False,
        open_interest_stale=True,
    )

    assert health.status is HealthStatus.DISCONNECTED
    assert health.symbol == "*"


def test_liquidation_side_names_the_position_that_was_liquidated() -> None:
    event = LiquidationEvent(
        **metadata(Exchange.OKX),
        liquidation_id="liq-1",
        position_side=LiquidatedPositionSide.LONG,
        price=Decimal("62000"),
        quantity=Decimal("0.5"),
    )

    assert event.position_side is LiquidatedPositionSide.LONG
    assert event.notional == Decimal("31000.0")

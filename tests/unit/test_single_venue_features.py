"""Deterministic single-venue feature calculations and decision-time boundaries."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from cvf.config import Settings, load_settings
from cvf.features import (
    CrowdingState,
    FeatureUnavailableCode,
    MarketStateStore,
    PriceOpenInterestState,
    SingleVenueFeatureEngine,
)
from cvf.models import (
    AggressorSide,
    Exchange,
    FundingRate,
    IndexPrice,
    LiquidatedPositionSide,
    LiquidationEvent,
    MarkPrice,
    OpenInterest,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderBookUpdate,
    Trade,
)

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
SYMBOL = "BTC-USDT-PERP"


def metadata(at: datetime, sequence: int | None = None) -> dict[str, object]:
    return {
        "exchange": Exchange.BINANCE,
        "symbol": SYMBOL,
        "exchange_timestamp": at,
        "local_receive_timestamp": at + timedelta(milliseconds=1),
        "normalization_timestamp": at + timedelta(milliseconds=2),
        "sequence_id": sequence,
    }


def settings() -> Settings:
    base = load_settings(environ={})
    features = base.features.model_copy(
        update={
            "large_trade_notional_usdt": Decimal("150"),
            "depth_walk_notional_usdt": Decimal("100"),
        }
    )
    return base.model_copy(update={"features": features})


def trade(
    seconds_before: float,
    *,
    sequence: int,
    price: str,
    quantity: str,
    side: AggressorSide,
) -> Trade:
    return Trade(
        **metadata(NOW - timedelta(seconds=seconds_before), sequence),
        trade_id=str(sequence),
        price=Decimal(price),
        quantity=Decimal(quantity),
        aggressor_side=side,
    )


def populated_store(config: Settings) -> MarketStateStore:
    store = MarketStateStore(config.features)
    store.ingest(
        OrderBookSnapshot(
            **metadata(NOW - timedelta(seconds=10), 100),
            bids=[
                OrderBookLevel(price=Decimal("99"), quantity=Decimal("2")),
                OrderBookLevel(price=Decimal("98"), quantity=Decimal("3")),
            ],
            asks=[
                OrderBookLevel(price=Decimal("101"), quantity=Decimal("3")),
                OrderBookLevel(price=Decimal("102"), quantity=Decimal("4")),
            ],
            depth=2,
            generation=1,
        )
    )
    store.ingest(
        trade(
            7,
            sequence=1,
            price="100",
            quantity="1",
            side=AggressorSide.BUY,
        )
    )
    store.ingest(
        OpenInterest(
            **metadata(NOW - timedelta(seconds=5), 2),
            open_interest_contracts=Decimal("100"),
            open_interest_base=Decimal("100"),
        )
    )
    store.ingest(
        trade(
            4,
            sequence=3,
            price="100",
            quantity="2",
            side=AggressorSide.BUY,
        )
    )
    store.ingest(
        trade(
            2,
            sequence=4,
            price="101",
            quantity="1",
            side=AggressorSide.SELL,
        )
    )
    store.ingest(
        OrderBookUpdate(
            **metadata(NOW - timedelta(seconds=1.8), 101),
            bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("4"))],
            asks=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("1"))],
            previous_sequence_id=100,
            generation=1,
        )
    )
    store.ingest(
        LiquidationEvent(
            **metadata(NOW - timedelta(seconds=1.5), 5),
            liquidation_id="long",
            position_side=LiquidatedPositionSide.LONG,
            price=Decimal("100"),
            quantity=Decimal("2"),
        )
    )
    store.ingest(
        LiquidationEvent(
            **metadata(NOW - timedelta(seconds=1.4), 6),
            liquidation_id="short",
            position_side=LiquidatedPositionSide.SHORT,
            price=Decimal("101"),
            quantity=Decimal("1"),
        )
    )
    store.ingest(
        OpenInterest(
            **metadata(NOW - timedelta(seconds=1.3), 7),
            open_interest_contracts=Decimal("110"),
            open_interest_base=Decimal("110"),
        )
    )
    store.ingest(
        FundingRate(
            **metadata(NOW - timedelta(seconds=1.2), 8),
            funding_rate=Decimal("0.001"),
        )
    )
    store.ingest(
        IndexPrice(
            **metadata(NOW - timedelta(seconds=1.1), 9),
            index_price=Decimal("100"),
        )
    )
    store.ingest(
        MarkPrice(
            **metadata(NOW - timedelta(seconds=1), 10),
            mark_price=Decimal("102"),
        )
    )
    return store


def test_calculates_single_venue_feature_families() -> None:
    config = settings()
    store = populated_store(config)

    result = SingleVenueFeatureEngine(config).calculate(
        store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.trade_flow is not None
    assert result.trade_flow.aggressive_buy_notional == Decimal("200")
    assert result.trade_flow.aggressive_sell_notional == Decimal("101")
    assert result.trade_flow.taker_imbalance == pytest.approx(99 / 301)
    assert result.trade_flow.trade_notional_impulse == pytest.approx(2.01)
    assert result.trade_flow.trade_count_impulse == pytest.approx(1)
    assert result.trade_flow.average_trade_notional == Decimal("150.5")
    assert result.trade_flow.large_trade_share == pytest.approx(200 / 301)

    assert result.order_book is not None
    assert result.order_book.weighted_bid_depth == Decimal("6.4")
    assert result.order_book.weighted_ask_depth == Decimal("4.2")
    assert result.order_book.bid_liquidity_change == Decimal("2")
    assert result.order_book.ask_liquidity_change == Decimal("-2")
    assert result.order_book.added_liquidity_quantity == Decimal("2")
    assert result.order_book.removed_liquidity_quantity == Decimal("2")
    assert result.order_book.liquidity_recovery_quantity_per_second == pytest.approx(0)
    assert result.order_book.depth_imbalance == pytest.approx(2.2 / 10.6)
    assert result.order_book.spread == Decimal("2")
    assert result.order_book.relative_spread == pytest.approx(0.02)
    assert result.order_book.mid_price == Decimal("100")
    assert result.order_book.microprice == Decimal("100.6")
    assert result.order_book.buy_slippage_bps == pytest.approx(100)
    assert result.order_book.sell_slippage_bps == pytest.approx(100)
    assert result.order_book.order_flow_imbalance == pytest.approx(4)

    assert result.price is not None
    assert result.price.return_value == pytest.approx(0.02)
    assert result.price.trailing_high == Decimal("102")
    assert result.price.trailing_low == Decimal("100")
    assert result.open_interest is not None
    assert result.open_interest.change == Decimal("10")
    assert result.open_interest.percentage_change == pytest.approx(0.1)
    assert result.open_interest.data_age_ms == pytest.approx(1300)
    assert result.open_interest.price_oi_state is PriceOpenInterestState.PRICE_UP_OI_UP
    assert result.crowding is not None
    assert result.crowding.funding_rate == Decimal("0.001")
    assert result.crowding.mark_index_premium == pytest.approx(0.02)
    assert result.crowding.taker_bias == pytest.approx(99 / 301)
    assert result.crowding.joint_state is CrowdingState.CROWDED_LONG
    assert result.liquidation is not None
    assert result.liquidation.public_sample_long_notional == Decimal("200")
    assert result.liquidation.public_sample_short_notional == Decimal("101")
    assert result.liquidation.activity_with_oi_decline is False


def test_same_decision_is_deterministic_and_excludes_future_non_book_events() -> None:
    config = settings()
    store = populated_store(config)
    state = store.state(Exchange.BINANCE, SYMBOL)
    engine = SingleVenueFeatureEngine(config)
    before = engine.calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    store.ingest(
        trade(
            -1,
            sequence=11,
            price="1000",
            quantity="100",
            side=AggressorSide.SELL,
        )
    )
    store.ingest(
        OpenInterest(
            **metadata(NOW + timedelta(seconds=1), 12),
            open_interest_contracts=Decimal("9999"),
            open_interest_base=Decimal("9999"),
        )
    )
    after = engine.calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert after.feature_snapshot_id == before.feature_snapshot_id
    assert after.trade_flow == before.trade_flow
    assert after.price == before.price
    assert after.open_interest == before.open_interest
    assert after.source_event_count == before.source_event_count
    assert after.newest_source_timestamp == before.newest_source_timestamp


def test_book_recovery_rate_uses_additions_after_first_removal() -> None:
    config = settings()
    store = populated_store(config)
    state = store.state(Exchange.BINANCE, SYMBOL)
    store.ingest(
        OrderBookUpdate(
            **metadata(NOW - timedelta(seconds=0.5), 102),
            bids=[OrderBookLevel(price=Decimal("98"), quantity=Decimal("4"))],
            asks=[OrderBookLevel(price=Decimal("102"), quantity=Decimal("5"))],
            previous_sequence_id=101,
            generation=1,
        )
    )

    result = SingleVenueFeatureEngine(config).calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.order_book is not None
    assert result.order_book.added_liquidity_quantity == Decimal("4")
    assert result.order_book.removed_liquidity_quantity == Decimal("2")
    assert result.order_book.liquidity_recovery_quantity_per_second == pytest.approx(
        2 / 1.8
    )


def test_declining_oi_is_combined_with_price_and_liquidation_activity() -> None:
    config = settings()
    store = populated_store(config)
    state = store.state(Exchange.BINANCE, SYMBOL)
    store.ingest(
        OpenInterest(
            **metadata(NOW - timedelta(seconds=0.5), 11),
            open_interest_contracts=Decimal("90"),
            open_interest_base=Decimal("90"),
        )
    )

    result = SingleVenueFeatureEngine(config).calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.open_interest is not None
    assert (
        result.open_interest.price_oi_state
        is PriceOpenInterestState.PRICE_UP_OI_DOWN
    )
    assert result.liquidation is not None
    assert result.liquidation.activity_with_oi_decline is True
    assert result.crowding is not None
    assert result.crowding.joint_state is CrowdingState.MIXED


def test_future_book_state_is_explicitly_unavailable() -> None:
    config = settings()
    store = populated_store(config)
    state = store.state(Exchange.BINANCE, SYMBOL)
    store.ingest(
        OrderBookUpdate(
            **metadata(NOW + timedelta(seconds=1), 102),
            bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("5"))],
            previous_sequence_id=101,
            generation=1,
        )
    )

    result = SingleVenueFeatureEngine(config).calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.order_book is None
    assert not result.is_healthy
    assert any(
        reason.channel == "order_book"
        and "after the decision boundary" in reason.detail
        for reason in result.unavailable_reasons
    )


def test_zero_volume_window_is_unavailable_instead_of_fabricated_zero() -> None:
    config = settings()
    state = populated_store(config).state(Exchange.BINANCE, SYMBOL)

    result = SingleVenueFeatureEngine(config).calculate(
        state,
        decision_timestamp=NOW + timedelta(seconds=10),
        window_seconds=5,
    )

    assert result.trade_flow is not None
    assert result.trade_flow.taker_imbalance is None
    assert not result.is_warm
    assert FeatureUnavailableCode.NO_TRADES in {
        reason.code for reason in result.unavailable_reasons
    }


def test_calculate_all_emits_every_configured_window() -> None:
    config = settings()
    state = populated_store(config).state(Exchange.BINANCE, SYMBOL)

    results = SingleVenueFeatureEngine(config).calculate_all(
        [state],
        decision_timestamp=NOW,
    )

    assert [result.window_seconds for result in results] == [5, 15, 60]
    assert len({result.feature_snapshot_id for result in results}) == 3


def test_rejects_unconfigured_window_and_naive_decision_time() -> None:
    config = settings()
    state = populated_store(config).state(Exchange.BINANCE, SYMBOL)
    engine = SingleVenueFeatureEngine(config)

    with pytest.raises(ValueError, match="configured feature window"):
        engine.calculate(state, decision_timestamp=NOW, window_seconds=10)
    with pytest.raises(ValueError, match="timezone-aware"):
        engine.calculate(
            state,
            decision_timestamp=NOW.replace(tzinfo=None),
            window_seconds=5,
        )

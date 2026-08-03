"""Deterministic single-venue feature calculations and decision-time boundaries."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

import cvf.features.single_venue as single_venue_module
from cvf.config import Settings, load_settings
from cvf.features import (
    CrowdingState,
    FeatureUnavailableCode,
    MarketStateStore,
    PriceOpenInterestState,
    SingleVenueFeatureEngine,
)
from cvf.features.models import FeatureSnapshot
from cvf.features.single_venue import _MetricHistory
from cvf.models import (
    AggressorSide,
    Exchange,
    ExchangeHealth,
    FundingRate,
    HealthStatus,
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


def test_price_cache_preserves_open_left_boundary_and_return_pairs() -> None:
    config = settings()
    store = MarketStateStore(config.features)
    for seconds_before, sequence, price in (
        (6, 1, "10"),
        (5, 2, "1000"),
        (4, 3, "100"),
        (3, 4, "110"),
        (2, 5, "121"),
    ):
        store.ingest(
            MarkPrice(
                **metadata(
                    NOW - timedelta(seconds=seconds_before),
                    sequence,
                ),
                mark_price=Decimal(price),
            )
        )

    result = SingleVenueFeatureEngine(config).calculate(
        store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.price is not None
    assert result.price.return_value == pytest.approx(0.21)
    expected = math.sqrt(2 * math.log(1.1) ** 2)
    assert result.price.realized_volatility == pytest.approx(expected)


def test_price_sources_are_hashed_once_then_queried_incrementally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = settings()
    store = MarketStateStore(config.features)
    for offset in range(100):
        store.ingest(
            MarkPrice(
                **metadata(
                    NOW - timedelta(seconds=100 - offset),
                    offset + 1,
                ),
                mark_price=Decimal(100 + offset),
            )
        )

    calls = 0
    original = single_venue_module.semantic_source_digest

    def counted_digest(timestamp: datetime, value: object) -> str:
        nonlocal calls
        calls += 1
        return original(timestamp, value)

    monkeypatch.setattr(
        single_venue_module,
        "semantic_source_digest",
        counted_digest,
    )
    engine = SingleVenueFeatureEngine(config)
    state = store.state(Exchange.BINANCE, SYMBOL)

    engine.calculate_all([state], decision_timestamp=NOW)
    assert calls == 100
    engine.calculate_all([state], decision_timestamp=NOW)
    assert calls == 100

    store.ingest(
        MarkPrice(
            **metadata(NOW + timedelta(seconds=1), 101),
            mark_price=Decimal("201"),
        )
    )
    engine.calculate_all(
        [state],
        decision_timestamp=NOW + timedelta(seconds=1),
    )
    assert calls == 101


def test_price_cache_excludes_capacity_evictions_without_full_compaction() -> None:
    base = settings()
    config = base.model_copy(
        update={
            "features": base.features.model_copy(
                update={"maximum_events_per_stream": 3}
            )
        }
    )
    store = MarketStateStore(config.features)
    state = store.state(Exchange.BINANCE, SYMBOL)
    engine = SingleVenueFeatureEngine(config)
    result = None
    for offset in range(5):
        at = NOW + timedelta(seconds=offset)
        store.ingest(
            MarkPrice(
                **metadata(at, offset + 1),
                mark_price=Decimal(100 + offset),
            )
        )
        result = engine.calculate(
            state,
            decision_timestamp=at,
            window_seconds=5,
        )

    assert result is not None
    assert result.price.return_value == pytest.approx(104 / 102 - 1)
    assert result.source_event_count == 3


def test_price_cache_rebuilds_exactly_for_inserted_late_event() -> None:
    base = settings()
    config = base.model_copy(
        update={
            "features": base.features.model_copy(
                update={
                    "late_event_policy": "insert",
                    "maximum_lateness_ms": 5_000,
                }
            )
        }
    )
    store = MarketStateStore(config.features)
    state = store.state(Exchange.BINANCE, SYMBOL)
    engine = SingleVenueFeatureEngine(config)
    store.ingest(
        MarkPrice(
            **metadata(NOW - timedelta(seconds=1), 2),
            mark_price=Decimal("110"),
        )
    )
    engine.calculate(state, decision_timestamp=NOW, window_seconds=5)
    store.ingest(
        MarkPrice(
            **metadata(NOW - timedelta(seconds=2), 1),
            mark_price=Decimal("100"),
        )
    )
    store.ingest(
        MarkPrice(
            **metadata(NOW - timedelta(milliseconds=500), 3),
            mark_price=Decimal("121"),
        )
    )

    result = engine.calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.price.return_value == pytest.approx(0.21)
    assert result.price.realized_volatility == pytest.approx(
        math.sqrt(2 * math.log(1.1) ** 2)
    )
    assert result.source_event_count == 3


def test_price_cache_cold_start_rebuilds_after_inserted_late_event() -> None:
    base = settings()
    config = base.model_copy(
        update={
            "features": base.features.model_copy(
                update={
                    "late_event_policy": "insert",
                    "maximum_lateness_ms": 5_000,
                }
            )
        }
    )
    store = MarketStateStore(config.features)
    state = store.state(Exchange.BINANCE, SYMBOL)
    engine = SingleVenueFeatureEngine(config)
    for offset, sequence, price in (
        (-4, 1, "100"),
        (-3, 2, "110"),
        (-2, 3, "121"),
        (-3.5, 4, "105"),
    ):
        at = NOW + timedelta(seconds=offset)
        store.ingest(
            MarkPrice(
                **metadata(at, sequence),
                mark_price=Decimal(price),
            )
        )

    result = engine.calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.price.return_value == pytest.approx(0.21)
    assert result.source_event_count == 4
    assert result.oldest_source_timestamp == NOW - timedelta(seconds=4)
    assert result.newest_source_timestamp == NOW - timedelta(seconds=2)


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


def test_snapshot_id_changes_when_non_book_source_content_changes() -> None:
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
            0.5,
            sequence=11,
            price="103",
            quantity="3",
            side=AggressorSide.BUY,
        )
    )
    after = engine.calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert after.source_sequence_id == before.source_sequence_id
    assert after.book_generation == before.book_generation
    assert after.trade_flow != before.trade_flow
    assert after.feature_snapshot_id != before.feature_snapshot_id
    assert after.raw_payload_reference != before.raw_payload_reference


def test_snapshot_id_ignores_nondeterministic_normalization_timestamp() -> None:
    config = settings()
    source = trade(
        1,
        sequence=11,
        price="103",
        quantity="3",
        side=AggressorSide.BUY,
    )
    first_store = MarketStateStore(config.features)
    second_store = MarketStateStore(config.features)
    first_store.ingest(source)
    second_store.ingest(
        source.model_copy(
            update={
                "normalization_timestamp": (
                    source.normalization_timestamp + timedelta(seconds=1)
                )
            }
        )
    )

    first = SingleVenueFeatureEngine(config).calculate(
        first_store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )
    second = SingleVenueFeatureEngine(config).calculate(
        second_store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert first.feature_snapshot_id == second.feature_snapshot_id
    assert first.raw_payload_reference == second.raw_payload_reference


def test_single_venue_id_binds_feature_configuration() -> None:
    base = settings()
    changed = base.model_copy(
        update={
            "features": base.features.model_copy(
                update={"cross_venue_max_snapshot_age_ms": 2_001}
            )
        }
    )
    state = populated_store(base).state(Exchange.BINANCE, SYMBOL)

    first = SingleVenueFeatureEngine(base).calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )
    second = SingleVenueFeatureEngine(changed).calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert first.trade_flow == second.trade_flow
    assert first.feature_snapshot_id != second.feature_snapshot_id


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


@pytest.mark.parametrize(
    "future_book_event",
    ("same_generation_update", "next_generation_snapshot", "next_generation_update"),
)
def test_future_book_event_cannot_change_an_earlier_decision(
    future_book_event: str,
) -> None:
    config = settings()
    baseline_store = populated_store(config)
    future_store = populated_store(config)
    baseline_state = baseline_store.state(Exchange.BINANCE, SYMBOL)
    future_state = future_store.state(Exchange.BINANCE, SYMBOL)
    baseline_engine = SingleVenueFeatureEngine(config)
    future_engine = SingleVenueFeatureEngine(config)

    seed_decision = NOW - timedelta(milliseconds=500)
    assert baseline_engine.calculate(
        baseline_state,
        decision_timestamp=seed_decision,
        window_seconds=5,
    ) == future_engine.calculate(
        future_state,
        decision_timestamp=seed_decision,
        window_seconds=5,
    )

    if future_book_event == "next_generation_snapshot":
        event = OrderBookSnapshot(
            **metadata(NOW + timedelta(seconds=1), 200),
            bids=[OrderBookLevel(price=Decimal("98"), quantity=Decimal("8"))],
            asks=[OrderBookLevel(price=Decimal("102"), quantity=Decimal("9"))],
            depth=1,
            generation=2,
        )
    else:
        next_generation = future_book_event == "next_generation_update"
        event = OrderBookUpdate(
            **metadata(
                NOW + timedelta(seconds=1),
                200 if next_generation else 102,
            ),
            bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("9"))],
            previous_sequence_id=199 if next_generation else 101,
            generation=2 if next_generation else 1,
        )
    future_store.ingest(event)

    for decision in (NOW, NOW + timedelta(milliseconds=500)):
        baseline = baseline_engine.calculate(
            baseline_state,
            decision_timestamp=decision,
            window_seconds=5,
        )
        after_future_ingest = future_engine.calculate(
            future_state,
            decision_timestamp=decision,
            window_seconds=5,
        )

        assert after_future_ingest == baseline
        assert (
            after_future_ingest.feature_snapshot_id
            == baseline.feature_snapshot_id
        )
        assert (
            after_future_ingest.source_sequence_id
            == baseline.source_sequence_id
        )
        assert after_future_ingest.book_generation == baseline.book_generation
        assert after_future_ingest.order_book == baseline.order_book
        assert {
            key: tuple((item.timestamp, item.value) for item in values)
            for key, values in future_engine._history._windows.items()
        } == {
            key: tuple((item.timestamp, item.value) for item in values)
            for key, values in baseline_engine._history._windows.items()
        }
        assert future_engine._history._last == baseline_engine._history._last
        assert future_engine._book_generations == baseline_engine._book_generations


def test_future_health_event_cannot_change_an_earlier_decision() -> None:
    config = settings()
    baseline_store = populated_store(config)
    future_store = populated_store(config)
    future_store.ingest(
        ExchangeHealth(
            **metadata(NOW + timedelta(seconds=1), 500),
            channel="trades",
            status=HealthStatus.STALE,
            is_connected=True,
        )
    )

    baseline = SingleVenueFeatureEngine(config).calculate(
        baseline_store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )
    after_future_health = SingleVenueFeatureEngine(config).calculate(
        future_store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert after_future_health == baseline


def test_health_gate_event_is_part_of_feature_lineage() -> None:
    config = settings()
    baseline_store = populated_store(config)
    blocked_store = populated_store(config)
    blocked_store.ingest(
        ExchangeHealth(
            **metadata(NOW - timedelta(seconds=1), 501),
            channel="trades",
            status=HealthStatus.STALE,
            is_connected=True,
        )
    )

    baseline = SingleVenueFeatureEngine(config).calculate(
        baseline_store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )
    blocked = SingleVenueFeatureEngine(config).calculate(
        blocked_store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert not blocked.is_healthy
    assert FeatureUnavailableCode.HEALTH_BLOCKED in {
        reason.code for reason in blocked.unavailable_reasons
    }
    assert blocked.source_event_count == baseline.source_event_count + 1
    assert blocked.feature_snapshot_id != baseline.feature_snapshot_id


def test_lineage_includes_old_latest_inputs_used_by_formulas() -> None:
    config = settings()
    store = MarketStateStore(config.features)
    old = NOW - timedelta(seconds=10)
    store.ingest(
        OpenInterest(
            **metadata(old, 1),
            open_interest_contracts=Decimal("123"),
            open_interest_base=Decimal("123"),
        )
    )
    store.ingest(
        FundingRate(
            **metadata(old, 2),
            funding_rate=Decimal("0.001"),
        )
    )
    store.ingest(
        MarkPrice(
            **metadata(old, 3),
            mark_price=Decimal("101"),
        )
    )
    store.ingest(
        IndexPrice(
            **metadata(old, 4),
            index_price=Decimal("100"),
        )
    )

    result = SingleVenueFeatureEngine(config).calculate(
        store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.open_interest is not None
    assert result.open_interest.change == Decimal(0)
    assert result.crowding is not None
    assert result.crowding.funding_rate == Decimal("0.001")
    assert result.crowding.mark_index_premium == pytest.approx(0.01)
    assert result.source_event_count == 4
    assert result.oldest_source_timestamp == old
    assert result.newest_source_timestamp == old
    assert result.data_age_ms == pytest.approx(10_000)


def test_lineage_includes_book_base_snapshot_outside_feature_window() -> None:
    config = settings()
    store = MarketStateStore(config.features)
    book_at = NOW - timedelta(seconds=10)
    store.ingest(
        OrderBookSnapshot(
            **metadata(book_at, 100),
            bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("2"))],
            asks=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("3"))],
            depth=1,
            generation=1,
        )
    )
    store.ingest(
        trade(
            1,
            sequence=1,
            price="100",
            quantity="1",
            side=AggressorSide.BUY,
        )
    )

    result = SingleVenueFeatureEngine(config).calculate(
        store.state(Exchange.BINANCE, SYMBOL),
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.order_book is not None
    assert result.source_event_count == 2
    assert result.oldest_source_timestamp == book_at


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


def test_empty_snapshot_has_null_data_age_not_fabricated_zero() -> None:
    config = settings()
    state = MarketStateStore(config.features).state(Exchange.BINANCE, SYMBOL)

    result = SingleVenueFeatureEngine(config).calculate(
        state,
        decision_timestamp=NOW,
        window_seconds=5,
    )

    assert result.source_event_count == 0
    assert result.oldest_source_timestamp is None
    assert result.newest_source_timestamp is None
    assert result.data_age_ms is None
    with pytest.raises(ValueError, match="cannot fabricate a data age"):
        FeatureSnapshot.model_validate(
            {
                **result.model_dump(exclude_computed_fields=True),
                "data_age_ms": 0.0,
            }
        )


def test_zero_variance_metric_history_is_unavailable_not_zero() -> None:
    config = settings()
    features = config.features.model_copy(
        update={"zscore_lookback_seconds": 2}
    )
    history = _MetricHistory(config.model_copy(update={"features": features}))
    start = NOW - timedelta(seconds=3)

    history.record("constant", start, 1.0)
    history.record("constant", start + timedelta(seconds=1), 1.0)
    history.record("constant", start + timedelta(seconds=2), 1.0)
    zscore, ready = history.record("constant", NOW, 1.0)

    assert zscore is None
    assert ready is False


def test_metric_history_replaces_changed_value_at_same_decision() -> None:
    history = _MetricHistory(settings())
    history.record("metric", NOW - timedelta(seconds=2), 1.0)
    history.record("metric", NOW - timedelta(seconds=1), 3.0)

    initial, _ = history.record("metric", NOW, 5.0)
    replaced, _ = history.record("metric", NOW, 9.0)
    repeated, _ = history.record("metric", NOW, 9.0)

    assert initial == pytest.approx(3.0)
    assert replaced == pytest.approx(7.0)
    assert repeated == pytest.approx(7.0)
    assert len(history._windows["metric"]) == 3


def test_book_generation_change_clears_derived_metric_history() -> None:
    config = settings()
    store = populated_store(config)
    state = store.state(Exchange.BINANCE, SYMBOL)
    engine = SingleVenueFeatureEngine(config)
    engine.calculate(state, decision_timestamp=NOW, window_seconds=5)
    sentinel = f"{Exchange.BINANCE.value}:{SYMBOL}:5:sentinel"
    engine._history.record(sentinel, NOW, 1.0)
    assert sentinel in engine._history._windows

    store.ingest(
        OrderBookSnapshot(
            **metadata(NOW + timedelta(seconds=1), 200),
            bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("2"))],
            asks=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("2"))],
            depth=1,
            generation=2,
        )
    )
    engine.calculate(
        state,
        decision_timestamp=NOW + timedelta(seconds=1),
        window_seconds=5,
    )

    assert sentinel not in engine._history._windows


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

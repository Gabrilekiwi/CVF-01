"""Bounded feature windows, book generations, and availability gates."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

from cvf.config import load_settings
from cvf.features import (
    AppendStatus,
    BoundedTimeWindow,
    FeatureSnapshot,
    FeatureUnavailableCode,
    FeatureUnavailableReason,
    LateEventPolicy,
    MarketStateStore,
    StateUpdateStatus,
    evaluate_availability,
)
from cvf.models import (
    AggressorSide,
    Exchange,
    HealthStatus,
    OpenInterest,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderBookUpdate,
    Trade,
)

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def trade(at: datetime, sequence: int = 1) -> Trade:
    return Trade(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        exchange_timestamp=at,
        local_receive_timestamp=at + timedelta(milliseconds=1),
        normalization_timestamp=at + timedelta(milliseconds=2),
        sequence_id=sequence,
        raw_payload_reference=f"raw://{sequence:032x}",
        trade_id=str(sequence),
        price=Decimal("100"),
        quantity=Decimal("1"),
        aggressor_side=AggressorSide.BUY,
    )


def snapshot(at: datetime, *, generation: int = 1, sequence: int = 100) -> OrderBookSnapshot:
    return OrderBookSnapshot(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        exchange_timestamp=at,
        local_receive_timestamp=at,
        normalization_timestamp=at,
        sequence_id=sequence,
        bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("2"))],
        asks=[OrderBookLevel(price=Decimal("101"), quantity=Decimal("3"))],
        depth=1,
        generation=generation,
    )


def update(
    at: datetime,
    *,
    generation: int,
    sequence: int,
    previous: int,
) -> OrderBookUpdate:
    return OrderBookUpdate(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        exchange_timestamp=at,
        local_receive_timestamp=at,
        normalization_timestamp=at,
        sequence_id=sequence,
        bids=[OrderBookLevel(price=Decimal("99"), quantity=Decimal("4"))],
        previous_sequence_id=previous,
        generation=generation,
    )


def small_store(*, maximum_items: int = 3) -> MarketStateStore:
    config = load_settings(environ={}).features.model_copy(
        update={
            "maximum_events_per_stream": maximum_items,
            "late_event_policy": "drop",
        }
    )
    return MarketStateStore(config)


def test_window_is_right_closed_time_bounded_and_capacity_bounded() -> None:
    window = BoundedTimeWindow[int](
        retention=timedelta(seconds=5),
        maximum_items=3,
    )
    for offset in range(5):
        assert window.append(NOW + timedelta(seconds=offset), offset) is AppendStatus.ACCEPTED

    assert len(window) == 3
    assert window.stats.capacity_evictions == 2
    assert window.values_between(
        NOW + timedelta(seconds=2),
        NOW + timedelta(seconds=4),
    ) == [3, 4]

    window.append(NOW + timedelta(seconds=8), 8)
    assert [item.value for item in window] == [4, 8]


def test_late_event_policy_is_explicit_and_configurable() -> None:
    dropped = BoundedTimeWindow[int](
        retention=timedelta(seconds=10),
        maximum_items=10,
        late_event_policy=LateEventPolicy.DROP,
        maximum_lateness=timedelta(seconds=2),
    )
    dropped.append(NOW + timedelta(seconds=2), 2)
    assert dropped.append(NOW + timedelta(seconds=1), 1) is AppendStatus.LATE_DROPPED

    inserted = BoundedTimeWindow[int](
        retention=timedelta(seconds=10),
        maximum_items=10,
        late_event_policy=LateEventPolicy.INSERT,
        maximum_lateness=timedelta(seconds=2),
    )
    inserted.append(NOW + timedelta(seconds=2), 2)
    assert inserted.append(NOW + timedelta(seconds=1), 1) is AppendStatus.ACCEPTED
    assert [item.value for item in inserted] == [1, 2]
    assert (
        inserted.append(NOW - timedelta(seconds=1), -1)
        is AppendStatus.LATE_DROPPED
    )


def test_market_state_is_isolated_and_hard_capped() -> None:
    store = small_store(maximum_items=3)
    for sequence in range(5):
        result = store.ingest(trade(NOW + timedelta(seconds=sequence), sequence))
        assert result.status is StateUpdateStatus.ACCEPTED

    state = store.state(Exchange.BINANCE, "BTC-USDT-PERP")
    assert len(state.trades) == 3
    assert len(state.prices) == 3
    assert state.trades.stats.capacity_evictions == 2
    assert state.accepted_events == 5
    assert store.state(Exchange.OKX, "BTC-USDT-PERP") is not state


def test_book_buffers_until_snapshot_and_generation_change_restarts_warmup() -> None:
    store = small_store()
    buffered = store.ingest(
        update(
            NOW + timedelta(seconds=1),
            generation=1,
            sequence=101,
            previous=100,
        )
    )
    assert buffered.status is StateUpdateStatus.BUFFERED_FOR_SNAPSHOT

    installed = store.ingest(snapshot(NOW, generation=1, sequence=100))
    assert installed.status is StateUpdateStatus.ACCEPTED
    state = store.state(Exchange.BINANCE, "BTC-USDT-PERP")
    view = state.order_book.view(depth=1)
    assert view.synchronized
    assert view.sequence_id == 101
    assert view.bids[0].quantity == 4
    assert len(state.book_updates) == 1

    changed = store.ingest(
        update(
            NOW + timedelta(seconds=2),
            generation=2,
            sequence=200,
            previous=199,
        )
    )
    assert changed.status is StateUpdateStatus.BUFFERED_FOR_SNAPSHOT
    assert not state.order_book.view(depth=1).synchronized
    assert len(state.book_updates) == 0


def test_availability_distinguishes_missing_warmup_and_health() -> None:
    store = small_store()
    state = store.state(Exchange.BINANCE, "BTC-USDT-PERP")
    empty = evaluate_availability(
        state,
        decision_timestamp=NOW,
        warmup=timedelta(seconds=5),
        open_interest_stale_after=timedelta(seconds=15),
        blocked_health_statuses=frozenset(
            {HealthStatus.STALE, HealthStatus.RESYNCING, HealthStatus.DISCONNECTED}
        ),
    )
    codes = {reason.code for reason in empty.reasons}
    assert FeatureUnavailableCode.NO_TRADES in codes
    assert FeatureUnavailableCode.BOOK_UNSYNCHRONIZED in codes
    assert FeatureUnavailableCode.OPEN_INTEREST_MISSING in codes
    assert not empty.is_warm
    assert not empty.is_healthy

    store.ingest(trade(NOW - timedelta(seconds=10)))
    store.ingest(snapshot(NOW - timedelta(seconds=10)))
    store.ingest(
        OpenInterest(
            exchange=Exchange.BINANCE,
            symbol="BTC-USDT-PERP",
            exchange_timestamp=NOW - timedelta(seconds=20),
            local_receive_timestamp=NOW - timedelta(seconds=20),
            normalization_timestamp=NOW - timedelta(seconds=20),
            open_interest_contracts=Decimal("100"),
        )
    )
    stale = evaluate_availability(
        state,
        decision_timestamp=NOW,
        warmup=timedelta(seconds=5),
        open_interest_stale_after=timedelta(seconds=15),
        blocked_health_statuses=frozenset(),
    )
    assert stale.is_warm
    assert not stale.is_healthy
    assert FeatureUnavailableCode.OPEN_INTEREST_STALE in {
        reason.code for reason in stale.reasons
    }
    state.health_by_channel["trades"] = HealthStatus.STALE
    blocked = evaluate_availability(
        state,
        decision_timestamp=NOW,
        warmup=timedelta(seconds=5),
        open_interest_stale_after=timedelta(seconds=15),
        blocked_health_statuses=frozenset({HealthStatus.STALE}),
    )
    assert FeatureUnavailableCode.HEALTH_BLOCKED in {
        reason.code for reason in blocked.reasons
    }


def test_feature_snapshot_rejects_future_sources_and_implicit_unavailability() -> None:
    reason = FeatureUnavailableReason(
        code=FeatureUnavailableCode.NOT_WARM,
        detail="window is not warm",
    )
    snapshot_model = FeatureSnapshot(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        exchange_timestamp=NOW,
        local_receive_timestamp=NOW,
        normalization_timestamp=NOW + timedelta(milliseconds=1),
        strategy_version="0.1.0",
        calculation_timestamp=NOW + timedelta(milliseconds=1),
        decision_timestamp=NOW,
        window_seconds=5,
        book_generation=1,
        source_event_count=1,
        oldest_source_timestamp=NOW - timedelta(seconds=1),
        newest_source_timestamp=NOW,
        data_age_ms=0,
        is_warm=False,
        is_healthy=True,
        unavailable_reasons=(reason,),
    )
    assert snapshot_model.schema_version == 1
    source = snapshot_model.model_dump(
        exclude={"feature_snapshot_id", "receive_latency_ms"}
    )

    with pytest.raises(ValidationError, match="future source"):
        FeatureSnapshot(
            **{key: value for key, value in source.items() if key != "newest_source_timestamp"},
            newest_source_timestamp=NOW + timedelta(microseconds=1),
        )

    with pytest.raises(ValidationError, match="structured reasons"):
        FeatureSnapshot(
            **{key: value for key, value in source.items() if key != "unavailable_reasons"},
            unavailable_reasons=(),
        )

"""Health transitions, counters, stream isolation, and bounded deduplication."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from cvf.exchanges import BoundedTTLDeduplicator
from cvf.models import Exchange, HealthStatus
from cvf.monitoring import StreamHealthRegistry, StreamKey, estimate_clock_skew_ms

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def registry() -> StreamHealthRegistry:
    return StreamHealthRegistry(
        stale_after_ms=1_500,
        maximum_core_latency_ms=500,
        clock_skew_warning_ms=250,
        open_interest_stale_after_ms=5_000,
    )


def record_message(
    health: StreamHealthRegistry,
    key: StreamKey,
    *,
    receive_delay_ms: int = 10,
    clock_skew_ms: float | None = None,
    is_open_interest: bool = False,
) -> None:
    received_at = NOW + timedelta(milliseconds=receive_delay_ms)
    health.record_message(
        key,
        exchange_timestamp=NOW,
        receive_timestamp=received_at,
        normalization_timestamp=received_at + timedelta(milliseconds=2),
        clock_skew_ms=clock_skew_ms,
        is_open_interest=is_open_interest,
    )


def test_health_is_independent_per_exchange_symbol_and_channel() -> None:
    health = registry()
    trades = StreamKey(Exchange.BINANCE, "BTC-USDT-PERP", "trades")
    books = StreamKey(Exchange.BINANCE, "BTC-USDT-PERP", "books")
    okx_trades = StreamKey(Exchange.OKX, "BTC-USDT-PERP", "trades")

    health.mark_connected(trades, at=NOW)
    record_message(health, trades)
    health.record_duplicate(trades)
    health.record_parse_error(trades, error="bad trade")

    assert health.snapshot(trades, now=NOW).message_count == 1
    assert health.snapshot(trades, now=NOW).duplicate_count == 1
    assert health.snapshot(books, now=NOW).message_count == 0
    assert health.snapshot(okx_trades, now=NOW).message_count == 0
    assert health.snapshot(books, now=NOW).status is HealthStatus.DISCONNECTED


def test_health_status_precedence_and_recovery() -> None:
    health = registry()
    key = StreamKey(Exchange.OKX, "ETH-USDT-PERP", "books")
    health.mark_connected(key, at=NOW)

    assert health.snapshot(key, now=NOW).status is HealthStatus.CONNECTED

    record_message(health, key, receive_delay_ms=700)
    assert health.snapshot(key, now=NOW + timedelta(milliseconds=700)).status is (
        HealthStatus.DEGRADED
    )

    record_message(health, key, receive_delay_ms=20)
    assert health.snapshot(key, now=NOW + timedelta(milliseconds=20)).status is (
        HealthStatus.CONNECTED
    )

    health.record_rest_result(key, healthy=False, error="HTTP 503")
    assert health.snapshot(key, now=NOW + timedelta(milliseconds=20)).status is (
        HealthStatus.DEGRADED
    )
    health.record_rest_result(key, healthy=True)

    assert health.snapshot(key, now=NOW + timedelta(seconds=2)).status is HealthStatus.STALE

    health.record_sequence_gap(key, book_generation=1)
    snapshot = health.snapshot(key, now=NOW + timedelta(seconds=2))
    assert snapshot.status is HealthStatus.RESYNCING
    assert snapshot.sequence_gap_count == 1
    assert snapshot.sequence_gap_detected

    health.mark_book_live(key, book_generation=2)
    record_message(health, key, receive_delay_ms=30)
    snapshot = health.snapshot(key, now=NOW + timedelta(milliseconds=30))
    assert snapshot.status is HealthStatus.CONNECTED
    assert not snapshot.sequence_gap_detected
    assert snapshot.book_generation == 2

    health.mark_disconnected(key, error="peer closed")
    snapshot = health.snapshot(key, now=NOW + timedelta(milliseconds=30))
    assert snapshot.status is HealthStatus.DISCONNECTED
    assert snapshot.last_error == "peer closed"


def test_health_counters_latency_skew_and_exchange_event() -> None:
    health = registry()
    key = StreamKey(Exchange.BINANCE, "BTC-USDT-PERP", "depth")
    health.mark_connected(key, at=NOW)
    record_message(health, key, receive_delay_ms=310, clock_skew_ms=300)
    record_message(health, key, receive_delay_ms=50, clock_skew_ms=10)
    health.record_reconnect(key, error="wire lost")
    health.record_resubscribe(key)
    health.record_checksum_failure(key, book_generation=3)
    health.record_parse_error(key, error="invalid decimal")
    health.record_drop(key, backpressure=True)
    health.record_duplicate(key)

    snapshot = health.snapshot(key, now=NOW + timedelta(milliseconds=50))
    assert snapshot.message_count == 2
    assert snapshot.last_latency_ms == 40
    assert snapshot.average_latency_ms == 25
    assert snapshot.maximum_latency_ms == 40
    assert snapshot.last_normalization_latency_ms == 2
    assert snapshot.reconnect_count == 1
    assert snapshot.resubscribe_count == 1
    assert snapshot.checksum_failure_count == 1
    assert snapshot.parse_error_count == 1
    assert snapshot.dropped_event_count == 1
    assert snapshot.backpressure_count == 1

    event = health.exchange_health(key, now=NOW + timedelta(milliseconds=50))
    assert event.channel == "depth"
    assert event.status is HealthStatus.RESYNCING
    assert event.messages_received == 2
    assert event.average_latency_ms == 25
    assert event.checksum_failures == 1
    assert event.backpressure_events == 1


def test_open_interest_staleness_has_its_own_threshold() -> None:
    health = registry()
    key = StreamKey(Exchange.OKX, "BTC-USDT-PERP", "open_interest")
    health.mark_connected(key, at=NOW)

    assert health.snapshot(key, now=NOW).status is HealthStatus.STALE
    record_message(health, key, is_open_interest=True)
    assert health.snapshot(key, now=NOW + timedelta(seconds=4)).status is HealthStatus.CONNECTED
    assert health.snapshot(key, now=NOW + timedelta(seconds=6)).status is HealthStatus.STALE
    assert health.snapshot(
        key,
        now=NOW + timedelta(milliseconds=10),
    ).status is HealthStatus.CONNECTED


def test_event_driven_channel_can_disable_inactivity_staleness() -> None:
    health = StreamHealthRegistry(
        stale_after_ms=1_500,
        maximum_core_latency_ms=500,
        clock_skew_warning_ms=250,
        channel_stale_after_ms={"liquidation-orders": None},
    )
    key = StreamKey(Exchange.OKX, "BTC-USDT-PERP", "liquidation-orders")
    health.mark_connected(key, at=NOW)

    assert health.snapshot(key, now=NOW + timedelta(days=1)).status is (
        HealthStatus.CONNECTED
    )


def test_deduplicator_refreshes_ttl_and_evicts_lru_at_capacity() -> None:
    dedupe = BoundedTTLDeduplicator(capacity=2, ttl_seconds=1.0)

    assert not dedupe.seen_or_add(("trade", "1"), now=0.0)
    assert dedupe.seen_or_add(("trade", "1"), now=0.5)
    assert not dedupe.seen_or_add(("trade", "2"), now=0.6)
    assert not dedupe.seen_or_add(("trade", "3"), now=0.7)
    assert dedupe.size == 2
    assert not dedupe.seen_or_add(("trade", "1"), now=0.8)
    assert dedupe.size == 2


def test_deduplicator_expires_entries_and_rejects_backwards_time() -> None:
    dedupe = BoundedTTLDeduplicator(capacity=10, ttl_seconds=1.0)

    assert not dedupe.seen_or_add("event", now=2.0)
    assert not dedupe.seen_or_add("event", now=3.0)
    with pytest.raises(ValueError, match="cannot move backwards"):
        dedupe.seen_or_add("other", now=2.9)


def test_clock_skew_uses_rest_round_trip_midpoint() -> None:
    assert estimate_clock_skew_ms(
        request_sent_at=NOW,
        response_received_at=NOW + timedelta(milliseconds=100),
        exchange_timestamp=NOW + timedelta(milliseconds=30),
    ) == 20

    with pytest.raises(ValueError, match="cannot precede"):
        estimate_clock_skew_ms(
            request_sent_at=NOW,
            response_received_at=NOW - timedelta(milliseconds=1),
            exchange_timestamp=NOW,
        )

"""Per-exchange, symbol, and channel market-data health accounting."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from cvf.models.enums import Exchange, HealthStatus
from cvf.models.market import ExchangeHealth
from cvf.utils.validation import validate_canonical_symbol


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("health timestamps must be timezone-aware")
    return value.astimezone(UTC)


def estimate_clock_skew_ms(
    *,
    request_sent_at: datetime,
    response_received_at: datetime,
    exchange_timestamp: datetime,
) -> float:
    """Estimate local-minus-exchange clock offset at the REST round-trip midpoint."""

    sent_at = _aware_utc(request_sent_at)
    received_at = _aware_utc(response_received_at)
    exchange_at = _aware_utc(exchange_timestamp)
    if received_at < sent_at:
        raise ValueError("response timestamp cannot precede request timestamp")
    midpoint = sent_at + (received_at - sent_at) / 2
    return (midpoint - exchange_at).total_seconds() * 1000.0


@dataclass(frozen=True, slots=True)
class StreamKey:
    exchange: Exchange
    symbol: str
    channel: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "symbol",
            validate_canonical_symbol(self.symbol, allow_wildcard=True),
        )
        if not self.channel or self.channel.isspace():
            raise ValueError("channel cannot be empty")


@dataclass(frozen=True, slots=True)
class StreamHealthSnapshot:
    key: StreamKey
    status: HealthStatus
    is_connected: bool
    connected_at: datetime | None
    last_event_timestamp: datetime | None
    last_receive_timestamp: datetime | None
    last_latency_ms: float | None
    average_latency_ms: float | None
    maximum_latency_ms: float | None
    last_normalization_latency_ms: float | None
    clock_skew_ms: float | None
    message_count: int
    duplicate_count: int
    sequence_gap_count: int
    checksum_failure_count: int
    reconnect_count: int
    resubscribe_count: int
    parse_error_count: int
    dropped_event_count: int
    backpressure_count: int
    sequence_gap_detected: bool
    resyncing: bool
    book_generation: int
    rest_healthy: bool
    open_interest_stale: bool
    last_error: str | None


@dataclass(slots=True)
class _StreamState:
    is_connected: bool = False
    connected_at: datetime | None = None
    last_event_timestamp: datetime | None = None
    last_receive_timestamp: datetime | None = None
    last_latency_ms: float | None = None
    latency_sum_ms: float = 0.0
    maximum_latency_ms: float | None = None
    last_normalization_latency_ms: float | None = None
    clock_skew_ms: float | None = None
    message_count: int = 0
    duplicate_count: int = 0
    sequence_gap_count: int = 0
    checksum_failure_count: int = 0
    reconnect_count: int = 0
    resubscribe_count: int = 0
    parse_error_count: int = 0
    dropped_event_count: int = 0
    backpressure_count: int = 0
    sequence_gap_detected: bool = False
    resyncing: bool = False
    book_generation: int = 0
    rest_healthy: bool = True
    last_open_interest_receive_timestamp: datetime | None = None
    last_error: str | None = None


@dataclass(slots=True)
class StreamHealthRegistry:
    """Track current state and lifetime counters for independent data streams."""

    stale_after_ms: int
    maximum_core_latency_ms: int
    clock_skew_warning_ms: int
    open_interest_stale_after_ms: int | None = None
    channel_stale_after_ms: dict[str, int | None] = field(default_factory=dict)
    _states: dict[StreamKey, _StreamState] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        for name, value in (
            ("stale_after_ms", self.stale_after_ms),
            ("maximum_core_latency_ms", self.maximum_core_latency_ms),
            ("clock_skew_warning_ms", self.clock_skew_warning_ms),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.open_interest_stale_after_ms is None:
            self.open_interest_stale_after_ms = self.stale_after_ms
        elif self.open_interest_stale_after_ms <= 0:
            raise ValueError("open_interest_stale_after_ms must be positive")
        for channel, threshold in self.channel_stale_after_ms.items():
            if not channel or channel.isspace():
                raise ValueError("health channel names cannot be empty")
            if threshold is not None and threshold <= 0:
                raise ValueError("channel stale thresholds must be positive or null")

    def _state(self, key: StreamKey) -> _StreamState:
        return self._states.setdefault(key, _StreamState())

    def keys(self) -> tuple[StreamKey, ...]:
        return tuple(self._states)

    def mark_connected(self, key: StreamKey, *, at: datetime) -> None:
        state = self._state(key)
        state.is_connected = True
        state.connected_at = _aware_utc(at)
        state.last_error = None

    def mark_disconnected(
        self,
        key: StreamKey,
        *,
        error: str | None = None,
    ) -> None:
        state = self._state(key)
        state.is_connected = False
        state.resyncing = False
        state.last_error = error

    def record_reconnect(self, key: StreamKey, *, error: str | None = None) -> None:
        state = self._state(key)
        state.reconnect_count += 1
        state.last_error = error

    def record_resubscribe(self, key: StreamKey) -> None:
        self._state(key).resubscribe_count += 1

    def record_message(
        self,
        key: StreamKey,
        *,
        exchange_timestamp: datetime,
        receive_timestamp: datetime,
        normalization_timestamp: datetime,
        clock_skew_ms: float | None = None,
        is_open_interest: bool = False,
    ) -> None:
        state = self._state(key)
        exchange_at = _aware_utc(exchange_timestamp)
        received_at = _aware_utc(receive_timestamp)
        normalized_at = _aware_utc(normalization_timestamp)
        if normalized_at < received_at:
            raise ValueError("normalization timestamp cannot precede receipt")
        observed_latency = (received_at - exchange_at).total_seconds() * 1000.0
        adjusted_latency = observed_latency
        if clock_skew_ms is not None:
            adjusted_latency -= clock_skew_ms
            state.clock_skew_ms = clock_skew_ms
        state.last_event_timestamp = exchange_at
        state.last_receive_timestamp = received_at
        state.last_latency_ms = adjusted_latency
        state.latency_sum_ms += adjusted_latency
        state.maximum_latency_ms = (
            adjusted_latency
            if state.maximum_latency_ms is None
            else max(state.maximum_latency_ms, adjusted_latency)
        )
        state.last_normalization_latency_ms = (
            normalized_at - received_at
        ).total_seconds() * 1000.0
        state.message_count += 1
        if is_open_interest:
            state.last_open_interest_receive_timestamp = received_at

    def record_duplicate(self, key: StreamKey) -> None:
        self._state(key).duplicate_count += 1

    def mark_resyncing(
        self,
        key: StreamKey,
        *,
        book_generation: int,
        reason: str,
    ) -> None:
        if book_generation < 0:
            raise ValueError("book_generation cannot be negative")
        state = self._state(key)
        state.resyncing = True
        state.book_generation = book_generation
        state.last_error = reason

    def record_sequence_gap(self, key: StreamKey, *, book_generation: int) -> None:
        state = self._state(key)
        state.sequence_gap_count += 1
        state.sequence_gap_detected = True
        self.mark_resyncing(
            key,
            book_generation=book_generation,
            reason="order-book sequence gap",
        )

    def record_checksum_failure(self, key: StreamKey, *, book_generation: int) -> None:
        state = self._state(key)
        state.checksum_failure_count += 1
        self.mark_resyncing(
            key,
            book_generation=book_generation,
            reason="order-book checksum mismatch",
        )

    def mark_book_live(self, key: StreamKey, *, book_generation: int) -> None:
        if book_generation < 0:
            raise ValueError("book_generation cannot be negative")
        state = self._state(key)
        state.resyncing = False
        state.sequence_gap_detected = False
        state.book_generation = book_generation
        state.last_error = None

    def record_parse_error(self, key: StreamKey, *, error: str) -> None:
        state = self._state(key)
        state.parse_error_count += 1
        state.last_error = error

    def record_drop(self, key: StreamKey, *, backpressure: bool = False) -> None:
        state = self._state(key)
        state.dropped_event_count += 1
        if backpressure:
            state.backpressure_count += 1
        state.last_error = "event dropped by backpressure" if backpressure else "event dropped"

    def record_rest_result(
        self,
        key: StreamKey,
        *,
        healthy: bool,
        error: str | None = None,
    ) -> None:
        state = self._state(key)
        state.rest_healthy = healthy
        if not healthy:
            state.last_error = error or "REST health check failed"
        elif state.last_error == "REST health check failed":
            state.last_error = None

    def snapshot(self, key: StreamKey, *, now: datetime) -> StreamHealthSnapshot:
        checked_at = _aware_utc(now)
        state = self._state(key)
        stale_reference = state.last_receive_timestamp or state.connected_at
        oi_threshold = self.open_interest_stale_after_ms
        assert oi_threshold is not None
        is_open_interest_channel = key.channel in {"open_interest", "open-interest"}
        if key.channel in self.channel_stale_after_ms:
            stream_stale_threshold = self.channel_stale_after_ms[key.channel]
        else:
            stream_stale_threshold = (
                oi_threshold if is_open_interest_channel else self.stale_after_ms
            )
        is_stale = (
            state.is_connected
            and stale_reference is not None
            and stream_stale_threshold is not None
            and (checked_at - stale_reference).total_seconds() * 1000.0
            > stream_stale_threshold
        )
        open_interest_stale = is_open_interest_channel and (
            state.last_open_interest_receive_timestamp is None
            or (checked_at - state.last_open_interest_receive_timestamp).total_seconds() * 1000.0
            > oi_threshold
        )
        latency_degraded = (
            state.last_latency_ms is not None
            and state.last_latency_ms > self.maximum_core_latency_ms
        )
        skew_degraded = (
            state.clock_skew_ms is not None
            and abs(state.clock_skew_ms) > self.clock_skew_warning_ms
        )
        if not state.is_connected:
            status = HealthStatus.DISCONNECTED
        elif state.resyncing:
            status = HealthStatus.RESYNCING
        elif is_stale or open_interest_stale:
            status = HealthStatus.STALE
        elif latency_degraded or skew_degraded or not state.rest_healthy:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.CONNECTED
        average_latency = (
            state.latency_sum_ms / state.message_count if state.message_count else None
        )
        return StreamHealthSnapshot(
            key=key,
            status=status,
            is_connected=state.is_connected,
            connected_at=state.connected_at,
            last_event_timestamp=state.last_event_timestamp,
            last_receive_timestamp=state.last_receive_timestamp,
            last_latency_ms=state.last_latency_ms,
            average_latency_ms=average_latency,
            maximum_latency_ms=state.maximum_latency_ms,
            last_normalization_latency_ms=state.last_normalization_latency_ms,
            clock_skew_ms=state.clock_skew_ms,
            message_count=state.message_count,
            duplicate_count=state.duplicate_count,
            sequence_gap_count=state.sequence_gap_count,
            checksum_failure_count=state.checksum_failure_count,
            reconnect_count=state.reconnect_count,
            resubscribe_count=state.resubscribe_count,
            parse_error_count=state.parse_error_count,
            dropped_event_count=state.dropped_event_count,
            backpressure_count=state.backpressure_count,
            sequence_gap_detected=state.sequence_gap_detected,
            resyncing=state.resyncing,
            book_generation=state.book_generation,
            rest_healthy=state.rest_healthy,
            open_interest_stale=open_interest_stale,
            last_error=state.last_error,
        )

    def exchange_health(
        self,
        key: StreamKey,
        *,
        now: datetime,
        raw_payload_reference: str | None = None,
    ) -> ExchangeHealth:
        snapshot = self.snapshot(key, now=now)
        timestamp = _aware_utc(now)
        return ExchangeHealth(
            exchange=key.exchange,
            symbol=key.symbol,
            exchange_timestamp=snapshot.last_event_timestamp or timestamp,
            local_receive_timestamp=timestamp,
            sequence_id=None,
            raw_payload_reference=raw_payload_reference,
            channel=key.channel,
            status=snapshot.status,
            is_connected=snapshot.is_connected,
            last_event_timestamp=snapshot.last_event_timestamp,
            last_latency_ms=snapshot.last_latency_ms,
            average_latency_ms=snapshot.average_latency_ms,
            maximum_latency_ms=snapshot.maximum_latency_ms,
            last_normalization_latency_ms=snapshot.last_normalization_latency_ms,
            clock_skew_ms=snapshot.clock_skew_ms,
            messages_received=snapshot.message_count,
            duplicate_events=snapshot.duplicate_count,
            sequence_gaps=snapshot.sequence_gap_count,
            checksum_failures=snapshot.checksum_failure_count,
            reconnects=snapshot.reconnect_count,
            resubscriptions=snapshot.resubscribe_count,
            parse_errors=snapshot.parse_error_count,
            dropped_events=snapshot.dropped_event_count,
            backpressure_events=snapshot.backpressure_count,
            sequence_gap_detected=snapshot.sequence_gap_detected,
            resyncing=snapshot.resyncing,
            book_generation=snapshot.book_generation,
            rest_healthy=snapshot.rest_healthy,
            open_interest_stale=snapshot.open_interest_stale,
            last_error=snapshot.last_error,
        )

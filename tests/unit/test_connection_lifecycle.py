"""Deterministic tests for reusable public WebSocket session lifecycle."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import pytest

from cvf.exchanges.session import (
    BackoffPolicy,
    ProtocolPingHeartbeat,
    PublicWebSocketSession,
    SessionEvent,
    SessionEventKind,
    TextPingHeartbeat,
    WebSocketTransport,
)

STALL = object()


class FakeTransport:
    def __init__(self, incoming: list[object]) -> None:
        self.incoming = deque(incoming)
        self.sent: list[str] = []
        self.ping_count = 0
        self.closed = False
        self._closed = asyncio.Event()

    async def send(self, message: str) -> None:
        if self.closed:
            raise ConnectionError("transport is closed")
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        if self.incoming:
            item = self.incoming.popleft()
            if isinstance(item, BaseException):
                raise item
            if item is not STALL:
                assert isinstance(item, (str, bytes))
                return item
        await self._closed.wait()
        raise ConnectionError("transport is closed")

    async def ping(self) -> None:
        if self.closed:
            raise ConnectionError("transport is closed")
        self.ping_count += 1

    async def close(self) -> None:
        self.closed = True
        self._closed.set()


class FakeFactory:
    def __init__(self, transports: list[FakeTransport]) -> None:
        self.transports = deque(transports)
        self.calls = 0

    async def __call__(self) -> WebSocketTransport:
        self.calls += 1
        if not self.transports:
            raise ConnectionError("no more fake transports")
        return self.transports.popleft()


def backoff() -> BackoffPolicy:
    return BackoffPolicy(
        initial_seconds=1.0,
        maximum_seconds=8.0,
        jitter_seconds=0.5,
        stable_reset_seconds=30.0,
    )


def fixed_now() -> datetime:
    return datetime(2026, 7, 24, 12, 0, tzinfo=UTC)


def session_for(
    *,
    factory: FakeFactory,
    message_handler: Callable[[str | bytes, datetime], Awaitable[None]],
    heartbeat: ProtocolPingHeartbeat | TextPingHeartbeat | None = None,
    event_handler: Callable[[SessionEvent], Awaitable[None]] | None = None,
    sleep: Callable[[float], Awaitable[None]] | None = None,
) -> PublicWebSocketSession:
    return PublicWebSocketSession(
        connection_factory=factory,
        subscribe_messages=('{"op":"subscribe"}',),
        unsubscribe_messages=('{"op":"unsubscribe"}',),
        message_handler=message_handler,
        heartbeat_policy=heartbeat or ProtocolPingHeartbeat(),
        receive_timeout_seconds=0.01,
        heartbeat_timeout_seconds=0.01,
        backoff_policy=backoff(),
        event_handler=event_handler,
        sleep=sleep or asyncio.sleep,
        utc_now=fixed_now,
        uniform=lambda _low, high: high,
    )


@pytest.mark.asyncio
async def test_subscribes_receives_and_gracefully_unsubscribes() -> None:
    transport = FakeTransport(["payload"])
    received: list[tuple[str | bytes, datetime]] = []
    events: list[SessionEvent] = []
    session: PublicWebSocketSession

    async def handle(message: str | bytes, received_at: datetime) -> None:
        received.append((message, received_at))
        await session.stop()

    async def record(event: SessionEvent) -> None:
        events.append(event)

    session = session_for(
        factory=FakeFactory([transport]),
        message_handler=handle,
        event_handler=record,
    )
    await session.run()

    assert received == [("payload", fixed_now())]
    assert transport.sent == ['{"op":"subscribe"}', '{"op":"unsubscribe"}']
    assert transport.closed
    assert [event.kind for event in events] == [
        SessionEventKind.CONNECTING,
        SessionEventKind.CONNECTED,
        SessionEventKind.SUBSCRIBED,
        SessionEventKind.MESSAGE_RECEIVED,
        SessionEventKind.UNSUBSCRIBED,
        SessionEventKind.CLOSED,
    ]


@pytest.mark.asyncio
async def test_protocol_heartbeat_then_delivers_next_message() -> None:
    transport = FakeTransport([STALL, "after-pong"])
    events: list[SessionEventKind] = []
    session: PublicWebSocketSession

    async def handle(_message: str | bytes, _received_at: datetime) -> None:
        await session.stop()

    async def record(event: SessionEvent) -> None:
        events.append(event.kind)

    session = session_for(
        factory=FakeFactory([transport]),
        message_handler=handle,
        event_handler=record,
    )
    await session.run()

    assert transport.ping_count == 1
    assert SessionEventKind.HEARTBEAT_SENT in events
    assert SessionEventKind.HEARTBEAT_OK in events


@pytest.mark.asyncio
async def test_text_heartbeat_consumes_pong_without_forwarding_it() -> None:
    transport = FakeTransport([STALL, "pong", "market-data"])
    received: list[str | bytes] = []
    session: PublicWebSocketSession

    async def handle(message: str | bytes, _received_at: datetime) -> None:
        received.append(message)
        await session.stop()

    session = session_for(
        factory=FakeFactory([transport]),
        message_handler=handle,
        heartbeat=TextPingHeartbeat(),
    )
    await session.run()

    assert received == ["market-data"]
    assert transport.sent == [
        '{"op":"subscribe"}',
        "ping",
        '{"op":"unsubscribe"}',
    ]


@pytest.mark.asyncio
async def test_reconnects_with_backoff_and_resubscribes() -> None:
    first = FakeTransport([ConnectionError("wire lost")])
    second = FakeTransport(["recovered"])
    factory = FakeFactory([first, second])
    sleeps: list[float] = []
    events: list[SessionEvent] = []
    session: PublicWebSocketSession

    async def handle(_message: str | bytes, _received_at: datetime) -> None:
        await session.stop()

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    async def record(event: SessionEvent) -> None:
        events.append(event)

    session = session_for(
        factory=factory,
        message_handler=handle,
        event_handler=record,
        sleep=fake_sleep,
    )
    await session.run()

    assert factory.calls == 2
    assert first.sent == ['{"op":"subscribe"}']
    assert second.sent == ['{"op":"subscribe"}', '{"op":"unsubscribe"}']
    assert sleeps == [1.5]
    assert session.connection_generation == 2
    reconnect_event = next(
        event for event in events if event.kind is SessionEventKind.RECONNECT_SCHEDULED
    )
    assert reconnect_event.reconnect_attempt == 1
    assert reconnect_event.backoff_seconds == 1.5


@pytest.mark.asyncio
async def test_external_stop_wakes_blocked_receive_without_reconnect() -> None:
    transport = FakeTransport([STALL])
    subscribed = asyncio.Event()
    events: list[SessionEventKind] = []

    async def handle(_message: str | bytes, _received_at: datetime) -> None:
        raise AssertionError("no market message expected")

    async def record(event: SessionEvent) -> None:
        events.append(event.kind)
        if event.kind is SessionEventKind.SUBSCRIBED:
            subscribed.set()

    session = session_for(
        factory=FakeFactory([transport]),
        message_handler=handle,
        event_handler=record,
    )
    task = asyncio.create_task(session.run())
    await asyncio.wait_for(subscribed.wait(), timeout=1.0)
    await session.stop()
    await asyncio.wait_for(task, timeout=1.0)

    assert transport.closed
    assert SessionEventKind.RECONNECT_SCHEDULED not in events
    assert SessionEventKind.DISCONNECTED not in events
    assert events.count(SessionEventKind.CLOSED) == 1


def test_backoff_is_exponential_capped_jittered_and_large_attempt_safe() -> None:
    policy = backoff()

    def use_upper_bound(_low: float, high: float) -> float:
        return high

    assert policy.delay(1, uniform=use_upper_bound) == 1.5
    assert policy.delay(2, uniform=use_upper_bound) == 2.5
    assert policy.delay(4, uniform=use_upper_bound) == 8.5
    assert policy.delay(1_000_000, uniform=use_upper_bound) == 8.5

    with pytest.raises(ValueError, match="at least one"):
        policy.delay(0)

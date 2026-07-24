"""Reusable public WebSocket lifecycle with heartbeat and bounded reconnect backoff."""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake


class SessionError(RuntimeError):
    """Base class for recoverable public WebSocket session failures."""


class SessionProtocolError(SessionError):
    """Raised by a message handler when a venue rejects or corrupts a session."""


class HeartbeatTimeout(SessionError):
    """Raised when a heartbeat probe does not receive a timely response."""


class SessionEventKind(StrEnum):
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    SUBSCRIBED = "SUBSCRIBED"
    MESSAGE_RECEIVED = "MESSAGE_RECEIVED"
    HEARTBEAT_SENT = "HEARTBEAT_SENT"
    HEARTBEAT_OK = "HEARTBEAT_OK"
    DISCONNECTED = "DISCONNECTED"
    RECONNECT_SCHEDULED = "RECONNECT_SCHEDULED"
    UNSUBSCRIBED = "UNSUBSCRIBED"
    CLOSED = "CLOSED"


@dataclass(frozen=True, slots=True)
class SessionEvent:
    """Observable lifecycle event for health aggregation."""

    kind: SessionEventKind
    timestamp: datetime
    connection_generation: int
    reconnect_attempt: int
    reason: str | None = None
    backoff_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    """Exponential reconnect delay with bounded additive jitter."""

    initial_seconds: float
    maximum_seconds: float
    jitter_seconds: float
    stable_reset_seconds: float

    def __post_init__(self) -> None:
        if self.initial_seconds <= 0:
            raise ValueError("initial_seconds must be positive")
        if self.maximum_seconds < self.initial_seconds:
            raise ValueError("maximum_seconds cannot be below initial_seconds")
        if self.jitter_seconds < 0:
            raise ValueError("jitter_seconds cannot be negative")
        if self.stable_reset_seconds <= 0:
            raise ValueError("stable_reset_seconds must be positive")

    def delay(
        self,
        attempt: int,
        *,
        uniform: Callable[[float, float], float] = random.uniform,
    ) -> float:
        if attempt < 1:
            raise ValueError("attempt must be at least one")
        base = self.initial_seconds
        for _ in range(attempt - 1):
            base = min(self.maximum_seconds, base * 2)
            if base == self.maximum_seconds:
                break
        return base + uniform(0.0, self.jitter_seconds)


class WebSocketTransport(Protocol):
    """Minimal transport operations required by the lifecycle."""

    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def ping(self) -> None: ...

    async def close(self) -> None: ...


class ConnectionFactory(Protocol):
    async def __call__(self) -> WebSocketTransport: ...


type MessageHandler = Callable[[str | bytes, datetime], Awaitable[None]]
type SessionEventHandler = Callable[[SessionEvent], Awaitable[None]]


class HeartbeatPolicy(Protocol):
    async def probe(
        self,
        transport: WebSocketTransport,
        *,
        timeout_seconds: float,
    ) -> str | bytes | None: ...


class ProtocolPingHeartbeat:
    """Use a WebSocket protocol ping and wait for its pong."""

    async def probe(
        self,
        transport: WebSocketTransport,
        *,
        timeout_seconds: float,
    ) -> str | bytes | None:
        try:
            await asyncio.wait_for(transport.ping(), timeout=timeout_seconds)
        except TimeoutError as exc:
            raise HeartbeatTimeout("protocol pong timed out") from exc
        return None


class TextPingHeartbeat:
    """OKX-style text `ping` / `pong` heartbeat."""

    def __init__(self, *, ping_message: str = "ping", pong_message: str = "pong") -> None:
        self.ping_message = ping_message
        self.pong_message = pong_message

    async def probe(
        self,
        transport: WebSocketTransport,
        *,
        timeout_seconds: float,
    ) -> str | bytes | None:
        await transport.send(self.ping_message)
        try:
            response = await asyncio.wait_for(
                transport.recv(),
                timeout=timeout_seconds,
            )
        except TimeoutError as exc:
            raise HeartbeatTimeout("text pong timed out") from exc
        if response == self.pong_message:
            return None
        return response


class WebsocketsTransport:
    """Adapter around the installed `websockets` asyncio client."""

    def __init__(self, connection: ClientConnection) -> None:
        self._connection = connection

    async def send(self, message: str) -> None:
        await self._connection.send(message)

    async def recv(self) -> str | bytes:
        return await self._connection.recv(decode=None)

    async def ping(self) -> None:
        pong_waiter = await self._connection.ping()
        await pong_waiter

    async def close(self) -> None:
        await self._connection.close()


def websocket_connection_factory(
    url: str,
    *,
    connect_timeout_seconds: float,
    close_timeout_seconds: float | None = None,
) -> ConnectionFactory:
    """Create an unauthenticated public WebSocket connection factory."""

    async def factory() -> WebSocketTransport:
        connection = await connect(
            url,
            open_timeout=connect_timeout_seconds,
            ping_interval=None,
            close_timeout=close_timeout_seconds or connect_timeout_seconds,
            max_queue=16,
        )
        return WebsocketsTransport(connection)

    return factory


async def _noop_event_handler(_event: SessionEvent) -> None:
    return None


class PublicWebSocketSession:
    """Subscribe, receive, heartbeat, reconnect, unsubscribe, and close."""

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        subscribe_messages: Sequence[str],
        unsubscribe_messages: Sequence[str],
        message_handler: MessageHandler,
        heartbeat_policy: HeartbeatPolicy,
        receive_timeout_seconds: float,
        heartbeat_timeout_seconds: float,
        backoff_policy: BackoffPolicy,
        event_handler: SessionEventHandler | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        utc_now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        uniform: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if receive_timeout_seconds <= 0:
            raise ValueError("receive_timeout_seconds must be positive")
        if heartbeat_timeout_seconds <= 0:
            raise ValueError("heartbeat_timeout_seconds must be positive")
        self._connection_factory = connection_factory
        self._subscribe_messages = tuple(subscribe_messages)
        self._unsubscribe_messages = tuple(unsubscribe_messages)
        self._message_handler = message_handler
        self._heartbeat_policy = heartbeat_policy
        self._receive_timeout_seconds = receive_timeout_seconds
        self._heartbeat_timeout_seconds = heartbeat_timeout_seconds
        self._backoff_policy = backoff_policy
        self._event_handler = event_handler or _noop_event_handler
        self._sleep = sleep
        self._utc_now = utc_now
        self._monotonic = monotonic
        self._uniform = uniform
        self._stop = asyncio.Event()
        self._active_transport: WebSocketTransport | None = None
        self._active_generation = 0
        self._closed_generation: int | None = None

    @property
    def connection_generation(self) -> int:
        return self._active_generation

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    async def _emit(
        self,
        kind: SessionEventKind,
        *,
        reconnect_attempt: int,
        reason: str | None = None,
        backoff_seconds: float | None = None,
    ) -> None:
        await self._event_handler(
            SessionEvent(
                kind=kind,
                timestamp=self._utc_now(),
                connection_generation=self._active_generation,
                reconnect_attempt=reconnect_attempt,
                reason=reason,
                backoff_seconds=backoff_seconds,
            )
        )

    async def _send_all(
        self,
        transport: WebSocketTransport,
        messages: Sequence[str],
    ) -> None:
        for message in messages:
            await transport.send(message)

    async def _receive_loop(
        self,
        transport: WebSocketTransport,
        *,
        reconnect_attempt: int,
    ) -> None:
        while not self._stop.is_set():
            try:
                message = await asyncio.wait_for(
                    transport.recv(),
                    timeout=self._receive_timeout_seconds,
                )
            except TimeoutError:
                await self._emit(
                    SessionEventKind.HEARTBEAT_SENT,
                    reconnect_attempt=reconnect_attempt,
                )
                heartbeat_message = await self._heartbeat_policy.probe(
                    transport,
                    timeout_seconds=self._heartbeat_timeout_seconds,
                )
                await self._emit(
                    SessionEventKind.HEARTBEAT_OK,
                    reconnect_attempt=reconnect_attempt,
                )
                if heartbeat_message is None:
                    continue
                message = heartbeat_message
            received_at = self._utc_now()
            await self._emit(
                SessionEventKind.MESSAGE_RECEIVED,
                reconnect_attempt=reconnect_attempt,
            )
            await self._message_handler(message, received_at)

    async def _close_active(
        self,
        *,
        reconnect_attempt: int,
        graceful: bool,
    ) -> None:
        transport = self._active_transport
        if transport is None or self._closed_generation == self._active_generation:
            return
        self._closed_generation = self._active_generation
        if graceful and self._unsubscribe_messages:
            with suppress(ConnectionClosed, ConnectionError, OSError):
                await self._send_all(transport, self._unsubscribe_messages)
                await self._emit(
                    SessionEventKind.UNSUBSCRIBED,
                    reconnect_attempt=reconnect_attempt,
                )
        with suppress(ConnectionClosed, ConnectionError, OSError):
            await transport.close()
        await self._emit(
            SessionEventKind.CLOSED,
            reconnect_attempt=reconnect_attempt,
        )
        self._active_transport = None

    async def stop(self) -> None:
        """Request prompt graceful shutdown and wake any blocked receive."""

        self._stop.set()
        await self._close_active(reconnect_attempt=0, graceful=True)

    async def _sleep_until_reconnect_or_stop(self, delay: float) -> None:
        sleeper = asyncio.ensure_future(self._sleep(delay))
        stopper = asyncio.create_task(self._stop.wait())
        done, pending = await asyncio.wait(
            {sleeper, stopper},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        for task in pending:
            with suppress(asyncio.CancelledError):
                await task
        for task in done:
            await task

    async def run(self) -> None:
        """Run until `stop()` is requested, reconnecting recoverable failures."""

        reconnect_attempt = 0
        recoverable_errors = (
            ConnectionClosed,
            ConnectionError,
            OSError,
            TimeoutError,
            InvalidHandshake,
            SessionError,
        )
        while not self._stop.is_set():
            await self._emit(
                SessionEventKind.CONNECTING,
                reconnect_attempt=reconnect_attempt,
            )
            connection_started = self._monotonic()
            try:
                transport = await self._connection_factory()
                self._active_generation += 1
                self._closed_generation = None
                self._active_transport = transport
                await self._emit(
                    SessionEventKind.CONNECTED,
                    reconnect_attempt=reconnect_attempt,
                )
                await self._send_all(transport, self._subscribe_messages)
                await self._emit(
                    SessionEventKind.SUBSCRIBED,
                    reconnect_attempt=reconnect_attempt,
                )
                await self._receive_loop(
                    transport,
                    reconnect_attempt=reconnect_attempt,
                )
            except asyncio.CancelledError:
                self._stop.set()
                await self._close_active(
                    reconnect_attempt=reconnect_attempt,
                    graceful=True,
                )
                raise
            except recoverable_errors as exc:
                if not self._stop.is_set():
                    await self._emit(
                        SessionEventKind.DISCONNECTED,
                        reconnect_attempt=reconnect_attempt,
                        reason=f"{type(exc).__name__}: {exc}",
                    )
            finally:
                await self._close_active(
                    reconnect_attempt=reconnect_attempt,
                    graceful=self._stop.is_set(),
                )

            if self._stop.is_set():
                break
            stable = (
                self._monotonic() - connection_started
                >= self._backoff_policy.stable_reset_seconds
            )
            reconnect_attempt = 1 if stable else reconnect_attempt + 1
            delay = self._backoff_policy.delay(
                reconnect_attempt,
                uniform=self._uniform,
            )
            await self._emit(
                SessionEventKind.RECONNECT_SCHEDULED,
                reconnect_attempt=reconnect_attempt,
                backoff_seconds=delay,
            )
            await self._sleep_until_reconnect_or_stop(delay)

"""Binance USDⓈ-M public market-data connector."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Literal
from urllib.parse import quote

import httpx

from cvf.config import ExchangeConnectionConfig
from cvf.exchanges.base import ExchangeConnector, PlannedSubscription
from cvf.exchanges.deduplication import BoundedTTLDeduplicator
from cvf.exchanges.runtime import (
    NormalizedEventSink,
    RawRecordWriter,
    decode_json_message,
    discard_event,
    exact_message_bytes,
    normalized_event_identity,
    utc_now,
)
from cvf.exchanges.session import (
    BackoffPolicy,
    ConnectionFactory,
    ProtocolPingHeartbeat,
    PublicWebSocketSession,
    SessionEvent,
    SessionEventKind,
    SessionProtocolError,
    websocket_connection_factory,
)
from cvf.models.enums import Exchange
from cvf.models.market import OrderBookSnapshot, OrderBookUpdate
from cvf.monitoring import (
    StreamHealthRegistry,
    StreamHealthSnapshot,
    StreamKey,
    estimate_clock_skew_ms,
)
from cvf.normalization.binance import (
    BinanceDepthSnapshotPayload,
    BinanceDepthUpdatePayload,
    BinanceNormalizer,
    parse_binance_payload,
)
from cvf.normalization.common import NormalizationContext, NormalizedMarketEvent
from cvf.orderbook import BookStatus, BookTransition
from cvf.orderbook.binance import BinanceOrderBookManager
from cvf.storage.raw import RawMarketRecord

ConnectionFactoryBuilder = Callable[[str], ConnectionFactory]


def _milliseconds_timestamp(value: object) -> datetime | None:
    if isinstance(value, (int, str)):
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
        except (ValueError, OverflowError):
            return None
    return None


class BinanceMarketDataConnector(ExchangeConnector):
    """Collect, preserve, validate, normalize, and monitor Binance public data."""

    def __init__(
        self,
        config: ExchangeConnectionConfig,
        *,
        stale_after_ms: int,
        health_registry: StreamHealthRegistry | None = None,
        raw_writer: RawRecordWriter | None = None,
        event_sink: NormalizedEventSink = discard_event,
        duplicate_cache_size: int = 10_000,
        duplicate_ttl_seconds: float = 300,
        http_client: httpx.AsyncClient | None = None,
        connection_factory_builder: ConnectionFactoryBuilder | None = None,
    ) -> None:
        super().__init__(
            exchange=Exchange.BINANCE,
            config=config,
            stale_after_ms=stale_after_ms,
        )
        self._logger = logging.getLogger("cvf.exchanges.binance")
        self._health = health_registry or StreamHealthRegistry(
            stale_after_ms=stale_after_ms,
            maximum_core_latency_ms=500,
            clock_skew_warning_ms=250,
        )
        self._raw_writer = raw_writer
        self._event_sink = event_sink
        self._dedupe = BoundedTTLDeduplicator(
            capacity=duplicate_cache_size,
            ttl_seconds=duplicate_ttl_seconds,
        )
        self._http_client = http_client
        self._owns_http_client = http_client is None
        self._connection_factory_builder = connection_factory_builder
        self._normalizer = BinanceNormalizer()
        self._books = BinanceOrderBookManager(
            max_buffer_events=config.book_buffer_events
        )
        self._sessions: list[PublicWebSocketSession] = []
        self._session_tasks: list[asyncio.Task[None]] = []
        self._poller_tasks: list[asyncio.Task[None]] = []
        self._snapshot_tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._clock_skew_ms: float | None = None
        self._reverse_symbols = {
            venue_symbol.upper(): canonical
            for canonical, venue_symbol in self.config.symbols.items()
        }

    def _canonical(self, venue_symbol: str) -> str:
        try:
            return self._reverse_symbols[venue_symbol.upper()]
        except KeyError as exc:
            raise ValueError(f"unconfigured Binance symbol {venue_symbol!r}") from exc

    def _key(self, symbol: str, channel: str) -> StreamKey:
        return StreamKey(Exchange.BINANCE, symbol, channel)

    def _configured_keys(self, channels: set[str] | None = None) -> list[StreamKey]:
        available = [*self.config.channels, *self.config.rest_pollers]
        selected = set(available) if channels is None else channels
        return [
            self._key(canonical, channel)
            for canonical in self.config.symbols
            for channel in available
            if channel in selected
        ]

    @staticmethod
    def _route(channel: str) -> str:
        if channel == "bookTicker" or channel.startswith("depth"):
            return "public"
        return "market"

    @staticmethod
    def _root_url(configured: str) -> str:
        root = configured.rstrip("/")
        for suffix in ("/public/ws", "/market/ws", "/ws"):
            if root.endswith(suffix):
                return root.removesuffix(suffix)
        return root

    def _combined_url(self, route: str, streams: list[str]) -> str:
        joined = quote("/".join(streams), safe="/@")
        return f"{self._root_url(self.config.public_websocket_url)}/{route}/stream?streams={joined}"

    def _connection_factory(self, url: str) -> ConnectionFactory:
        if self._connection_factory_builder is not None:
            return self._connection_factory_builder(url)
        return websocket_connection_factory(
            url,
            connect_timeout_seconds=self.config.connect_timeout_seconds,
            close_timeout_seconds=self.config.close_timeout_seconds,
        )

    def websocket_urls(self) -> dict[str, str]:
        """Return the exact routed combined-stream URLs used by `connect()`."""

        urls: dict[str, str] = {}
        for route in ("public", "market"):
            streams = [
                f"{venue_symbol.lower()}@{channel}"
                for venue_symbol in self.config.symbols.values()
                for channel in self.config.channels
                if self._route(channel) == route
            ]
            if streams:
                urls[route] = self._combined_url(route, streams)
        return urls

    async def connect(self) -> None:
        """Start routed public WebSockets and configured REST pollers."""

        if self._session_tasks or self._poller_tasks:
            return
        self._stop.clear()
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.config.rest_url,
                timeout=self.config.rest_timeout_seconds,
            )
        for route, url in self.websocket_urls().items():
            channels = {
                channel for channel in self.config.channels if self._route(channel) == route
            }
            session = PublicWebSocketSession(
                connection_factory=self._connection_factory(url),
                subscribe_messages=(),
                unsubscribe_messages=(),
                message_handler=self._handle_websocket_message,
                heartbeat_policy=ProtocolPingHeartbeat(),
                receive_timeout_seconds=self.config.receive_timeout_seconds,
                heartbeat_timeout_seconds=self.config.heartbeat_timeout_seconds,
                backoff_policy=BackoffPolicy(
                    initial_seconds=self.config.reconnect_initial_seconds,
                    maximum_seconds=self.config.reconnect_max_seconds,
                    jitter_seconds=self.config.reconnect_jitter_seconds,
                    stable_reset_seconds=self.config.reconnect_stable_reset_seconds,
                ),
                event_handler=self._session_event_handler(route, channels),
            )
            self._sessions.append(session)
            self._session_tasks.append(
                asyncio.create_task(
                    session.run(),
                    name=f"binance-{route}-websocket",
                )
            )
        if "open_interest" in self.config.rest_pollers:
            self._poller_tasks.append(
                asyncio.create_task(
                    self._open_interest_loop(),
                    name="binance-open-interest",
                )
            )

    async def wait(self) -> None:
        """Wait until a connector task fails or the connector is stopped."""

        tasks = [*self._session_tasks, *self._poller_tasks]
        if tasks:
            await asyncio.gather(*tasks)

    async def disconnect(self) -> None:
        self._stop.set()
        await asyncio.gather(*(session.stop() for session in self._sessions))
        for task in [*self._poller_tasks, *self._snapshot_tasks.values()]:
            task.cancel()
        await asyncio.gather(
            *self._session_tasks,
            *self._poller_tasks,
            *self._snapshot_tasks.values(),
            return_exceptions=True,
        )
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
        self._sessions.clear()
        self._session_tasks.clear()
        self._poller_tasks.clear()
        self._snapshot_tasks.clear()
        for key in self._configured_keys():
            self._health.mark_disconnected(key)
        await super().disconnect()

    def normalize_message(self, payload: dict[str, Any]) -> list[Any]:
        now = utc_now()
        return self._normalizer.normalize(
            payload,
            context=NormalizationContext(
                local_receive_timestamp=now,
                normalization_timestamp=now,
                raw_payload_reference=None,
            ),
        )

    def planned_subscriptions(self) -> list[PlannedSubscription]:
        plans: list[PlannedSubscription] = []
        for canonical, venue_symbol in self.config.symbols.items():
            stream_symbol = venue_symbol.lower()
            for channel in self.config.channels:
                key = f"{stream_symbol}@{channel}"
                plans.append(
                    PlannedSubscription(
                        transport="websocket",
                        channel=channel,
                        venue_symbol=venue_symbol,
                        subscription_key=key,
                        parameters={
                            "canonical_symbol": canonical,
                            "route": self._route(channel),
                        },
                    )
                )
            for poller in self.config.rest_pollers:
                plans.append(
                    PlannedSubscription(
                        transport="rest",
                        channel=poller,
                        venue_symbol=venue_symbol,
                        subscription_key=f"{poller}:{venue_symbol}",
                        parameters={"canonical_symbol": canonical},
                    )
                )
        return plans

    def health_snapshots(self, *, now: datetime | None = None) -> list[StreamHealthSnapshot]:
        checked_at = now or utc_now()
        return [
            self._health.snapshot(key, now=checked_at)
            for key in self._configured_keys()
        ]

    def _session_event_handler(
        self,
        route: str,
        channels: set[str],
    ) -> Callable[[SessionEvent], Awaitable[None]]:
        keys = self._configured_keys(channels)

        async def handle(event: SessionEvent) -> None:
            if event.kind is SessionEventKind.CONNECTED:
                self._is_connected = True
                for key in keys:
                    self._health.mark_connected(key, at=event.timestamp)
                for canonical, venue_symbol in self.config.symbols.items():
                    for channel in channels:
                        if not channel.startswith("depth"):
                            continue
                        result = self._books.book(venue_symbol).begin_resync(
                            "Binance WebSocket connection requires a fresh REST snapshot"
                        )
                        self._health.mark_resyncing(
                            self._key(canonical, channel),
                            book_generation=result.generation,
                            reason=result.reason or "Binance depth snapshot required",
                        )
                        self._schedule_depth_snapshot(venue_symbol, channel)
            elif event.kind is SessionEventKind.SUBSCRIBED and event.connection_generation > 1:
                for key in keys:
                    self._health.record_resubscribe(key)
            elif event.kind is SessionEventKind.DISCONNECTED:
                for key in keys:
                    self._health.mark_disconnected(key, error=event.reason)
            elif event.kind is SessionEventKind.RECONNECT_SCHEDULED:
                for key in keys:
                    self._health.record_reconnect(key, error=event.reason)
            await self._store_internal_event(route, event)

        return handle

    async def _store_internal_event(self, route: str, event: SessionEvent) -> None:
        if self._raw_writer is None:
            return
        payload = json.dumps(
            {
                "kind": event.kind.value,
                "generation": event.connection_generation,
                "reconnect_attempt": event.reconnect_attempt,
                "reason": event.reason,
                "backoff_seconds": event.backoff_seconds,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        await self._raw_writer.write(
            RawMarketRecord(
                exchange=Exchange.BINANCE,
                symbol="*",
                channel=f"_session_{route}",
                message_kind="connection_lifecycle",
                transport="internal",
                local_receive_timestamp=event.timestamp,
                normalization_timestamp=event.timestamp,
                connection_generation=event.connection_generation,
                raw_payload=payload,
            )
        )

    def _websocket_routing(
        self,
        payload: dict[str, Any],
    ) -> tuple[str, str, datetime | None, int | str | None]:
        stream = payload.get("stream")
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            raise SessionProtocolError("Binance combined payload data must be an object")
        event_type = data.get("e")
        channel = (
            str(stream).split("@", 1)[1]
            if isinstance(stream, str) and "@" in stream
            else str(event_type or "_unknown")
        )
        symbol_raw = data.get("s")
        order = data.get("o")
        if symbol_raw is None and isinstance(order, dict):
            symbol_raw = order.get("s")
        if not isinstance(symbol_raw, str):
            raise SessionProtocolError("Binance payload is missing a symbol")
        timestamp = _milliseconds_timestamp(
            data.get("T") or data.get("E") or (order.get("T") if isinstance(order, dict) else None)
        )
        sequence = data.get("u") or data.get("a")
        return channel, self._canonical(symbol_raw), timestamp, sequence

    async def _persist_payload(
        self,
        *,
        raw_payload: bytes,
        symbol: str,
        channel: str,
        transport: Literal["websocket", "rest", "internal"],
        received_at: datetime,
        exchange_timestamp: datetime | None,
        sequence_id: int | str | None,
        connection_generation: int,
    ) -> str | None:
        if self._raw_writer is None:
            return None
        record = RawMarketRecord(
            exchange=Exchange.BINANCE,
            symbol=symbol,
            channel=channel,
            message_kind="market_data",
            transport=transport,
            exchange_timestamp=exchange_timestamp,
            local_receive_timestamp=received_at,
            sequence_id=sequence_id,
            connection_generation=connection_generation,
            raw_payload=raw_payload,
        )
        await self._raw_writer.write(record)
        return record.raw_payload_reference

    async def _handle_websocket_message(
        self,
        message: str | bytes,
        local_receive_timestamp: datetime,
    ) -> None:
        received_at = local_receive_timestamp
        raw_payload = exact_message_bytes(message)
        try:
            payload = decode_json_message(message)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            await self._persist_payload(
                raw_payload=raw_payload,
                symbol="*",
                channel="_unparsed",
                transport="websocket",
                received_at=received_at,
                exchange_timestamp=None,
                sequence_id=None,
                connection_generation=max(
                    (session.connection_generation for session in self._sessions),
                    default=0,
                ),
            )
            raise SessionProtocolError(f"invalid Binance JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise SessionProtocolError("Binance payload must be an object")
        channel, symbol, exchange_at, sequence = self._websocket_routing(payload)
        generation = max(
            (session.connection_generation for session in self._sessions),
            default=0,
        )
        reference = await self._persist_payload(
            raw_payload=raw_payload,
            symbol=symbol,
            channel=channel,
            transport="websocket",
            received_at=received_at,
            exchange_timestamp=exchange_at,
            sequence_id=sequence,
            connection_generation=generation,
        )
        parsed = parse_binance_payload(payload)
        if isinstance(parsed, BinanceDepthUpdatePayload):
            result = self._books.ingest(parsed)
            key = self._key(symbol, channel)
            if result.transition in {
                BookTransition.SEQUENCE_GAP,
                BookTransition.BUFFER_OVERFLOW,
            }:
                self._health.record_sequence_gap(
                    key,
                    book_generation=result.generation,
                )
            elif result.needs_snapshot:
                self._health.mark_resyncing(
                    key,
                    book_generation=result.generation,
                    reason=result.reason or "Binance depth snapshot required",
                )
            if result.status is BookStatus.LIVE:
                self._health.mark_book_live(key, book_generation=result.generation)
            if result.needs_snapshot:
                self._schedule_depth_snapshot(parsed.venue_symbol, channel)
        normalized_at = utc_now()
        events = self._normalizer.normalize(
            payload,
            context=NormalizationContext(
                local_receive_timestamp=received_at,
                normalization_timestamp=normalized_at,
                raw_payload_reference=reference,
            ),
        )
        if isinstance(parsed, BinanceDepthUpdatePayload):
            generation = self._books.book(parsed.venue_symbol).generation
            events = [
                event.model_copy(update={"generation": generation})
                if isinstance(event, (OrderBookSnapshot, OrderBookUpdate))
                else event
                for event in events
            ]
        await self._emit_events(events, health_channel=channel)

    async def process_websocket_message(
        self,
        message: str | bytes,
        *,
        local_receive_timestamp: datetime,
    ) -> None:
        """Process one received frame; exposed for deterministic integration tests."""

        await self._handle_websocket_message(message, local_receive_timestamp)

    async def _emit_events(
        self,
        events: list[NormalizedMarketEvent],
        *,
        health_channel: str,
    ) -> None:
        for event in events:
            key = self._key(event.symbol, health_channel)
            if self._dedupe.seen_or_add(normalized_event_identity(event)):
                self._health.record_duplicate(key)
                continue
            self._health.record_message(
                key,
                exchange_timestamp=event.exchange_timestamp,
                receive_timestamp=event.local_receive_timestamp,
                normalization_timestamp=event.normalization_timestamp,
                clock_skew_ms=self._clock_skew_ms,
                is_open_interest=health_channel == "open_interest",
            )
            self._record_event(
                exchange_timestamp=event.exchange_timestamp,
                local_receive_timestamp=event.local_receive_timestamp,
            )
            await self._event_sink(event)

    def _schedule_depth_snapshot(self, venue_symbol: str, channel: str) -> None:
        current = self._snapshot_tasks.get(venue_symbol)
        if current is not None and not current.done():
            return
        task = asyncio.create_task(
            self._refresh_depth_snapshot(venue_symbol, channel),
            name=f"binance-depth-snapshot-{venue_symbol}",
        )
        self._snapshot_tasks[venue_symbol] = task

    async def _refresh_depth_snapshot(self, venue_symbol: str, channel: str) -> None:
        client = self._http_client
        if client is None:
            raise RuntimeError("Binance HTTP client is not initialized")
        canonical = self._canonical(venue_symbol)
        key = self._key(canonical, channel)
        try:
            received_at = utc_now()
            response = await client.get(
                "/fapi/v1/depth",
                params={
                    "symbol": venue_symbol,
                    "limit": self.config.book_snapshot_depth,
                },
            )
            received_at = utc_now()
            response.raise_for_status()
            payload = response.json()
            parsed = parse_binance_payload(payload)
            if not isinstance(parsed, BinanceDepthSnapshotPayload):
                raise ValueError("Binance depth endpoint returned a non-snapshot payload")
            reference = await self._persist_payload(
                raw_payload=response.content,
                symbol=canonical,
                channel="depth_snapshot",
                transport="rest",
                received_at=received_at,
                exchange_timestamp=_milliseconds_timestamp(
                    parsed.transaction_time_ms or parsed.event_time_ms
                ),
                sequence_id=parsed.last_update_id,
                connection_generation=0,
            )
            result = self._books.install_snapshot(venue_symbol, parsed)
            if result.status is BookStatus.LIVE:
                self._health.mark_book_live(key, book_generation=result.generation)
            elif result.needs_snapshot:
                self._health.mark_resyncing(
                    key,
                    book_generation=result.generation,
                    reason=result.reason or "Binance depth snapshot retry required",
                )
            events = self._normalizer.normalize(
                payload,
                context=NormalizationContext(
                    local_receive_timestamp=received_at,
                    normalization_timestamp=utc_now(),
                    raw_payload_reference=reference,
                    canonical_symbol=canonical,
                ),
            )
            await self._emit_events(events, health_channel=channel)
            self._health.record_rest_result(key, healthy=True)
        except (httpx.HTTPError, ValueError, KeyError) as exc:
            self._health.record_rest_result(key, healthy=False, error=str(exc))
            self._logger.exception(
                "Binance depth snapshot failed",
                extra={
                    "event": "depth_snapshot_failed",
                    "exchange": Exchange.BINANCE.value,
                    "symbol": canonical,
                },
            )

    async def _poll_server_time(self) -> None:
        client = self._http_client
        if client is None:
            raise RuntimeError("Binance HTTP client is not initialized")
        sent_at = utc_now()
        response = await client.get("/fapi/v1/time")
        received_at = utc_now()
        response.raise_for_status()
        payload = response.json()
        server_at = _milliseconds_timestamp(payload.get("serverTime"))
        if server_at is None:
            raise ValueError("Binance server time response is invalid")
        self._clock_skew_ms = estimate_clock_skew_ms(
            request_sent_at=sent_at,
            response_received_at=received_at,
            exchange_timestamp=server_at,
        )
        await self._persist_payload(
            raw_payload=response.content,
            symbol="*",
            channel="server_time",
            transport="rest",
            received_at=received_at,
            exchange_timestamp=server_at,
            sequence_id=None,
            connection_generation=0,
        )

    async def _poll_open_interest_once(self) -> None:
        client = self._http_client
        if client is None:
            raise RuntimeError("Binance HTTP client is not initialized")
        for canonical, venue_symbol in self.config.symbols.items():
            key = self._key(canonical, "open_interest")
            received_at = utc_now()
            response = await client.get(
                "/fapi/v1/openInterest",
                params={"symbol": venue_symbol},
            )
            received_at = utc_now()
            response.raise_for_status()
            payload = response.json()
            parsed = parse_binance_payload(payload)
            reference = await self._persist_payload(
                raw_payload=response.content,
                symbol=canonical,
                channel="open_interest",
                transport="rest",
                received_at=received_at,
                exchange_timestamp=_milliseconds_timestamp(
                    getattr(parsed, "event_time_ms", None)
                ),
                sequence_id=None,
                connection_generation=0,
            )
            events = self._normalizer.normalize(
                payload,
                context=NormalizationContext(
                    local_receive_timestamp=received_at,
                    normalization_timestamp=utc_now(),
                    raw_payload_reference=reference,
                ),
            )
            await self._emit_events(events, health_channel="open_interest")
            self._health.mark_connected(key, at=received_at)
            self._health.record_rest_result(key, healthy=True)

    async def _open_interest_loop(self) -> None:
        loop = asyncio.get_running_loop()
        next_clock_check = 0.0
        while not self._stop.is_set():
            try:
                if loop.time() >= next_clock_check:
                    await self._poll_server_time()
                    next_clock_check = loop.time() + 60
                await self._poll_open_interest_once()
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                for key in self._configured_keys({"open_interest"}):
                    self._health.record_rest_result(key, healthy=False, error=str(exc))
                self._logger.exception(
                    "Binance REST poll failed",
                    extra={
                        "event": "rest_poll_failed",
                        "exchange": Exchange.BINANCE.value,
                    },
                )
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self.config.open_interest_poll_seconds,
                )
            except TimeoutError:
                continue

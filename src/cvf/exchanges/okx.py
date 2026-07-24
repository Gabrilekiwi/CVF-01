"""OKX v5 public market-data connector."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Literal

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
    PublicWebSocketSession,
    SessionEvent,
    SessionEventKind,
    SessionProtocolError,
    TextPingHeartbeat,
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
from cvf.normalization.common import NormalizationContext, NormalizedMarketEvent
from cvf.normalization.instruments import ContractSpecification
from cvf.normalization.okx import (
    OkxBooks5Message,
    OkxBooksMessage,
    OkxNormalizer,
    contract_specifications_from_response,
    parse_okx_payload,
)
from cvf.orderbook import BookStatus, BookTransition
from cvf.orderbook.okx import OkxOrderBookManager
from cvf.storage.raw import RawMarketRecord

ConnectionFactoryBuilder = Callable[[str], ConnectionFactory]


def _milliseconds_timestamp(value: object) -> datetime | None:
    if isinstance(value, (int, str)):
        try:
            return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
        except (ValueError, OverflowError):
            return None
    return None


class OKXMarketDataConnector(ExchangeConnector):
    """Collect and validate OKX public streams using live contract metadata."""

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
            exchange=Exchange.OKX,
            config=config,
            stale_after_ms=stale_after_ms,
        )
        self._logger = logging.getLogger("cvf.exchanges.okx")
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
        self._normalizer: OkxNormalizer | None = None
        self._books: OkxOrderBookManager | None = None
        self._specifications: dict[str, ContractSpecification] = {}
        self._session: PublicWebSocketSession | None = None
        self._session_task: asyncio.Task[None] | None = None
        self._poller_tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._clock_skew_ms: float | None = None
        self._reverse_symbols = {
            venue_symbol.upper(): canonical
            for canonical, venue_symbol in self.config.symbols.items()
        }

    def _canonical(self, venue_symbol: str) -> str:
        normalized = venue_symbol.upper()
        if not normalized.endswith("-SWAP"):
            normalized = f"{normalized}-SWAP"
        try:
            return self._reverse_symbols[normalized]
        except KeyError as exc:
            raise ValueError(f"unconfigured OKX symbol {venue_symbol!r}") from exc

    def _key(self, symbol: str, channel: str) -> StreamKey:
        return StreamKey(Exchange.OKX, symbol, channel)

    def _configured_keys(self) -> list[StreamKey]:
        channels = [*self.config.channels, *self.config.rest_pollers]
        return [
            self._key(canonical, channel)
            for canonical in self.config.symbols
            for channel in channels
        ]

    def _websocket_keys(self) -> list[StreamKey]:
        return [
            self._key(canonical, channel)
            for canonical in self.config.symbols
            for channel in self.config.channels
        ]

    def _connection_factory(self) -> ConnectionFactory:
        if self._connection_factory_builder is not None:
            return self._connection_factory_builder(self.config.public_websocket_url)
        return websocket_connection_factory(
            self.config.public_websocket_url,
            connect_timeout_seconds=self.config.connect_timeout_seconds,
            close_timeout_seconds=self.config.close_timeout_seconds,
        )

    def _subscription_arguments(self) -> list[dict[str, str]]:
        arguments: list[dict[str, str]] = []
        liquidation_added = False
        for venue_symbol in self.config.symbols.values():
            for channel in self.config.channels:
                if channel == "liquidation-orders":
                    if liquidation_added:
                        continue
                    arguments.append(
                        {
                            "channel": "liquidation-orders",
                            "instType": "SWAP",
                        }
                    )
                    liquidation_added = True
                    continue
                instrument = (
                    venue_symbol.removesuffix("-SWAP")
                    if channel == "index-tickers"
                    else venue_symbol
                )
                argument = {"channel": channel, "instId": instrument}
                arguments.append(argument)
        return arguments

    async def connect(self) -> None:
        """Load contract metadata, then start the public socket and clock poller."""

        if self._session_task is not None:
            return
        self._stop.clear()
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                base_url=self.config.rest_url,
                timeout=self.config.rest_timeout_seconds,
            )
        await self._load_contract_specifications()
        self._normalizer = OkxNormalizer(self._specifications)
        self._books = OkxOrderBookManager(self._specifications)
        arguments = self._subscription_arguments()
        subscribe = json.dumps(
            {"id": "1512", "op": "subscribe", "args": arguments},
            separators=(",", ":"),
        )
        unsubscribe = json.dumps(
            {"id": "1512", "op": "unsubscribe", "args": arguments},
            separators=(",", ":"),
        )
        self._session = PublicWebSocketSession(
            connection_factory=self._connection_factory(),
            subscribe_messages=(subscribe,),
            unsubscribe_messages=(unsubscribe,),
            message_handler=self._handle_websocket_message,
            heartbeat_policy=TextPingHeartbeat(),
            receive_timeout_seconds=self.config.receive_timeout_seconds,
            heartbeat_timeout_seconds=self.config.heartbeat_timeout_seconds,
            backoff_policy=BackoffPolicy(
                initial_seconds=self.config.reconnect_initial_seconds,
                maximum_seconds=self.config.reconnect_max_seconds,
                jitter_seconds=self.config.reconnect_jitter_seconds,
                stable_reset_seconds=self.config.reconnect_stable_reset_seconds,
            ),
            event_handler=self._handle_session_event,
        )
        self._session_task = asyncio.create_task(
            self._session.run(),
            name="okx-public-websocket",
        )
        self._poller_tasks.append(
            asyncio.create_task(
                self._server_time_loop(),
                name="okx-server-time",
            )
        )

    async def wait(self) -> None:
        tasks = [*self._poller_tasks]
        if self._session_task is not None:
            tasks.append(self._session_task)
        if tasks:
            await asyncio.gather(*tasks)

    async def disconnect(self) -> None:
        self._stop.set()
        if self._session is not None:
            await self._session.stop()
        for task in self._poller_tasks:
            task.cancel()
        tasks: list[asyncio.Task[None]] = [*self._poller_tasks]
        if self._session_task is not None:
            tasks.append(self._session_task)
        await asyncio.gather(*tasks, return_exceptions=True)
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
        self._session = None
        self._session_task = None
        self._poller_tasks.clear()
        for key in self._configured_keys():
            self._health.mark_disconnected(key)
        await super().disconnect()

    def normalize_message(self, payload: dict[str, Any]) -> list[Any]:
        if self._normalizer is None:
            raise RuntimeError("OKX contract metadata must be loaded before normalization")
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
            for channel in self.config.channels:
                instrument = (
                    venue_symbol.removesuffix("-SWAP")
                    if channel == "index-tickers"
                    else venue_symbol
                )
                plans.append(
                    PlannedSubscription(
                        transport="websocket",
                        channel=channel,
                        venue_symbol=instrument,
                        subscription_key=f"{channel}:{instrument}",
                        parameters={
                            "canonical_symbol": canonical,
                            "instType": "SWAP",
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
                        parameters={
                            "canonical_symbol": canonical,
                            "instType": "SWAP",
                        },
                    )
                )
        return plans

    def health_snapshots(self, *, now: datetime | None = None) -> list[StreamHealthSnapshot]:
        checked_at = now or utc_now()
        return [
            self._health.snapshot(key, now=checked_at)
            for key in self._configured_keys()
        ]

    async def _handle_session_event(self, event: SessionEvent) -> None:
        keys = self._websocket_keys()
        if event.kind is SessionEventKind.CONNECTED:
            self._is_connected = True
            for key in keys:
                self._health.mark_connected(key, at=event.timestamp)
            self._begin_book_resync(event.connection_generation)
        elif event.kind is SessionEventKind.SUBSCRIBED and event.connection_generation > 1:
            for key in keys:
                self._health.record_resubscribe(key)
        elif event.kind is SessionEventKind.DISCONNECTED:
            for key in keys:
                self._health.mark_disconnected(key, error=event.reason)
        elif event.kind is SessionEventKind.RECONNECT_SCHEDULED:
            for key in keys:
                self._health.record_reconnect(key, error=event.reason)
        await self._store_internal_event(event)

    def _begin_book_resync(self, connection_generation: int) -> None:
        books = self._books
        if books is None:
            return
        for canonical, venue_symbol in self.config.symbols.items():
            for channel in self.config.channels:
                if channel not in {"books", "books5"}:
                    continue
                book_channel: Literal["books", "books5"] = (
                    "books" if channel == "books" else "books5"
                )
                book = books.book(venue_symbol, channel=book_channel)
                result = book.begin_resync(
                    f"OKX connection generation {connection_generation} awaiting snapshot"
                )
                self._health.mark_resyncing(
                    self._key(canonical, channel),
                    book_generation=result.generation,
                    reason=result.reason or "OKX book snapshot required",
                )

    async def _store_internal_event(self, event: SessionEvent) -> None:
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
                exchange=Exchange.OKX,
                symbol="*",
                channel="_session_public",
                message_kind="connection_lifecycle",
                transport="internal",
                local_receive_timestamp=event.timestamp,
                normalization_timestamp=event.timestamp,
                connection_generation=event.connection_generation,
                raw_payload=payload,
            )
        )

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
        message_kind: str = "market_data",
    ) -> str | None:
        if self._raw_writer is None:
            return None
        record = RawMarketRecord(
            exchange=Exchange.OKX,
            symbol=symbol,
            channel=channel,
            message_kind=message_kind,
            transport=transport,
            exchange_timestamp=exchange_timestamp,
            local_receive_timestamp=received_at,
            sequence_id=sequence_id,
            connection_generation=connection_generation,
            raw_payload=raw_payload,
        )
        await self._raw_writer.write(record)
        return record.raw_payload_reference

    def _websocket_routing(
        self,
        payload: dict[str, Any],
    ) -> tuple[str, str, datetime | None, int | str | None]:
        argument = payload.get("arg")
        if not isinstance(argument, dict):
            return "_control", "*", None, None
        channel = str(argument.get("channel") or "_unknown")
        venue_symbol = argument.get("instId")
        data = payload.get("data")
        first = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else {}
        if not isinstance(venue_symbol, str):
            venue_symbol = first.get("instId")
        symbol = "*"
        if channel != "liquidation-orders" and isinstance(venue_symbol, str):
            try:
                symbol = self._canonical(venue_symbol)
            except ValueError:
                symbol = "*"
        timestamp_value = first.get("ts")
        details = first.get("details")
        if timestamp_value is None and isinstance(details, list) and details:
            detail = details[0]
            if isinstance(detail, dict):
                timestamp_value = detail.get("ts")
        return (
            channel,
            symbol,
            _milliseconds_timestamp(timestamp_value),
            first.get("seqId"),
        )

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
                connection_generation=(
                    self._session.connection_generation if self._session else 0
                ),
                message_kind="parse_error",
            )
            raise SessionProtocolError(f"invalid OKX JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise SessionProtocolError("OKX payload must be an object")
        channel, symbol, exchange_at, sequence = self._websocket_routing(payload)
        generation = self._session.connection_generation if self._session else 0
        reference = await self._persist_payload(
            raw_payload=raw_payload,
            symbol=symbol,
            channel=channel,
            transport="websocket",
            received_at=received_at,
            exchange_timestamp=exchange_at,
            sequence_id=sequence,
            connection_generation=generation,
            message_kind="control" if isinstance(payload.get("event"), str) else "market_data",
        )
        control_event = payload.get("event")
        if isinstance(control_event, str):
            if control_event in {"error", "notice"}:
                raise SessionProtocolError(
                    f"OKX control event {control_event}: "
                    f"{payload.get('code')} {payload.get('msg')}"
                )
            return
        normalization_payload = payload
        if channel == "liquidation-orders":
            data = payload.get("data")
            if not isinstance(data, list):
                raise SessionProtocolError("OKX liquidation payload data must be a list")
            configured_data = [
                item
                for item in data
                if isinstance(item, dict)
                and isinstance(item.get("instId"), str)
                and item["instId"].upper() in self._reverse_symbols
            ]
            if not configured_data:
                return
            normalization_payload = {**payload, "data": configured_data}
        normalizer = self._normalizer
        books = self._books
        if normalizer is None or books is None:
            raise RuntimeError("OKX connector runtime is not initialized")
        parsed = parse_okx_payload(normalization_payload)
        book_generation: int | None = None
        if isinstance(parsed, OkxBooksMessage):
            results = books.ingest_books(parsed)
            venue_symbol = parsed.argument.instrument_id
            if venue_symbol is None:
                raise SessionProtocolError("OKX books payload is missing instId")
            key = self._key(self._canonical(venue_symbol), "books")
            for result in results:
                book_generation = result.generation
                if result.transition is BookTransition.CHECKSUM_MISMATCH:
                    self._health.record_checksum_failure(
                        key,
                        book_generation=result.generation,
                    )
                    raise SessionProtocolError(result.reason or "OKX checksum mismatch")
                if result.transition is BookTransition.SEQUENCE_GAP:
                    self._health.record_sequence_gap(
                        key,
                        book_generation=result.generation,
                    )
                    raise SessionProtocolError(result.reason or "OKX sequence gap")
                if result.status is BookStatus.LIVE:
                    self._health.mark_book_live(
                        key,
                        book_generation=result.generation,
                    )
        elif isinstance(parsed, OkxBooks5Message):
            results = books.ingest_books5(parsed)
            for data, result in zip(parsed.data, results, strict=True):
                book_generation = result.generation
                if result.status is BookStatus.LIVE:
                    self._health.mark_book_live(
                        self._key(self._canonical(data.instrument_id), "books5"),
                        book_generation=result.generation,
                    )
        events = normalizer.normalize(
            normalization_payload,
            context=NormalizationContext(
                local_receive_timestamp=received_at,
                normalization_timestamp=utc_now(),
                raw_payload_reference=reference,
            ),
        )
        if book_generation is not None:
            events = [
                event.model_copy(update={"generation": book_generation})
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
                is_open_interest=health_channel == "open-interest",
            )
            self._record_event(
                exchange_timestamp=event.exchange_timestamp,
                local_receive_timestamp=event.local_receive_timestamp,
            )
            await self._event_sink(event)

    async def _load_contract_specifications(self) -> None:
        client = self._http_client
        if client is None:
            raise RuntimeError("OKX HTTP client is not initialized")
        specifications: dict[str, ContractSpecification] = {}
        for canonical, venue_symbol in self.config.symbols.items():
            received_at = utc_now()
            response = await client.get(
                "/api/v5/public/instruments",
                params={"instType": "SWAP", "instId": venue_symbol},
            )
            received_at = utc_now()
            response.raise_for_status()
            payload = response.json()
            reference = await self._persist_payload(
                raw_payload=response.content,
                symbol=canonical,
                channel="instrument_metadata",
                transport="rest",
                received_at=received_at,
                exchange_timestamp=None,
                sequence_id=None,
                connection_generation=0,
            )
            specifications.update(contract_specifications_from_response(payload))
            key = self._key(canonical, "instrument_metadata")
            self._health.mark_connected(key, at=received_at)
            self._health.record_rest_result(key, healthy=True)
            if reference is None and self._raw_writer is not None:
                raise RuntimeError("OKX metadata raw payload was not persisted")
        missing = set(self.config.symbols.values()) - set(specifications)
        if missing:
            raise ValueError(f"OKX metadata missing configured instruments: {sorted(missing)}")
        self._specifications = specifications

    async def _poll_server_time(self) -> None:
        client = self._http_client
        if client is None:
            raise RuntimeError("OKX HTTP client is not initialized")
        sent_at = utc_now()
        response = await client.get("/api/v5/public/time")
        received_at = utc_now()
        response.raise_for_status()
        payload = response.json()
        data = payload.get("data")
        first = data[0] if isinstance(data, list) and data and isinstance(data[0], dict) else {}
        server_at = _milliseconds_timestamp(first.get("ts"))
        if payload.get("code") != "0" or server_at is None:
            raise ValueError("OKX server time response is invalid")
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
        for key in self._configured_keys():
            self._health.record_rest_result(key, healthy=True)

    async def _server_time_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_server_time()
            except (httpx.HTTPError, ValueError, KeyError) as exc:
                for key in self._configured_keys():
                    self._health.record_rest_result(key, healthy=False, error=str(exc))
                self._logger.exception(
                    "OKX server-time poll failed",
                    extra={
                        "event": "rest_poll_failed",
                        "exchange": Exchange.OKX.value,
                    },
                )
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=60)
            except TimeoutError:
                continue

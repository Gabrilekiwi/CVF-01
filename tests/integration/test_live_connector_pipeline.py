"""Network-free end-to-end connector pipeline tests with public fixtures."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from cvf.config import load_settings
from cvf.exchanges.binance import BinanceMarketDataConnector
from cvf.exchanges.okx import OKXMarketDataConnector
from cvf.exchanges.session import WebSocketTransport
from cvf.models import LiquidationEvent, OrderBookSnapshot, OrderBookUpdate, Trade
from cvf.normalization.common import NormalizedMarketEvent
from cvf.storage import RawMarketRecord

FIXTURES = Path(__file__).parents[1] / "fixtures"
RECEIVED_AT = datetime(2026, 7, 24, 0, 7, tzinfo=UTC)


class MemoryRawWriter:
    def __init__(self) -> None:
        self.records: list[RawMarketRecord] = []

    async def write(self, record: RawMarketRecord) -> None:
        self.records.append(record)


class BlockingTransport:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.closed = asyncio.Event()

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        await self.closed.wait()
        raise ConnectionError("closed")

    async def ping(self) -> None:
        return None

    async def close(self) -> None:
        self.closed.set()


def fixture_bytes(exchange: str, filename: str) -> bytes:
    return (FIXTURES / exchange / filename).read_bytes()


@pytest.mark.asyncio
async def test_binance_raw_normalize_deduplicate_pipeline() -> None:
    settings = load_settings(environ={})
    raw_writer = MemoryRawWriter()
    events: list[NormalizedMarketEvent] = []

    async def sink(event: NormalizedMarketEvent) -> None:
        events.append(event)

    connector = BinanceMarketDataConnector(
        settings.exchanges.binance,
        stale_after_ms=settings.health.stale_after_ms,
        raw_writer=raw_writer,
        event_sink=sink,
    )
    payload = fixture_bytes("binance", "agg_trade_live.json")

    await connector.process_websocket_message(
        payload,
        local_receive_timestamp=RECEIVED_AT,
    )
    await connector.process_websocket_message(
        payload,
        local_receive_timestamp=RECEIVED_AT,
    )

    assert len(raw_writer.records) == 2
    assert all(record.raw_payload == payload for record in raw_writer.records)
    assert len(events) == 1
    assert isinstance(events[0], Trade)
    trade_health = next(
        snapshot
        for snapshot in connector.health_snapshots(now=RECEIVED_AT)
        if snapshot.key.symbol == "BTC-USDT-PERP"
        and snapshot.key.channel == "aggTrade"
    )
    assert trade_health.message_count == 1
    assert trade_health.duplicate_count == 1


@pytest.mark.asyncio
async def test_okx_metadata_session_books_raw_and_deduplication() -> None:
    settings = load_settings(environ={})
    raw_writer = MemoryRawWriter()
    events: list[NormalizedMarketEvent] = []
    transport = BlockingTransport()

    async def sink(event: NormalizedMarketEvent) -> None:
        events.append(event)

    async def connection_factory() -> WebSocketTransport:
        return transport

    def factory_builder(_url: str):
        return connection_factory

    instrument_payloads = {
        "BTC-USDT-SWAP": fixture_bytes("okx", "instrument_btc_live.json"),
        "ETH-USDT-SWAP": fixture_bytes("okx", "instrument_eth_live.json"),
    }

    def http_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v5/public/instruments":
            instrument = request.url.params["instId"]
            return httpx.Response(200, content=instrument_payloads[instrument])
        if request.url.path == "/api/v5/public/time":
            return httpx.Response(
                200,
                json={"code": "0", "msg": "", "data": [{"ts": "1784851580000"}]},
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with httpx.AsyncClient(
        base_url=settings.exchanges.okx.rest_url,
        transport=httpx.MockTransport(http_handler),
    ) as client:
        connector = OKXMarketDataConnector(
            settings.exchanges.okx,
            stale_after_ms=settings.health.stale_after_ms,
            raw_writer=raw_writer,
            event_sink=sink,
            http_client=client,
            connection_factory_builder=factory_builder,
        )
        await connector.connect()
        await asyncio.sleep(0)

        trade = fixture_bytes("okx", "trade_live.json")
        await connector.process_websocket_message(
            trade,
            local_receive_timestamp=RECEIVED_AT,
        )
        await connector.process_websocket_message(
            trade,
            local_receive_timestamp=RECEIVED_AT,
        )
        await connector.process_websocket_message(
            fixture_bytes("okx", "books_snapshot_official.json"),
            local_receive_timestamp=RECEIVED_AT,
        )
        await connector.process_websocket_message(
            fixture_bytes("okx", "books_update_official.json"),
            local_receive_timestamp=RECEIVED_AT,
        )
        liquidation = json.loads(
            fixture_bytes("okx", "liquidation_official.json")
        )
        unknown = {**liquidation["data"][0], "instId": "INJ-USDT-SWAP"}
        liquidation["data"].insert(0, unknown)
        liquidation_bytes = json.dumps(liquidation).encode("utf-8")
        await connector.process_websocket_message(
            liquidation_bytes,
            local_receive_timestamp=RECEIVED_AT,
        )

        subscription = json.loads(transport.sent[0])
        assert subscription["id"] == "1512"
        assert subscription["op"] == "subscribe"
        assert any(argument["channel"] == "books" for argument in subscription["args"])
        liquidation_arguments = [
            argument
            for argument in subscription["args"]
            if argument["channel"] == "liquidation-orders"
        ]
        assert liquidation_arguments == [
            {"channel": "liquidation-orders", "instType": "SWAP"}
        ]
        assert sum(isinstance(event, Trade) for event in events) == 1
        assert any(isinstance(event, OrderBookSnapshot) for event in events)
        assert any(isinstance(event, OrderBookUpdate) for event in events)
        assert sum(isinstance(event, LiquidationEvent) for event in events) == 1
        assert any(record.raw_payload == trade for record in raw_writer.records)
        liquidation_record = next(
            record
            for record in raw_writer.records
            if record.raw_payload == liquidation_bytes
        )
        assert liquidation_record.symbol == "*"
        trade_health = next(
            snapshot
            for snapshot in connector.health_snapshots(now=RECEIVED_AT)
            if snapshot.key.symbol == "BTC-USDT-PERP"
            and snapshot.key.channel == "trades"
        )
        assert trade_health.message_count == 1
        assert trade_health.duplicate_count == 1

        await connector.disconnect()

    assert transport.closed.is_set()

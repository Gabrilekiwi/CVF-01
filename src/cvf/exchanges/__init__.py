"""Venue connectors and exact symbol mappings."""

from typing import TYPE_CHECKING, Any

from cvf.exchanges.base import (
    ConnectorNotImplementedError,
    ExchangeConnector,
    PlannedSubscription,
)
from cvf.exchanges.deduplication import BoundedTTLDeduplicator
from cvf.exchanges.session import (
    BackoffPolicy,
    HeartbeatTimeout,
    ProtocolPingHeartbeat,
    PublicWebSocketSession,
    SessionEvent,
    SessionEventKind,
    SessionProtocolError,
    TextPingHeartbeat,
    WebSocketTransport,
    websocket_connection_factory,
)

if TYPE_CHECKING:
    from cvf.exchanges.binance import BinanceMarketDataConnector
    from cvf.exchanges.okx import OKXMarketDataConnector


def __getattr__(name: str) -> Any:
    """Load connector runtimes lazily to keep normalization imports acyclic."""

    if name == "BinanceMarketDataConnector":
        from cvf.exchanges.binance import BinanceMarketDataConnector

        return BinanceMarketDataConnector
    if name == "OKXMarketDataConnector":
        from cvf.exchanges.okx import OKXMarketDataConnector

        return OKXMarketDataConnector
    raise AttributeError(name)


__all__ = [
    "BackoffPolicy",
    "BinanceMarketDataConnector",
    "BoundedTTLDeduplicator",
    "ConnectorNotImplementedError",
    "ExchangeConnector",
    "HeartbeatTimeout",
    "OKXMarketDataConnector",
    "PlannedSubscription",
    "ProtocolPingHeartbeat",
    "PublicWebSocketSession",
    "SessionEvent",
    "SessionEventKind",
    "SessionProtocolError",
    "TextPingHeartbeat",
    "WebSocketTransport",
    "websocket_connection_factory",
]

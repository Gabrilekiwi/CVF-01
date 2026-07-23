"""OKX public market-data connector skeleton."""

from __future__ import annotations

from typing import Any

from cvf.config import ExchangeConnectionConfig
from cvf.exchanges.base import (
    ConnectorNotImplementedError,
    ExchangeConnector,
    PlannedSubscription,
)
from cvf.models.enums import Exchange


class OKXMarketDataConnector(ExchangeConnector):
    """Subscription planner for OKX; network I/O arrives in phase 2."""

    def __init__(self, config: ExchangeConnectionConfig, *, stale_after_ms: int) -> None:
        super().__init__(
            exchange=Exchange.OKX,
            config=config,
            stale_after_ms=stale_after_ms,
        )

    async def connect(self) -> None:
        raise ConnectorNotImplementedError(
            "OKX network collection is intentionally deferred to phase 2"
        )

    def normalize_message(self, payload: dict[str, Any]) -> list[Any]:
        raise ConnectorNotImplementedError(
            "OKX payload normalization is intentionally deferred to phase 2"
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


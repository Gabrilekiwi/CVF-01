"""Exchange connector contract and shared health-state behavior."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import Field

from cvf.config import ExchangeConnectionConfig
from cvf.models.common import FrozenModel
from cvf.models.enums import Exchange, HealthStatus
from cvf.models.market import ExchangeHealth


class ConnectorNotImplementedError(NotImplementedError):
    """Raised when a phase-1 connector skeleton is asked to access the network."""


class PlannedSubscription(FrozenModel):
    """A human- and machine-readable subscription planned for phase 2."""

    transport: Literal["websocket", "rest"]
    channel: str
    venue_symbol: str
    subscription_key: str
    parameters: dict[str, str | int | float | bool] = Field(default_factory=dict)


class ExchangeConnector(ABC):
    """Network-agnostic contract implemented by every venue connector."""

    def __init__(
        self,
        *,
        exchange: Exchange,
        config: ExchangeConnectionConfig,
        stale_after_ms: int,
    ) -> None:
        if exchange not in {Exchange.BINANCE, Exchange.OKX}:
            raise ValueError("a market-data connector must represent BINANCE or OKX")
        self.exchange = exchange
        self.config = config
        self.stale_after_ms = stale_after_ms
        self._status = HealthStatus.DISCONNECTED
        self._is_connected = False
        self._last_event_timestamp: datetime | None = None
        self._last_receive_timestamp: datetime | None = None
        self._last_latency_ms: float | None = None
        self._last_error: str | None = None
        self._sequence_gap_detected = False
        self._duplicate_events = 0
        self._resyncing = False

    @abstractmethod
    async def connect(self) -> None:
        """Connect and subscribe. Phase-1 implementations fail explicitly."""

    async def disconnect(self) -> None:
        """Release connector resources and mark the connector disconnected."""

        self._is_connected = False
        self._resyncing = False
        self._status = HealthStatus.DISCONNECTED

    @abstractmethod
    def normalize_message(self, payload: dict[str, Any]) -> list[Any]:
        """Convert one venue payload into zero or more normalized events."""

    @abstractmethod
    def planned_subscriptions(self) -> list[PlannedSubscription]:
        """Return the channels and instruments the connector will subscribe to."""

    async def health_check(self) -> ExchangeHealth:
        """Return a point-in-time health snapshot without contacting the venue."""

        now = datetime.now(UTC)
        status = self._status
        if (
            self._is_connected
            and self._last_receive_timestamp is not None
            and (now - self._last_receive_timestamp).total_seconds() * 1000 > self.stale_after_ms
        ):
            status = HealthStatus.STALE

        return ExchangeHealth(
            exchange=self.exchange,
            symbol="*",
            exchange_timestamp=now,
            local_receive_timestamp=now,
            sequence_id=None,
            raw_payload_reference=None,
            status=status,
            is_connected=self._is_connected,
            last_event_timestamp=self._last_event_timestamp,
            last_latency_ms=self._last_latency_ms,
            duplicate_events=self._duplicate_events,
            sequence_gap_detected=self._sequence_gap_detected,
            resyncing=self._resyncing,
            rest_healthy=False,
            open_interest_stale=self._last_event_timestamp is None,
            last_error=self._last_error,
            details={
                "phase": "connector_skeleton",
                "planned_subscription_count": len(self.planned_subscriptions()),
                "network_attempted": False,
            },
        )

    def _record_event(
        self,
        *,
        exchange_timestamp: datetime,
        local_receive_timestamp: datetime,
    ) -> None:
        """Update shared health state after a validated event (for phase 2)."""

        self._last_event_timestamp = exchange_timestamp
        self._last_receive_timestamp = local_receive_timestamp
        self._last_latency_ms = (
            local_receive_timestamp - exchange_timestamp
        ).total_seconds() * 1000
        self._is_connected = True
        self._status = HealthStatus.CONNECTED

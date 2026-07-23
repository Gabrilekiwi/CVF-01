"""Common metadata and constrained scalar types for normalized records."""

from __future__ import annotations

import math
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

from cvf.models.enums import EventType, Exchange
from cvf.utils.validation import validate_canonical_symbol

PositiveDecimal = Annotated[Decimal, Field(gt=0)]
NonNegativeDecimal = Annotated[Decimal, Field(ge=0)]


class FrozenModel(BaseModel):
    """Immutable strict model used for value objects."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


class EventBase(FrozenModel):
    """Metadata present on every normalized event and paper-trading record."""

    exchange: Exchange
    symbol: str
    exchange_timestamp: datetime
    local_receive_timestamp: datetime
    event_type: EventType
    sequence_id: int | str | None = None
    raw_payload_reference: str | None = None

    @field_validator("symbol")
    @classmethod
    def symbol_is_canonical(cls, value: str) -> str:
        return validate_canonical_symbol(value, allow_wildcard=True)

    @field_validator("exchange_timestamp", "local_receive_timestamp")
    @classmethod
    def timestamp_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def wildcard_is_exchange_health_only(self) -> EventBase:
        if self.symbol == "*" and self.event_type is not EventType.EXCHANGE_HEALTH:
            raise ValueError("symbol='*' is reserved for exchange-wide health events")
        return self

    @computed_field  # type: ignore[prop-decorator]
    @property
    def receive_latency_ms(self) -> float:
        """Observed receive latency; may be negative when exchange clocks are skewed."""

        delta = self.local_receive_timestamp - self.exchange_timestamp
        return delta.total_seconds() * 1000.0


def ensure_finite_number(value: float | int | Decimal | None) -> float | int | Decimal | None:
    """Reject NaN/Infinity while preserving the supplied numeric type."""

    if value is not None and not math.isfinite(float(value)):
        raise ValueError("feature values must be finite")
    return value

"""Shared types and deterministic helpers for venue payload normalization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TypedDict

from cvf.models.enums import Exchange
from cvf.models.market import (
    BestBidAsk,
    ExchangeHealth,
    FundingRate,
    IndexPrice,
    LiquidationEvent,
    MarkPrice,
    OpenInterest,
    OrderBookSnapshot,
    OrderBookUpdate,
    Trade,
)

type NormalizedMarketEvent = (
    Trade
    | OrderBookSnapshot
    | OrderBookUpdate
    | BestBidAsk
    | OpenInterest
    | FundingRate
    | MarkPrice
    | IndexPrice
    | LiquidationEvent
    | ExchangeHealth
)


class PayloadNormalizationError(ValueError):
    """Base error for invalid or unsupported public venue payloads."""


class UnsupportedPayloadError(PayloadNormalizationError):
    """Raised when a payload does not belong to a supported public channel."""


@dataclass(frozen=True, slots=True)
class NormalizationContext:
    """Caller-supplied timing and lineage for deterministic normalization."""

    local_receive_timestamp: datetime
    normalization_timestamp: datetime
    raw_payload_reference: str | None = None
    canonical_symbol: str | None = None

    def __post_init__(self) -> None:
        timestamps = (
            ("local_receive_timestamp", self.local_receive_timestamp),
            ("normalization_timestamp", self.normalization_timestamp),
        )
        for field_name, value in timestamps:
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{field_name} must be timezone-aware")
            object.__setattr__(self, field_name, value.astimezone(UTC))


class EventMetadata(TypedDict):
    """Typed keyword metadata shared by every normalized event constructor."""

    exchange: Exchange
    symbol: str
    exchange_timestamp: datetime
    local_receive_timestamp: datetime
    normalization_timestamp: datetime
    sequence_id: int | str | None
    raw_payload_reference: str | None


def timestamp_from_milliseconds(value: int | str) -> datetime:
    """Convert an exchange Unix-millisecond timestamp to timezone-aware UTC."""

    try:
        milliseconds = int(value)
    except (TypeError, ValueError) as exc:
        raise PayloadNormalizationError(f"invalid millisecond timestamp: {value!r}") from exc
    return datetime.fromtimestamp(milliseconds / 1000, tz=UTC)


def decimal_from_text(value: str, *, field_name: str) -> Decimal:
    """Parse a finite decimal without passing through binary floating point."""

    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise PayloadNormalizationError(f"{field_name} is not a decimal: {value!r}") from exc
    if not parsed.is_finite():
        raise PayloadNormalizationError(f"{field_name} must be finite")
    return parsed

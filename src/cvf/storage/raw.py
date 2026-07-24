"""Lossless raw market-data records with stable normalized-event references."""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator

from cvf.models.common import FrozenModel
from cvf.models.enums import Exchange
from cvf.utils.validation import validate_canonical_symbol


class RawMarketRecord(FrozenModel):
    """One exact public payload plus receipt and routing metadata."""

    record_id: UUID = Field(default_factory=uuid4)
    exchange: Exchange
    symbol: str
    channel: str
    message_kind: str
    transport: Literal["websocket", "rest", "internal"]
    exchange_timestamp: datetime | None = None
    local_receive_timestamp: datetime
    normalization_timestamp: datetime | None = None
    sequence_id: int | str | None = None
    connection_generation: int = Field(default=0, ge=0)
    raw_payload: bytes

    @property
    def raw_payload_reference(self) -> str:
        return f"raw://{self.record_id}"

    @field_validator("symbol")
    @classmethod
    def symbol_is_canonical(cls, value: str) -> str:
        return validate_canonical_symbol(value, allow_wildcard=True)

    @field_validator("channel", "message_kind")
    @classmethod
    def text_is_not_empty(cls, value: str) -> str:
        if not value or value.isspace():
            raise ValueError("raw record routing fields cannot be empty")
        return value

    @field_validator(
        "exchange_timestamp",
        "local_receive_timestamp",
        "normalization_timestamp",
    )
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("raw record timestamps must be timezone-aware")
        return value

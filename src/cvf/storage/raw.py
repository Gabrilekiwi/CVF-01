"""Lossless raw market-data records with stable normalized-event references."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, Literal
from uuid import UUID, uuid4

from pydantic import Field, field_validator

from cvf.models.common import FrozenModel
from cvf.models.enums import Exchange
from cvf.utils.fingerprint import model_payload_json
from cvf.utils.validation import validate_canonical_symbol

if TYPE_CHECKING:
    from cvf.normalization.common import NormalizedMarketEvent


NORMALIZED_EVENT_JOURNAL_CHANNEL = "_normalized_event"
FEATURE_TIMELINE_END_MESSAGE_KIND = "feature_timeline_end"
_FEATURE_TIMELINE_SCHEMA_VERSION: Final[Literal[1]] = 1


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


def normalized_event_journal_record(
    event: NormalizedMarketEvent,
) -> RawMarketRecord:
    """Persist one exact post-dedup normalized event for deterministic replay."""

    generation = getattr(event, "generation", 0)
    if not isinstance(generation, int):
        generation = 0
    return RawMarketRecord(
        exchange=event.exchange,
        symbol=event.symbol,
        channel=NORMALIZED_EVENT_JOURNAL_CHANNEL,
        message_kind="normalized_event",
        transport="internal",
        exchange_timestamp=event.exchange_timestamp,
        local_receive_timestamp=event.local_receive_timestamp,
        normalization_timestamp=event.normalization_timestamp,
        sequence_id=event.sequence_id,
        connection_generation=generation,
        raw_payload=model_payload_json(event).encode("utf-8"),
    )


class FeatureTimelineEnd(FrozenModel):
    """One cleanly completed live feature-timeline watermark."""

    schema_version: Literal[1] = _FEATURE_TIMELINE_SCHEMA_VERSION
    through_timestamp: datetime

    @field_validator("through_timestamp")
    @classmethod
    def through_timestamp_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("feature timeline end timestamp must be timezone-aware")
        return value.astimezone(UTC)


def feature_timeline_end_journal_record(
    through_timestamp: datetime,
) -> RawMarketRecord:
    """Persist the exact clean-shutdown watermark needed by journal replay."""

    marker = FeatureTimelineEnd(through_timestamp=through_timestamp)
    return RawMarketRecord(
        exchange=Exchange.BINANCE,
        symbol="*",
        channel=NORMALIZED_EVENT_JOURNAL_CHANNEL,
        message_kind=FEATURE_TIMELINE_END_MESSAGE_KIND,
        transport="internal",
        local_receive_timestamp=marker.through_timestamp,
        normalization_timestamp=marker.through_timestamp,
        connection_generation=0,
        raw_payload=model_payload_json(marker).encode("utf-8"),
    )


def feature_timeline_end_timestamp(
    record: RawMarketRecord,
) -> datetime | None:
    """Validate and return a clean timeline watermark, or None for other rows."""

    if record.message_kind != FEATURE_TIMELINE_END_MESSAGE_KIND:
        return None
    if (
        record.channel != NORMALIZED_EVENT_JOURNAL_CHANNEL
        or record.transport != "internal"
        or record.symbol != "*"
        or record.exchange is not Exchange.BINANCE
        or record.exchange_timestamp is not None
        or record.sequence_id is not None
        or record.connection_generation != 0
    ):
        raise ValueError("invalid feature-timeline end routing")
    marker = FeatureTimelineEnd.model_validate_json(record.raw_payload)
    through_timestamp = marker.through_timestamp.astimezone(UTC)
    if (
        record.local_receive_timestamp.astimezone(UTC) != through_timestamp
        or record.normalization_timestamp is None
        or record.normalization_timestamp.astimezone(UTC) != through_timestamp
    ):
        raise ValueError("feature-timeline end metadata mismatch")
    return through_timestamp

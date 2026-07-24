"""Stable ordering rules shared by offline replay and compaction."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from cvf.storage.raw import RawMarketRecord


class ReplayOrder(StrEnum):
    EVENT_TIME = "event-time"
    RECEIVE_TIME = "receive-time"


def replay_timestamp(record: RawMarketRecord, order: ReplayOrder) -> datetime:
    if order is ReplayOrder.EVENT_TIME and record.exchange_timestamp is not None:
        return record.exchange_timestamp.astimezone(UTC)
    return record.local_receive_timestamp.astimezone(UTC)


def stable_record_key(
    record: RawMarketRecord,
    order: ReplayOrder = ReplayOrder.EVENT_TIME,
) -> tuple[int, datetime, datetime, int, str, str, str, str, str]:
    """Sort equal timestamps without relying on filesystem or task iteration order."""

    return (
        0 if record.channel == "instrument_metadata" else 1,
        replay_timestamp(record, order),
        record.local_receive_timestamp.astimezone(UTC),
        record.connection_generation,
        record.exchange.value,
        record.symbol,
        record.channel,
        "" if record.sequence_id is None else str(record.sequence_id),
        str(record.record_id),
    )

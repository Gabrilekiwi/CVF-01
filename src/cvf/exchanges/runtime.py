"""Shared helpers for live public market-data connector runtimes."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

from cvf.normalization.common import NormalizedMarketEvent
from cvf.storage.raw import RawMarketRecord


class RawRecordWriter(Protocol):
    async def write(self, record: RawMarketRecord) -> None: ...


type NormalizedEventSink = Callable[[NormalizedMarketEvent], Awaitable[None]]


async def discard_event(_event: NormalizedMarketEvent) -> None:
    return None


def exact_message_bytes(message: str | bytes) -> bytes:
    return message if isinstance(message, bytes) else message.encode("utf-8")


def decode_json_message(message: str | bytes) -> object:
    return json.loads(exact_message_bytes(message))


def normalized_event_identity(event: NormalizedMarketEvent) -> str:
    """Hash stable venue semantics while excluding local processing metadata."""

    payload = event.model_dump(
        mode="json",
        exclude={
            "local_receive_timestamp",
            "normalization_timestamp",
            "raw_payload_reference",
            "receive_latency_ms",
        },
    )
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def utc_now() -> datetime:
    return datetime.now(UTC)

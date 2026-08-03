"""Re-run exact raw JSON bytes through the live venue normalizers."""

from __future__ import annotations

import json
from datetime import UTC
from typing import Annotated

from pydantic import Field, TypeAdapter

from cvf.models.enums import Exchange
from cvf.normalization.binance import BinanceNormalizer
from cvf.normalization.common import NormalizationContext, NormalizedMarketEvent
from cvf.normalization.instruments import ContractSpecification
from cvf.normalization.okx import (
    OkxNormalizer,
    contract_specifications_from_response,
)
from cvf.storage.raw import (
    FEATURE_TIMELINE_END_MESSAGE_KIND,
    NORMALIZED_EVENT_JOURNAL_CHANNEL,
    RawMarketRecord,
    feature_timeline_end_timestamp,
)

_NON_MARKET_CHANNELS = {
    "_unparsed",
    "server_time",
}
_NORMALIZED_EVENT_ADAPTER: TypeAdapter[NormalizedMarketEvent] = TypeAdapter(
    Annotated[NormalizedMarketEvent, Field(discriminator="event_type")]
)


class RawRecordNormalizer:
    """Stateful replay normalizer, including OKX public contract metadata."""

    def __init__(self) -> None:
        self._binance = BinanceNormalizer()
        self._okx_specifications: dict[str, ContractSpecification] = {}
        self._okx = OkxNormalizer(self._okx_specifications)

    def normalize(self, record: RawMarketRecord) -> list[NormalizedMarketEvent]:
        if record.channel == NORMALIZED_EVENT_JOURNAL_CHANNEL:
            if record.message_kind == FEATURE_TIMELINE_END_MESSAGE_KIND:
                feature_timeline_end_timestamp(record)
                return []
            if (
                record.message_kind != "normalized_event"
                or record.transport != "internal"
            ):
                raise ValueError("invalid normalized-event journal routing")
            event = _NORMALIZED_EVENT_ADAPTER.validate_json(record.raw_payload)
            event_generation = getattr(event, "generation", 0)
            if not isinstance(event_generation, int):
                event_generation = 0
            metadata_matches = (
                event.exchange is record.exchange
                and event.symbol == record.symbol
                and event.exchange_timestamp == record.exchange_timestamp
                and event.local_receive_timestamp == record.local_receive_timestamp
                and event.normalization_timestamp == record.normalization_timestamp
                and (
                    None if event.sequence_id is None else str(event.sequence_id)
                )
                == (
                    None
                    if record.sequence_id is None
                    else str(record.sequence_id)
                )
                and event_generation == record.connection_generation
            )
            if not metadata_matches:
                raise ValueError("normalized-event journal metadata mismatch")
            return [event]
        if (
            record.message_kind != "market_data"
            or record.transport == "internal"
            or record.channel in _NON_MARKET_CHANNELS
        ):
            return []
        payload = json.loads(record.raw_payload)
        context = NormalizationContext(
            local_receive_timestamp=record.local_receive_timestamp,
            normalization_timestamp=(
                record.normalization_timestamp
                or record.local_receive_timestamp.astimezone(UTC)
            ),
            raw_payload_reference=record.raw_payload_reference,
            canonical_symbol=None if record.symbol == "*" else record.symbol,
        )
        if record.exchange is Exchange.BINANCE:
            return self._binance.normalize(payload, context=context)
        if record.exchange is Exchange.OKX:
            if record.channel == "instrument_metadata":
                specifications = contract_specifications_from_response(payload)
                self._okx_specifications.update(specifications)
                self._okx = OkxNormalizer(self._okx_specifications)
                return []
            if record.channel == "liquidation-orders" and isinstance(payload, dict):
                data = payload.get("data")
                if isinstance(data, list):
                    configured_data = [
                        item
                        for item in data
                        if isinstance(item, dict)
                        and isinstance(item.get("instId"), str)
                        and item["instId"].upper() in self._okx_specifications
                    ]
                    if not configured_data:
                        return []
                    payload = {**payload, "data": configured_data}
            return self._okx.normalize(payload, context=context)
        return []

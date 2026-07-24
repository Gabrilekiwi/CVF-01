"""Re-run exact raw JSON bytes through the live venue normalizers."""

from __future__ import annotations

import json
from datetime import UTC

from cvf.models.enums import Exchange
from cvf.normalization.binance import BinanceNormalizer
from cvf.normalization.common import NormalizationContext, NormalizedMarketEvent
from cvf.normalization.okx import (
    OkxNormalizer,
    contract_specifications_from_response,
)
from cvf.storage.raw import RawMarketRecord

_NON_MARKET_CHANNELS = {
    "_unparsed",
    "server_time",
}


class RawRecordNormalizer:
    """Stateful replay normalizer, including OKX public contract metadata."""

    def __init__(self) -> None:
        self._binance = BinanceNormalizer()
        self._okx = OkxNormalizer({})

    def normalize(self, record: RawMarketRecord) -> list[NormalizedMarketEvent]:
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
                self._okx = OkxNormalizer(specifications)
                return []
            return self._okx.normalize(payload, context=context)
        return []

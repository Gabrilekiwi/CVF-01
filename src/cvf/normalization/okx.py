"""Strongly typed OKX v5 public payload parsing and normalization."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cvf.exchanges.symbols import to_canonical_symbol
from cvf.models.enums import AggressorSide, Exchange, LiquidatedPositionSide
from cvf.models.market import (
    FundingRate,
    IndexPrice,
    LiquidationEvent,
    MarkPrice,
    OpenInterest,
    OrderBookLevel,
    OrderBookSnapshot,
    OrderBookUpdate,
    Trade,
)
from cvf.normalization.common import (
    EventMetadata,
    NormalizationContext,
    NormalizedMarketEvent,
    UnsupportedPayloadError,
    decimal_from_text,
    timestamp_from_milliseconds,
)
from cvf.normalization.instruments import ContractSpecification

type OkxBookLevelText = tuple[str, str, str, str]


class OkxPayloadModel(BaseModel):
    """Base model that tolerates additive OKX fields while typing known fields."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class OkxArgument(OkxPayloadModel):
    channel: str
    instrument_id: str | None = Field(default=None, alias="instId")
    instrument_type: str | None = Field(default=None, alias="instType")


class OkxTradeEntry(OkxPayloadModel):
    instrument_id: str = Field(alias="instId")
    trade_id: str = Field(alias="tradeId")
    price: str = Field(alias="px")
    size_contracts: str = Field(alias="sz")
    side: Literal["buy", "sell"]
    timestamp_ms: str = Field(alias="ts")
    count: str
    source: str
    sequence_id: int = Field(alias="seqId")


class OkxTradeMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxTradeEntry]


class OkxBookData(OkxPayloadModel):
    asks: list[OkxBookLevelText]
    bids: list[OkxBookLevelText]
    timestamp_ms: str = Field(alias="ts")
    checksum: int
    sequence_id: int = Field(alias="seqId")
    previous_sequence_id: int = Field(alias="prevSeqId")


class OkxBooksMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    action: Literal["snapshot", "update"]
    data: list[OkxBookData]


class OkxBooks5Data(OkxPayloadModel):
    asks: list[OkxBookLevelText]
    bids: list[OkxBookLevelText]
    instrument_id: str = Field(alias="instId")
    timestamp_ms: str = Field(alias="ts")
    sequence_id: int = Field(alias="seqId")


class OkxBooks5Message(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxBooks5Data]


class OkxOpenInterestEntry(OkxPayloadModel):
    instrument_id: str = Field(alias="instId")
    instrument_type: str = Field(alias="instType")
    contracts: str = Field(alias="oi")
    base_quantity: str = Field(alias="oiCcy")
    quote_notional: str = Field(alias="oiUsd")
    timestamp_ms: str = Field(alias="ts")


class OkxOpenInterestMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxOpenInterestEntry]


class OkxFundingRateEntry(OkxPayloadModel):
    instrument_id: str = Field(alias="instId")
    instrument_type: str = Field(alias="instType")
    funding_rate: str = Field(alias="fundingRate")
    funding_time_ms: str = Field(alias="fundingTime")
    next_funding_time_ms: str = Field(alias="nextFundingTime")
    timestamp_ms: str = Field(alias="ts")


class OkxFundingRateMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxFundingRateEntry]


class OkxMarkPriceEntry(OkxPayloadModel):
    instrument_id: str = Field(alias="instId")
    instrument_type: str = Field(alias="instType")
    mark_price: str = Field(alias="markPx")
    timestamp_ms: str = Field(alias="ts")


class OkxMarkPriceMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxMarkPriceEntry]


class OkxIndexTickerEntry(OkxPayloadModel):
    instrument_id: str = Field(alias="instId")
    index_price: str = Field(alias="idxPx")
    timestamp_ms: str = Field(alias="ts")


class OkxIndexTickerMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxIndexTickerEntry]


class OkxLiquidationDetail(OkxPayloadModel):
    bankruptcy_price: str = Field(alias="bkPx")
    position_side: Literal["long", "short"] = Field(alias="posSide")
    side: Literal["buy", "sell"]
    size_contracts: str = Field(alias="sz")
    timestamp_ms: str = Field(alias="ts")


class OkxLiquidationData(OkxPayloadModel):
    details: list[OkxLiquidationDetail]
    instrument_id: str = Field(alias="instId")
    instrument_type: str = Field(alias="instType")


class OkxLiquidationMessage(OkxPayloadModel):
    argument: OkxArgument = Field(alias="arg")
    data: list[OkxLiquidationData]


class OkxInstrumentEntry(OkxPayloadModel):
    instrument_id: str = Field(alias="instId")
    instrument_type: Literal["SWAP"] = Field(alias="instType")
    contract_type: Literal["linear"] = Field(alias="ctType")
    contract_value: str = Field(alias="ctVal")
    contract_multiplier: str = Field(alias="ctMult")
    contract_value_currency: str = Field(alias="ctValCcy")
    state: str


class OkxInstrumentResponse(OkxPayloadModel):
    code: Literal["0"]
    message: str = Field(alias="msg")
    data: list[OkxInstrumentEntry]


type OkxParsedPayload = (
    OkxTradeMessage
    | OkxBooksMessage
    | OkxBooks5Message
    | OkxOpenInterestMessage
    | OkxFundingRateMessage
    | OkxMarkPriceMessage
    | OkxIndexTickerMessage
    | OkxLiquidationMessage
    | OkxInstrumentResponse
)


def parse_okx_payload(payload: object) -> OkxParsedPayload:
    """Validate one supported OKX v5 public WebSocket or REST payload."""

    if not isinstance(payload, Mapping):
        raise UnsupportedPayloadError("OKX payload must be a JSON object")

    argument = payload.get("arg")
    if isinstance(argument, Mapping):
        channel = argument.get("channel")
        if channel == "trades":
            return OkxTradeMessage.model_validate(payload)
        if channel == "books":
            return OkxBooksMessage.model_validate(payload)
        if channel == "books5":
            return OkxBooks5Message.model_validate(payload)
        if channel == "open-interest":
            return OkxOpenInterestMessage.model_validate(payload)
        if channel == "funding-rate":
            return OkxFundingRateMessage.model_validate(payload)
        if channel == "mark-price":
            return OkxMarkPriceMessage.model_validate(payload)
        if channel == "index-tickers":
            return OkxIndexTickerMessage.model_validate(payload)
        if channel == "liquidation-orders":
            return OkxLiquidationMessage.model_validate(payload)
        raise UnsupportedPayloadError(f"unsupported OKX public channel: {channel!r}")

    if payload.get("code") == "0" and "data" in payload:
        return OkxInstrumentResponse.model_validate(payload)
    raise UnsupportedPayloadError("unsupported OKX payload")


def contract_specifications_from_response(
    payload: object,
) -> dict[str, ContractSpecification]:
    """Build unit-conversion metadata from the public instruments response."""

    parsed = parse_okx_payload(payload)
    if not isinstance(parsed, OkxInstrumentResponse):
        raise UnsupportedPayloadError("expected an OKX public instruments response")

    specifications: dict[str, ContractSpecification] = {}
    for instrument in parsed.data:
        if instrument.state != "live":
            continue
        canonical_symbol = to_canonical_symbol(Exchange.OKX, instrument.instrument_id)
        specifications[instrument.instrument_id] = ContractSpecification(
            exchange=Exchange.OKX,
            canonical_symbol=canonical_symbol,
            venue_symbol=instrument.instrument_id,
            contract_type=instrument.contract_type,
            contract_value=decimal_from_text(
                instrument.contract_value,
                field_name="contract_value",
            ),
            contract_multiplier=decimal_from_text(
                instrument.contract_multiplier,
                field_name="contract_multiplier",
            ),
            contract_value_currency=instrument.contract_value_currency,
        )
    return specifications


def _metadata(
    timestamp_ms: int | str,
    context: NormalizationContext,
    *,
    venue_symbol: str,
    sequence_id: int | None,
) -> EventMetadata:
    return {
        "exchange": Exchange.OKX,
        "symbol": to_canonical_symbol(Exchange.OKX, venue_symbol),
        "exchange_timestamp": timestamp_from_milliseconds(timestamp_ms),
        "local_receive_timestamp": context.local_receive_timestamp,
        "normalization_timestamp": context.normalization_timestamp,
        "sequence_id": sequence_id,
        "raw_payload_reference": context.raw_payload_reference,
    }


class OkxNormalizer:
    """Normalize supported OKX v5 payloads using public contract metadata."""

    def __init__(self, specifications: Mapping[str, ContractSpecification]) -> None:
        self._specifications = dict(specifications)

    def _specification(self, venue_symbol: str) -> ContractSpecification:
        try:
            return self._specifications[venue_symbol]
        except KeyError as exc:
            raise UnsupportedPayloadError(
                f"missing OKX contract specification for {venue_symbol}"
            ) from exc

    def _levels(
        self,
        levels: list[OkxBookLevelText],
        *,
        venue_symbol: str,
    ) -> list[OrderBookLevel]:
        specification = self._specification(venue_symbol)
        return [
            OrderBookLevel(
                price=decimal_from_text(price, field_name="price"),
                quantity=specification.contracts_to_base(
                    decimal_from_text(quantity, field_name="quantity_contracts")
                ),
            )
            for price, quantity, _deprecated, _order_count in levels
        ]

    def normalize(
        self,
        payload: object,
        *,
        context: NormalizationContext,
    ) -> list[NormalizedMarketEvent]:
        parsed = parse_okx_payload(payload)

        if isinstance(parsed, OkxInstrumentResponse):
            return []

        if isinstance(parsed, OkxTradeMessage):
            events: list[NormalizedMarketEvent] = []
            for trade in parsed.data:
                specification = self._specification(trade.instrument_id)
                contracts = decimal_from_text(
                    trade.size_contracts,
                    field_name="size_contracts",
                )
                events.append(
                    Trade(
                        **_metadata(
                            trade.timestamp_ms,
                            context,
                            venue_symbol=trade.instrument_id,
                            sequence_id=trade.sequence_id,
                        ),
                        trade_id=trade.trade_id,
                        price=decimal_from_text(trade.price, field_name="price"),
                        quantity=specification.contracts_to_base(contracts),
                        contract_quantity=contracts,
                        aggressor_side=(
                            AggressorSide.BUY if trade.side == "buy" else AggressorSide.SELL
                        ),
                    )
                )
            return events

        if isinstance(parsed, OkxBooksMessage):
            events = []
            venue_symbol = parsed.argument.instrument_id
            if venue_symbol is None:
                raise UnsupportedPayloadError("OKX books message is missing instId")
            for book in parsed.data:
                metadata = _metadata(
                    book.timestamp_ms,
                    context,
                    venue_symbol=venue_symbol,
                    sequence_id=book.sequence_id,
                )
                if parsed.action == "snapshot":
                    events.append(
                        OrderBookSnapshot(
                            **metadata,
                            bids=self._levels(book.bids, venue_symbol=venue_symbol),
                            asks=self._levels(book.asks, venue_symbol=venue_symbol),
                            depth=max(len(book.bids), len(book.asks)),
                            checksum=str(book.checksum),
                        )
                    )
                elif book.bids or book.asks:
                    events.append(
                        OrderBookUpdate(
                            **metadata,
                            bids=self._levels(book.bids, venue_symbol=venue_symbol),
                            asks=self._levels(book.asks, venue_symbol=venue_symbol),
                            previous_sequence_id=book.previous_sequence_id,
                            checksum=str(book.checksum),
                        )
                    )
            return events

        if isinstance(parsed, OkxBooks5Message):
            book5_events: list[NormalizedMarketEvent] = []
            for book5 in parsed.data:
                book5_events.append(
                    OrderBookSnapshot(
                        **_metadata(
                            book5.timestamp_ms,
                            context,
                            venue_symbol=book5.instrument_id,
                            sequence_id=book5.sequence_id,
                        ),
                        bids=self._levels(
                            book5.bids,
                            venue_symbol=book5.instrument_id,
                        ),
                        asks=self._levels(
                            book5.asks,
                            venue_symbol=book5.instrument_id,
                        ),
                        depth=5,
                    )
                )
            return book5_events

        if isinstance(parsed, OkxOpenInterestMessage):
            return [
                OpenInterest(
                    **_metadata(
                        entry.timestamp_ms,
                        context,
                        venue_symbol=entry.instrument_id,
                        sequence_id=None,
                    ),
                    open_interest_contracts=decimal_from_text(
                        entry.contracts,
                        field_name="open_interest_contracts",
                    ),
                    open_interest_base=decimal_from_text(
                        entry.base_quantity,
                        field_name="open_interest_base",
                    ),
                    open_interest_quote=decimal_from_text(
                        entry.quote_notional,
                        field_name="open_interest_quote",
                    ),
                )
                for entry in parsed.data
            ]

        if isinstance(parsed, OkxFundingRateMessage):
            return [
                FundingRate(
                    **_metadata(
                        entry.timestamp_ms,
                        context,
                        venue_symbol=entry.instrument_id,
                        sequence_id=None,
                    ),
                    funding_rate=decimal_from_text(
                        entry.funding_rate,
                        field_name="funding_rate",
                    ),
                    next_funding_timestamp=timestamp_from_milliseconds(entry.next_funding_time_ms),
                )
                for entry in parsed.data
            ]

        if isinstance(parsed, OkxMarkPriceMessage):
            return [
                MarkPrice(
                    **_metadata(
                        entry.timestamp_ms,
                        context,
                        venue_symbol=entry.instrument_id,
                        sequence_id=None,
                    ),
                    mark_price=decimal_from_text(
                        entry.mark_price,
                        field_name="mark_price",
                    ),
                )
                for entry in parsed.data
            ]

        if isinstance(parsed, OkxIndexTickerMessage):
            events = []
            for entry in parsed.data:
                swap_symbol = f"{entry.instrument_id}-SWAP"
                events.append(
                    IndexPrice(
                        **_metadata(
                            entry.timestamp_ms,
                            context,
                            venue_symbol=swap_symbol,
                            sequence_id=None,
                        ),
                        index_price=decimal_from_text(
                            entry.index_price,
                            field_name="index_price",
                        ),
                    )
                )
            return events

        if isinstance(parsed, OkxLiquidationMessage):
            events = []
            for liquidation in parsed.data:
                specification = self._specification(liquidation.instrument_id)
                for index, detail in enumerate(liquidation.details):
                    contracts = decimal_from_text(
                        detail.size_contracts,
                        field_name="size_contracts",
                    )
                    events.append(
                        LiquidationEvent(
                            **_metadata(
                                detail.timestamp_ms,
                                context,
                                venue_symbol=liquidation.instrument_id,
                                sequence_id=None,
                            ),
                            liquidation_id=(
                                f"{liquidation.instrument_id}:{detail.timestamp_ms}:{index}"
                            ),
                            position_side=(
                                LiquidatedPositionSide.LONG
                                if detail.position_side == "long"
                                else LiquidatedPositionSide.SHORT
                            ),
                            price=decimal_from_text(
                                detail.bankruptcy_price,
                                field_name="bankruptcy_price",
                            ),
                            quantity=specification.contracts_to_base(contracts),
                            contract_quantity=contracts,
                        )
                    )
            return events

        raise AssertionError(f"unhandled parsed OKX payload: {type(parsed).__name__}")


def contracts_and_notional(
    *,
    price: str,
    contracts: str,
    specification: ContractSpecification,
) -> tuple[Decimal, Decimal]:
    """Convert OKX contracts to base quantity and quote notional exactly."""

    base_quantity = specification.contracts_to_base(
        decimal_from_text(contracts, field_name="contracts")
    )
    quote_notional = decimal_from_text(price, field_name="price") * base_quantity
    return base_quantity, quote_notional

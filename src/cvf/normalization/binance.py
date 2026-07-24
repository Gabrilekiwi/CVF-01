"""Strongly typed Binance USDⓈ-M public payload parsing and normalization."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cvf.exchanges.symbols import to_canonical_symbol
from cvf.models.enums import AggressorSide, Exchange, LiquidatedPositionSide
from cvf.models.market import (
    BestBidAsk,
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

type BookLevelText = tuple[str, str]


class BinancePayloadModel(BaseModel):
    """Base model that tolerates additive exchange fields while typing known fields."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class BinanceAggTradePayload(BinancePayloadModel):
    event_type: Literal["aggTrade"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    aggregate_trade_id: int = Field(alias="a")
    venue_symbol: str = Field(alias="s")
    price: str = Field(alias="p")
    quantity: str = Field(alias="q")
    first_trade_id: int = Field(alias="f")
    last_trade_id: int = Field(alias="l")
    trade_time_ms: int = Field(alias="T")
    buyer_is_maker: bool = Field(alias="m")


class BinanceDepthUpdatePayload(BinancePayloadModel):
    event_type: Literal["depthUpdate"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    transaction_time_ms: int | None = Field(default=None, alias="T")
    venue_symbol: str = Field(alias="s")
    first_update_id: int = Field(alias="U")
    final_update_id: int = Field(alias="u")
    previous_final_update_id: int = Field(alias="pu")
    bids: list[BookLevelText] = Field(alias="b")
    asks: list[BookLevelText] = Field(alias="a")


class BinanceBookTickerPayload(BinancePayloadModel):
    event_type: Literal["bookTicker"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    transaction_time_ms: int = Field(alias="T")
    venue_symbol: str = Field(alias="s")
    update_id: int = Field(alias="u")
    bid_price: str = Field(alias="b")
    bid_quantity: str = Field(alias="B")
    ask_price: str = Field(alias="a")
    ask_quantity: str = Field(alias="A")


class BinanceDepthSnapshotPayload(BinancePayloadModel):
    last_update_id: int = Field(alias="lastUpdateId")
    event_time_ms: int | None = Field(default=None, alias="E")
    transaction_time_ms: int | None = Field(default=None, alias="T")
    bids: list[BookLevelText]
    asks: list[BookLevelText]


class BinanceMarkPricePayload(BinancePayloadModel):
    event_type: Literal["markPriceUpdate"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    venue_symbol: str = Field(alias="s")
    mark_price: str = Field(alias="p")
    index_price: str = Field(alias="i")
    funding_rate: str = Field(alias="r")
    next_funding_time_ms: int = Field(alias="T")


class BinanceForceOrderDetail(BinancePayloadModel):
    venue_symbol: str = Field(alias="s")
    order_side: Literal["BUY", "SELL"] = Field(alias="S")
    original_quantity: str = Field(alias="q")
    order_price: str = Field(alias="p")
    average_price: str = Field(alias="ap")
    accumulated_filled_quantity: str = Field(alias="z")
    trade_time_ms: int = Field(alias="T")


class BinanceForceOrderPayload(BinancePayloadModel):
    event_type: Literal["forceOrder"] = Field(alias="e")
    event_time_ms: int = Field(alias="E")
    order: BinanceForceOrderDetail = Field(alias="o")


class BinanceOpenInterestPayload(BinancePayloadModel):
    open_interest: str = Field(alias="openInterest")
    venue_symbol: str = Field(alias="symbol")
    event_time_ms: int = Field(alias="time")


type BinanceParsedPayload = (
    BinanceAggTradePayload
    | BinanceDepthUpdatePayload
    | BinanceBookTickerPayload
    | BinanceDepthSnapshotPayload
    | BinanceMarkPricePayload
    | BinanceForceOrderPayload
    | BinanceOpenInterestPayload
)


def unwrap_binance_payload(payload: object) -> object:
    """Return raw event data from a combined-stream wrapper when present."""

    if isinstance(payload, Mapping):
        data = payload.get("data")
        if isinstance(data, Mapping):
            return data
    return payload


def parse_binance_payload(payload: object) -> BinanceParsedPayload:
    """Validate one supported Binance public WebSocket or REST payload."""

    raw = unwrap_binance_payload(payload)
    if not isinstance(raw, Mapping):
        raise UnsupportedPayloadError("Binance payload must be a JSON object")

    event_type = raw.get("e")
    if event_type == "aggTrade":
        return BinanceAggTradePayload.model_validate(raw)
    if event_type == "depthUpdate":
        return BinanceDepthUpdatePayload.model_validate(raw)
    if event_type == "bookTicker":
        return BinanceBookTickerPayload.model_validate(raw)
    if event_type == "markPriceUpdate":
        return BinanceMarkPricePayload.model_validate(raw)
    if event_type == "forceOrder":
        return BinanceForceOrderPayload.model_validate(raw)
    if "lastUpdateId" in raw and "bids" in raw and "asks" in raw:
        return BinanceDepthSnapshotPayload.model_validate(raw)
    if "openInterest" in raw and "symbol" in raw:
        return BinanceOpenInterestPayload.model_validate(raw)
    raise UnsupportedPayloadError(f"unsupported Binance payload event type: {event_type!r}")


def _levels(levels: list[BookLevelText]) -> list[OrderBookLevel]:
    return [
        OrderBookLevel(
            price=decimal_from_text(price, field_name="price"),
            quantity=decimal_from_text(quantity, field_name="quantity"),
        )
        for price, quantity in levels
    ]


def _metadata(
    parsed_timestamp_ms: int,
    context: NormalizationContext,
    *,
    venue_symbol: str,
    sequence_id: int | None,
) -> EventMetadata:
    return {
        "exchange": Exchange.BINANCE,
        "symbol": to_canonical_symbol(Exchange.BINANCE, venue_symbol),
        "exchange_timestamp": timestamp_from_milliseconds(parsed_timestamp_ms),
        "local_receive_timestamp": context.local_receive_timestamp,
        "normalization_timestamp": context.normalization_timestamp,
        "sequence_id": sequence_id,
        "raw_payload_reference": context.raw_payload_reference,
    }


class BinanceNormalizer:
    """Normalize supported USDⓈ-M public payloads without network access."""

    def normalize(
        self,
        payload: object,
        *,
        context: NormalizationContext,
    ) -> list[NormalizedMarketEvent]:
        parsed = parse_binance_payload(payload)

        if isinstance(parsed, BinanceAggTradePayload):
            metadata = _metadata(
                parsed.trade_time_ms,
                context,
                venue_symbol=parsed.venue_symbol,
                sequence_id=parsed.aggregate_trade_id,
            )
            return [
                Trade(
                    **metadata,
                    trade_id=str(parsed.aggregate_trade_id),
                    price=decimal_from_text(parsed.price, field_name="price"),
                    quantity=decimal_from_text(parsed.quantity, field_name="quantity"),
                    aggressor_side=(
                        AggressorSide.SELL if parsed.buyer_is_maker else AggressorSide.BUY
                    ),
                )
            ]

        if isinstance(parsed, BinanceDepthUpdatePayload):
            timestamp_ms = parsed.transaction_time_ms or parsed.event_time_ms
            metadata = _metadata(
                timestamp_ms,
                context,
                venue_symbol=parsed.venue_symbol,
                sequence_id=parsed.final_update_id,
            )
            return [
                OrderBookUpdate(
                    **metadata,
                    bids=_levels(parsed.bids),
                    asks=_levels(parsed.asks),
                    previous_sequence_id=parsed.previous_final_update_id,
                )
            ]

        if isinstance(parsed, BinanceBookTickerPayload):
            metadata = _metadata(
                parsed.transaction_time_ms,
                context,
                venue_symbol=parsed.venue_symbol,
                sequence_id=parsed.update_id,
            )
            return [
                BestBidAsk(
                    **metadata,
                    bid_price=decimal_from_text(parsed.bid_price, field_name="bid_price"),
                    bid_quantity=decimal_from_text(
                        parsed.bid_quantity,
                        field_name="bid_quantity",
                    ),
                    ask_price=decimal_from_text(parsed.ask_price, field_name="ask_price"),
                    ask_quantity=decimal_from_text(
                        parsed.ask_quantity,
                        field_name="ask_quantity",
                    ),
                )
            ]

        if isinstance(parsed, BinanceDepthSnapshotPayload):
            if context.canonical_symbol is None:
                raise UnsupportedPayloadError(
                    "Binance REST depth snapshot requires context.canonical_symbol"
                )
            timestamp_ms = (
                parsed.transaction_time_ms
                or parsed.event_time_ms
                or int(context.local_receive_timestamp.timestamp() * 1000)
            )
            return [
                OrderBookSnapshot(
                    exchange=Exchange.BINANCE,
                    symbol=context.canonical_symbol,
                    exchange_timestamp=timestamp_from_milliseconds(timestamp_ms),
                    local_receive_timestamp=context.local_receive_timestamp,
                    normalization_timestamp=context.normalization_timestamp,
                    sequence_id=parsed.last_update_id,
                    raw_payload_reference=context.raw_payload_reference,
                    bids=_levels(parsed.bids),
                    asks=_levels(parsed.asks),
                    depth=max(len(parsed.bids), len(parsed.asks)),
                )
            ]

        if isinstance(parsed, BinanceMarkPricePayload):
            metadata = _metadata(
                parsed.event_time_ms,
                context,
                venue_symbol=parsed.venue_symbol,
                sequence_id=None,
            )
            return [
                MarkPrice(
                    **metadata,
                    mark_price=decimal_from_text(
                        parsed.mark_price,
                        field_name="mark_price",
                    ),
                ),
                IndexPrice(
                    **metadata,
                    index_price=decimal_from_text(
                        parsed.index_price,
                        field_name="index_price",
                    ),
                ),
                FundingRate(
                    **metadata,
                    funding_rate=decimal_from_text(
                        parsed.funding_rate,
                        field_name="funding_rate",
                    ),
                    next_funding_timestamp=timestamp_from_milliseconds(parsed.next_funding_time_ms),
                ),
            ]

        if isinstance(parsed, BinanceForceOrderPayload):
            order = parsed.order
            metadata = _metadata(
                order.trade_time_ms,
                context,
                venue_symbol=order.venue_symbol,
                sequence_id=None,
            )
            filled_quantity = decimal_from_text(
                order.accumulated_filled_quantity,
                field_name="accumulated_filled_quantity",
            )
            quantity = (
                filled_quantity
                if filled_quantity > 0
                else decimal_from_text(order.original_quantity, field_name="original_quantity")
            )
            average_price = decimal_from_text(
                order.average_price,
                field_name="average_price",
            )
            price = (
                average_price
                if average_price > 0
                else decimal_from_text(order.order_price, field_name="order_price")
            )
            return [
                LiquidationEvent(
                    **metadata,
                    position_side=(
                        LiquidatedPositionSide.LONG
                        if order.order_side == "SELL"
                        else LiquidatedPositionSide.SHORT
                    ),
                    price=price,
                    quantity=quantity,
                )
            ]

        if isinstance(parsed, BinanceOpenInterestPayload):
            metadata = _metadata(
                parsed.event_time_ms,
                context,
                venue_symbol=parsed.venue_symbol,
                sequence_id=None,
            )
            return [
                OpenInterest(
                    **metadata,
                    open_interest_contracts=decimal_from_text(
                        parsed.open_interest,
                        field_name="open_interest",
                    ),
                )
            ]

        raise AssertionError(f"unhandled parsed Binance payload: {type(parsed).__name__}")


def quantity_and_notional(price: str, quantity: str) -> tuple[Decimal, Decimal]:
    """Parse a Binance base quantity and compute quote notional exactly."""

    parsed_price = decimal_from_text(price, field_name="price")
    parsed_quantity = decimal_from_text(quantity, field_name="quantity")
    return parsed_quantity, parsed_price * parsed_quantity

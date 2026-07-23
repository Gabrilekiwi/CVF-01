"""Canonical-to-venue symbol mapping."""

from __future__ import annotations

from collections.abc import Mapping

from cvf.models.enums import Exchange
from cvf.utils.validation import validate_canonical_symbol

DEFAULT_SYMBOL_MAP: dict[Exchange, dict[str, str]] = {
    Exchange.BINANCE: {
        "BTC-USDT-PERP": "BTCUSDT",
        "ETH-USDT-PERP": "ETHUSDT",
    },
    Exchange.OKX: {
        "BTC-USDT-PERP": "BTC-USDT-SWAP",
        "ETH-USDT-PERP": "ETH-USDT-SWAP",
    },
}


class UnknownSymbolError(ValueError):
    """Raised when no exact canonical/venue mapping exists."""


def _supported_exchange(exchange: Exchange) -> Exchange:
    if exchange not in {Exchange.BINANCE, Exchange.OKX}:
        raise UnknownSymbolError(f"no market-data symbol map for exchange {exchange}")
    return exchange


def to_exchange_symbol(
    exchange: Exchange,
    canonical_symbol: str,
    *,
    symbol_map: Mapping[Exchange, Mapping[str, str]] = DEFAULT_SYMBOL_MAP,
) -> str:
    """Return a venue symbol for an exact canonical perpetual symbol."""

    venue = _supported_exchange(exchange)
    canonical = validate_canonical_symbol(canonical_symbol)
    try:
        return symbol_map[venue][canonical]
    except KeyError as exc:
        raise UnknownSymbolError(f"{canonical} is not configured for {venue}") from exc


def to_canonical_symbol(
    exchange: Exchange,
    venue_symbol: str,
    *,
    symbol_map: Mapping[Exchange, Mapping[str, str]] = DEFAULT_SYMBOL_MAP,
) -> str:
    """Return the canonical symbol for an exact venue symbol."""

    venue = _supported_exchange(exchange)
    normalized = venue_symbol.strip().upper()
    reverse = {raw.upper(): canonical for canonical, raw in symbol_map[venue].items()}
    try:
        return reverse[normalized]
    except KeyError as exc:
        raise UnknownSymbolError(f"{venue_symbol!r} is not configured for {venue}") from exc


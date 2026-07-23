"""Exact canonical symbol mapping tests."""

from __future__ import annotations

import pytest

from cvf.exchanges.symbols import (
    UnknownSymbolError,
    to_canonical_symbol,
    to_exchange_symbol,
)
from cvf.models import Exchange


@pytest.mark.parametrize(
    ("exchange", "canonical", "venue_symbol"),
    [
        (Exchange.BINANCE, "BTC-USDT-PERP", "BTCUSDT"),
        (Exchange.BINANCE, "ETH-USDT-PERP", "ETHUSDT"),
        (Exchange.OKX, "BTC-USDT-PERP", "BTC-USDT-SWAP"),
        (Exchange.OKX, "ETH-USDT-PERP", "ETH-USDT-SWAP"),
    ],
)
def test_symbol_mapping_round_trip(
    exchange: Exchange,
    canonical: str,
    venue_symbol: str,
) -> None:
    assert to_exchange_symbol(exchange, canonical.lower()) == venue_symbol
    assert to_canonical_symbol(exchange, venue_symbol.lower()) == canonical


def test_unknown_symbols_fail_closed() -> None:
    with pytest.raises(UnknownSymbolError, match="not configured"):
        to_exchange_symbol(Exchange.BINANCE, "SOL-USDT-PERP")

    with pytest.raises(UnknownSymbolError, match="not configured"):
        to_canonical_symbol(Exchange.OKX, "SOL-USDT-SWAP")


def test_cross_venue_is_not_a_market_data_venue() -> None:
    with pytest.raises(UnknownSymbolError, match="no market-data symbol map"):
        to_exchange_symbol(Exchange.CROSS_VENUE, "BTC-USDT-PERP")


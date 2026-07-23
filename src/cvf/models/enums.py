"""Shared enumerations for normalized events and paper-trading records."""

from __future__ import annotations

from enum import StrEnum


class Exchange(StrEnum):
    BINANCE = "BINANCE"
    OKX = "OKX"
    CROSS_VENUE = "CROSS_VENUE"
    SIMULATED = "SIMULATED"


class EventType(StrEnum):
    TRADE = "TRADE"
    ORDER_BOOK_SNAPSHOT = "ORDER_BOOK_SNAPSHOT"
    ORDER_BOOK_UPDATE = "ORDER_BOOK_UPDATE"
    BEST_BID_ASK = "BEST_BID_ASK"
    OPEN_INTEREST = "OPEN_INTEREST"
    FUNDING_RATE = "FUNDING_RATE"
    MARK_PRICE = "MARK_PRICE"
    INDEX_PRICE = "INDEX_PRICE"
    LIQUIDATION = "LIQUIDATION"
    EXCHANGE_HEALTH = "EXCHANGE_HEALTH"
    MARKET_FEATURE = "MARKET_FEATURE"
    TRADING_SIGNAL = "TRADING_SIGNAL"
    SIMULATED_ORDER = "SIMULATED_ORDER"
    SIMULATED_POSITION = "SIMULATED_POSITION"
    SIMULATED_TRADE = "SIMULATED_TRADE"


class AggressorSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class LiquidatedPositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class HealthStatus(StrEnum):
    CONNECTED = "CONNECTED"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    RESYNCING = "RESYNCING"
    DISCONNECTED = "DISCONNECTED"


class SignalType(StrEnum):
    LONG_ENTRY = "LONG_ENTRY"
    SHORT_ENTRY = "SHORT_ENTRY"
    LONG_EXIT = "LONG_EXIT"
    SHORT_EXIT = "SHORT_EXIT"
    HOLD = "HOLD"
    NO_TRADE = "NO_TRADE"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"


class OrderSide(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"


class OrderStatus(StrEnum):
    CREATED = "CREATED"
    FILLED = "FILLED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"


class PositionSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class PositionStatus(StrEnum):
    OPEN = "OPEN"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    CLOSED = "CLOSED"


class TradePurpose(StrEnum):
    ENTRY = "ENTRY"
    TAKE_PROFIT_1 = "TAKE_PROFIT_1"
    TAKE_PROFIT_2 = "TAKE_PROFIT_2"
    STOP_LOSS = "STOP_LOSS"
    TIME_STOP = "TIME_STOP"
    SIGNAL_EXIT = "SIGNAL_EXIT"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"
    MAX_HOLDING_EXIT = "MAX_HOLDING_EXIT"


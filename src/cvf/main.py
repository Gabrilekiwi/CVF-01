"""Offline inspection and explicit phase-2 collection entry points."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from types import FrameType
from typing import Any, NoReturn

from pydantic import ValidationError

from cvf.config import ConfigError, Settings, load_settings
from cvf.exchanges.base import ExchangeConnector
from cvf.exchanges.binance import BinanceMarketDataConnector
from cvf.exchanges.okx import OKXMarketDataConnector
from cvf.logging_config import configure_logging


def build_connectors(settings: Settings) -> list[ExchangeConnector]:
    """Construct enabled connector skeletons from validated settings."""

    connectors: list[ExchangeConnector] = []
    if settings.exchanges.binance.enabled:
        connectors.append(
            BinanceMarketDataConnector(
                settings.exchanges.binance,
                stale_after_ms=settings.health.stale_after_ms,
            )
        )
    if settings.exchanges.okx.enabled:
        connectors.append(
            OKXMarketDataConnector(
                settings.exchanges.okx,
                stale_after_ms=settings.health.stale_after_ms,
            )
        )
    return connectors


def _install_shutdown_handlers(
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> Callable[[], None]:
    """Install cross-platform SIGINT/SIGTERM handlers and return a restore callback."""

    loop = asyncio.get_running_loop()
    signal_handler = Callable[[int, FrameType | None], Any] | int | None
    previous: dict[signal.Signals, signal_handler] = {}
    registered_with_loop: list[signal.Signals] = []

    def request_shutdown(received: signal.Signals) -> None:
        logger.info(
            "shutdown signal received",
            extra={"event": "shutdown_requested", "signal": received.name},
        )
        loop.call_soon_threadsafe(stop_event.set)

    def fallback_handler(received: signal.Signals) -> Callable[[int, FrameType | None], None]:
        def handler(_number: int, _frame: FrameType | None) -> None:
            request_shutdown(received)

        return handler

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, request_shutdown, signum)
            registered_with_loop.append(signum)
        except (NotImplementedError, RuntimeError):
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, fallback_handler(signum))

    def restore() -> None:
        for signum in registered_with_loop:
            loop.remove_signal_handler(signum)
        for signum, handler in previous.items():
            signal.signal(signum, handler)

    return restore


async def run(settings: Settings, *, once: bool = False) -> int:
    """Run the network-free configuration and connector-plan process."""

    logger = logging.getLogger("cvf")
    connectors = build_connectors(settings)
    logger.info(
        "application initialized",
        extra={
            "event": "application_initialized",
            "strategy_version": settings.app.strategy_version,
            "paper_trading_only": settings.app.paper_trading_only,
            "connector_count": len(connectors),
        },
    )

    for connector in connectors:
        plans = [plan.model_dump(mode="json") for plan in connector.planned_subscriptions()]
        logger.info(
            "connector subscription plan",
            extra={
                "event": "connector_subscription_plan",
                "exchange": connector.exchange.value,
                "websocket_url": connector.config.public_websocket_url,
                "subscriptions": plans,
            },
        )
        health = await connector.health_check()
        logger.info(
            "connector health",
            extra={
                "event": "connector_health",
                "exchange": connector.exchange.value,
                "health": health.model_dump(mode="json"),
            },
        )

    if once:
        await asyncio.gather(*(connector.disconnect() for connector in connectors))
        logger.info("one-shot check complete", extra={"event": "one_shot_complete"})
        return 0

    stop_event = asyncio.Event()
    restore_handlers = _install_shutdown_handlers(stop_event, logger)
    logger.info(
        "waiting for shutdown",
        extra={
            "event": "waiting_for_shutdown",
            "network_collection_started": False,
        },
    )
    try:
        await stop_event.wait()
    finally:
        restore_handlers()
        await asyncio.gather(*(connector.disconnect() for connector in connectors))
        logger.info("application stopped", extra={"event": "application_stopped"})
    return 0


async def run_collection(
    settings: Settings,
    *,
    duration_seconds: float | None,
    output_path: Path | None,
) -> int:
    """Run explicit public collection until duration or shutdown signal."""

    from cvf.collector import MarketDataCollector

    logger = logging.getLogger("cvf")
    stop_event = asyncio.Event()
    restore_handlers = _install_shutdown_handlers(stop_event, logger)
    collector = MarketDataCollector(settings, output_path=output_path)
    try:
        summary = await collector.run(
            stop_event=stop_event,
            duration_seconds=duration_seconds,
        )
    finally:
        restore_handlers()
    logger.info(
        "market-data collection complete",
        extra={
            "event": "collection_complete",
            "duration_seconds": summary.duration_seconds,
            "output_path": summary.output_path,
            "normalized_event_counts": summary.normalized_event_counts,
            "health_status_counts": summary.health_status_counts,
            "parquet": summary.parquet,
        },
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cvf",
        description="CVF-01 offline inspection and public market-data collection",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional YAML overlay merged on top of config/default.yaml",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Print plans and disconnected health once without network access",
    )
    subparsers = parser.add_subparsers(dest="command")
    collect = subparsers.add_parser(
        "collect",
        help="Explicitly collect Binance and OKX public market data",
    )
    collect.add_argument(
        "--config",
        type=Path,
        help="Optional YAML overlay merged on top of config/default.yaml",
    )
    collect.add_argument(
        "--duration",
        type=float,
        help="Stop cleanly after this many seconds; omit to run until a signal",
    )
    collect.add_argument(
        "--output",
        type=Path,
        help="Override storage.raw_data_path for this collection",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI implementation separated from the console-script exit wrapper."""

    args = _parser().parse_args(argv)
    try:
        settings = load_settings(args.config)
    except (ConfigError, ValidationError) as exc:
        print(f"CVF-01 configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.logging.level, json_output=settings.logging.json_output)
    try:
        if args.command == "collect":
            return asyncio.run(
                run_collection(
                    settings,
                    duration_seconds=args.duration,
                    output_path=args.output,
                )
            )
        return asyncio.run(run(settings, once=args.once))
    except (OSError, RuntimeError, ValueError) as exc:
        logging.getLogger("cvf").exception(
            "CVF-01 command failed",
            extra={"event": "command_failed"},
        )
        print(f"CVF-01 error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def cli() -> NoReturn:
    """Installed console-script wrapper."""

    raise SystemExit(main())

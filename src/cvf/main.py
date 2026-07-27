"""Offline inspection and explicit phase-2 collection entry points."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import FrameType
from typing import Any, NoReturn
from uuid import UUID

from pydantic import ValidationError

from cvf.config import ConfigError, Settings, load_settings
from cvf.exchanges.base import ExchangeConnector
from cvf.exchanges.binance import BinanceMarketDataConnector
from cvf.exchanges.okx import OKXMarketDataConnector
from cvf.features.models import FeatureUnavailableCode
from cvf.logging_config import configure_logging
from cvf.models.enums import Exchange
from cvf.replay import RawParquetReader, RawScanFilter, ReplayOrder, ReplayRunner
from cvf.storage.compact import compact_raw_tree
from cvf.storage.features import (
    FeatureScanFilter,
    audit_feature_tree,
    compare_feature_trees,
)


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
            "pipeline": summary.pipeline,
            "feature_state": summary.feature_state,
        },
    )
    return 0


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a UTC offset")
    return parsed


async def run_replay(
    settings: Settings,
    *,
    input_path: Path,
    start: datetime | None,
    end: datetime | None,
    exchanges: list[str] | None,
    symbols: list[str] | None,
    channels: list[str] | None,
    order: ReplayOrder,
    speed: float | None,
) -> int:
    """Replay retained raw records without exchange connectivity."""

    from cvf.features import FeatureStatePipeline, MarketStateStore
    from cvf.pipeline import NormalizedEventBus

    bus = NormalizedEventBus(
        default_queue_capacity=settings.pipeline.consumer_queue_capacity
    )
    feature_state = FeatureStatePipeline(MarketStateStore(settings.features))
    bus.register(
        "feature-state",
        feature_state.consume,
        queue_capacity=settings.pipeline.consumer_queue_capacity,
    )
    reader = RawParquetReader(input_path)
    filters = RawScanFilter(
        start=start,
        end=end,
        exchanges=(
            None if not exchanges else frozenset(Exchange(value) for value in exchanges)
        ),
        symbols=None if not symbols else frozenset(symbols),
        channels=None if not channels else frozenset(channels),
    )
    runner = ReplayRunner(
        event_bus=bus,
        order=order,
        speed=settings.replay.default_speed if speed is None else speed,
    )
    summary = await runner.run(reader.iter_records(filters=filters, order=order))
    logging.getLogger("cvf").info(
        "raw replay complete",
        extra={
            "event": "replay_complete",
            "input_path": str(input_path.resolve()),
            **asdict(summary),
            "feature_state": asdict(feature_state.stats),
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
    replay = subparsers.add_parser(
        "replay",
        help="Offline deterministic replay of retained raw Parquet",
    )
    replay.add_argument("--config", type=Path)
    replay.add_argument("--input", type=Path, required=True)
    replay.add_argument("--start", type=_parse_timestamp)
    replay.add_argument("--end", type=_parse_timestamp)
    replay.add_argument(
        "--exchange",
        action="append",
        choices=[Exchange.BINANCE.value, Exchange.OKX.value],
    )
    replay.add_argument("--symbol", action="append")
    replay.add_argument("--channel", action="append")
    replay.add_argument(
        "--order",
        type=ReplayOrder,
        choices=list(ReplayOrder),
        default=ReplayOrder.EVENT_TIME,
    )
    replay.add_argument(
        "--speed",
        type=float,
        help="Replay multiplier; 0 is fastest and performs no wall-clock sleeps",
    )
    compact = subparsers.add_parser(
        "compact-raw",
        help="Compact raw Parquet into a separately audited output tree",
    )
    compact.add_argument("--config", type=Path)
    compact.add_argument("--input", type=Path, required=True)
    compact.add_argument("--output", type=Path, required=True)
    compact.add_argument("--target-rows", type=int, default=100_000)
    audit_features = subparsers.add_parser(
        "audit-features",
        help="Audit versioned feature Parquet schema, lineage, and partitions",
    )
    audit_features.add_argument("--config", type=Path)
    audit_features.add_argument("--input", type=Path, required=True)
    audit_features.add_argument("--start", type=_parse_timestamp)
    audit_features.add_argument("--end", type=_parse_timestamp)
    audit_features.add_argument(
        "--scope",
        action="append",
        choices=[
            Exchange.BINANCE.value,
            Exchange.OKX.value,
            Exchange.CROSS_VENUE.value,
        ],
    )
    audit_features.add_argument("--symbol", action="append")
    audit_features.add_argument("--window", action="append", type=int)
    audit_features.add_argument("--schema-version", action="append", type=int)
    audit_features.add_argument("--snapshot-id", action="append", type=UUID)
    audit_features.add_argument(
        "--unavailable-reason",
        action="append",
        choices=list(FeatureUnavailableCode),
    )
    audit_features.add_argument(
        "--warm",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    audit_features.add_argument(
        "--healthy",
        action=argparse.BooleanOptionalAction,
        default=None,
    )
    compare_features = subparsers.add_parser(
        "compare-features",
        help="Require two feature trees to have identical logical content",
    )
    compare_features.add_argument("--config", type=Path)
    compare_features.add_argument("--left", type=Path, required=True)
    compare_features.add_argument("--right", type=Path, required=True)
    phase3_acceptance = subparsers.add_parser(
        "accept-phase3",
        help="Replay a fixed public dataset twice and write Phase 3 evidence",
    )
    phase3_acceptance.add_argument("--config", type=Path)
    phase3_acceptance.add_argument("--input", type=Path, required=True)
    phase3_acceptance.add_argument("--output", type=Path, required=True)
    phase3_acceptance.add_argument("--first-batch-rows", type=int, default=1_000)
    phase3_acceptance.add_argument("--second-batch-rows", type=int, default=777)
    phase3_acceptance.add_argument(
        "--requested-stability-hours",
        type=float,
        default=6.0,
        help="Acceptance target recorded honestly against actual observed wall time",
    )
    phase3_stability = subparsers.add_parser(
        "stability-phase3",
        help="Repeat fixed-data acceptance toward a six-hour wall-clock target",
    )
    phase3_stability.add_argument("--config", type=Path)
    phase3_stability.add_argument("--input", type=Path, required=True)
    phase3_stability.add_argument("--output", type=Path, required=True)
    phase3_stability.add_argument("--target-hours", type=float, default=6.0)
    phase3_stability.add_argument("--maximum-iterations", type=int)
    phase3_stability.add_argument(
        "--retain-feature-trees",
        action="store_true",
        help="Retain audited per-run Parquet trees instead of summaries only",
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
        if args.command == "replay":
            return asyncio.run(
                run_replay(
                    settings,
                    input_path=args.input,
                    start=args.start,
                    end=args.end,
                    exchanges=args.exchange,
                    symbols=args.symbol,
                    channels=args.channel,
                    order=args.order,
                    speed=args.speed,
                )
            )
        if args.command == "compact-raw":
            compaction_report = compact_raw_tree(
                args.input,
                args.output,
                target_rows=args.target_rows,
            )
            logging.getLogger("cvf").info(
                "raw compaction complete",
                extra={
                    "event": "raw_compaction_complete",
                    "input_path": str(compaction_report.input_path),
                    "output_path": str(compaction_report.output_path),
                    "before": asdict(compaction_report.before),
                    "after": asdict(compaction_report.after),
                },
            )
            return 0
        if args.command == "audit-features":
            feature_filter = FeatureScanFilter(
                start=args.start,
                end=args.end,
                scopes=(
                    None
                    if not args.scope
                    else frozenset(Exchange(value) for value in args.scope)
                ),
                symbols=(
                    None if not args.symbol else frozenset(args.symbol)
                ),
                windows=(
                    None if not args.window else frozenset(args.window)
                ),
                schema_versions=(
                    None
                    if not args.schema_version
                    else frozenset(args.schema_version)
                ),
                snapshot_ids=(
                    None
                    if not args.snapshot_id
                    else frozenset(args.snapshot_id)
                ),
                unavailable_codes=(
                    None
                    if not args.unavailable_reason
                    else frozenset(
                        FeatureUnavailableCode(value)
                        for value in args.unavailable_reason
                    )
                ),
                is_warm=args.warm,
                is_healthy=args.healthy,
            )
            audit = audit_feature_tree(args.input, filters=feature_filter)
            logging.getLogger("cvf").info(
                "feature audit complete",
                extra={
                    "event": "feature_audit_complete",
                    "input_path": str(args.input.resolve()),
                    "audit": asdict(audit),
                },
            )
            return 0
        if args.command == "compare-features":
            feature_report = compare_feature_trees(args.left, args.right)
            logging.getLogger("cvf").info(
                "feature consistency comparison complete",
                extra={
                    "event": "feature_consistency_complete",
                    "left_path": str(feature_report.left_path),
                    "right_path": str(feature_report.right_path),
                    "identical": feature_report.identical,
                    "left": asdict(feature_report.left),
                    "right": asdict(feature_report.right),
                },
            )
            if not feature_report.identical:
                raise RuntimeError("feature tree consistency mismatch")
            return 0
        if args.command == "accept-phase3":
            from cvf.acceptance import run_phase3_acceptance

            acceptance_report = asyncio.run(
                run_phase3_acceptance(
                    settings,
                    input_path=args.input,
                    output_path=args.output,
                    first_batch_rows=args.first_batch_rows,
                    second_batch_rows=args.second_batch_rows,
                    requested_stability_seconds=(
                        args.requested_stability_hours * 60 * 60
                    ),
                )
            )
            logging.getLogger("cvf").info(
                "Phase 3 fixed-dataset acceptance complete",
                extra={
                    "event": "phase3_acceptance_complete",
                    "input_path": str(acceptance_report.input_path),
                    "output_path": str(acceptance_report.output_path),
                    "deterministic_replay": acceptance_report.deterministic_replay,
                    "no_lookahead": acceptance_report.no_lookahead,
                    "throughput_above_realtime": (
                        acceptance_report.throughput_above_realtime
                    ),
                    "full_stability_duration_completed": (
                        acceptance_report.full_stability_duration_completed
                    ),
                },
            )
            return 0
        if args.command == "stability-phase3":
            from cvf.acceptance import run_phase3_stability

            stability_report = asyncio.run(
                run_phase3_stability(
                    settings,
                    input_path=args.input,
                    output_path=args.output,
                    target_seconds=args.target_hours * 60 * 60,
                    maximum_iterations=args.maximum_iterations,
                    retain_feature_trees=args.retain_feature_trees,
                )
            )
            logging.getLogger("cvf").info(
                "Phase 3 stability run complete",
                extra={
                    "event": "phase3_stability_complete",
                    "input_path": str(stability_report.input_path),
                    "output_path": str(stability_report.output_path),
                    "target_seconds": stability_report.target_seconds,
                    "actual_wall_seconds": stability_report.actual_wall_seconds,
                    "target_completed": stability_report.target_completed,
                    "iterations": len(stability_report.iterations),
                },
            )
            return 0
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

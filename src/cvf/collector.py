"""Phase-2 public market-data collection orchestration."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from cvf import __version__
from cvf.config import Settings
from cvf.exchanges.binance import BinanceMarketDataConnector
from cvf.exchanges.okx import OKXMarketDataConnector
from cvf.features import FeatureStatePipeline, FeatureStatePipelineStats, MarketStateStore
from cvf.features.runtime import (
    FeatureRuntime,
    FeatureRuntimeStats,
    ReceiveTimeFeatureDriver,
)
from cvf.monitoring import StreamHealthRegistry, StreamHealthSnapshot, StreamKey
from cvf.monitoring.process import current_rss_bytes
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline import ConsumerStats, NormalizedEventBus
from cvf.storage import (
    AsyncPartitionedParquetWriter,
    CollectionManifest,
    ParquetWriterStats,
    RawMarketRecord,
    audit_normalized_journal,
    audit_raw_tree,
    begin_collection_manifest,
    complete_collection_manifest,
)
from cvf.storage.raw import (
    feature_timeline_end_journal_record,
    normalized_event_journal_record,
)
from cvf.utils.async_lifecycle import await_task_completion
from cvf.utils.fingerprint import settings_fingerprint


class CollectionError(RuntimeError):
    """Raised when a live connector exits before collection is stopped."""


@dataclass(frozen=True, slots=True)
class CollectionResourceMetrics:
    initial_rss_bytes: int
    final_rss_bytes: int
    peak_rss_bytes: int
    rss_growth_bytes: int
    process_cpu_seconds: float
    process_cpu_percent_of_one_core: float


@dataclass(frozen=True, slots=True)
class CollectionSummary:
    started_at: datetime
    finished_at: datetime
    output_path: Path
    normalized_event_counts: dict[str, int]
    health_status_counts: dict[str, int]
    parquet: ParquetWriterStats
    pipeline: dict[str, ConsumerStats]
    feature_state: FeatureStatePipelineStats
    feature_output_path: Path | None
    feature_runtime: FeatureRuntimeStats | None
    resources: CollectionResourceMetrics
    collection_manifest: CollectionManifest

    @property
    def duration_seconds(self) -> float:
        return (self.finished_at - self.started_at).total_seconds()


class MarketDataCollector:
    """Own connectors, shared health, raw writer, and clean shutdown."""

    def __init__(
        self,
        settings: Settings,
        *,
        output_path: Path | None = None,
        feature_output_path: Path | None = None,
        event_bus: NormalizedEventBus | None = None,
    ) -> None:
        self.settings = settings
        self.output_path = (output_path or settings.storage.raw_data_path).resolve()
        self.feature_output_path = (
            None if feature_output_path is None else feature_output_path.resolve()
        )
        if self.feature_output_path is not None and (
            self.output_path == self.feature_output_path
            or self.output_path in self.feature_output_path.parents
            or self.feature_output_path in self.output_path.parents
        ):
            raise ValueError("raw and feature output paths must be disjoint")
        self._logger = logging.getLogger("cvf.collector")
        self._health = StreamHealthRegistry(
            stale_after_ms=settings.health.stale_after_ms,
            maximum_core_latency_ms=settings.health.maximum_core_latency_ms,
            clock_skew_warning_ms=settings.health.clock_skew_warning_ms,
            open_interest_stale_after_ms=settings.health.open_interest_stale_after_ms,
            channel_stale_after_ms=settings.health.channel_stale_after_ms,
        )
        self._event_counts: Counter[str] = Counter()
        self._event_bus = event_bus or NormalizedEventBus(
            default_queue_capacity=settings.pipeline.consumer_queue_capacity
        )
        self._feature_runtime = (
            None
            if self.feature_output_path is None
            else FeatureRuntime(
                settings,
                output_path=self.feature_output_path,
            )
        )
        self._feature_state = (
            FeatureStatePipeline(MarketStateStore(settings.features))
            if self._feature_runtime is None
            else self._feature_runtime.feature_state
        )
        self._event_bus.register(
            (
                "feature-state"
                if self._feature_runtime is None
                else "feature-runtime"
            ),
            (
                self._feature_state.consume
                if self._feature_runtime is None
                else self._feature_runtime.consume_event
            ),
            queue_capacity=settings.pipeline.consumer_queue_capacity,
        )
        self._feature_driver = (
            None
            if self._feature_runtime is None
            else ReceiveTimeFeatureDriver(
                settings,
                event_bus=self._event_bus,
                runtime=self._feature_runtime,
            )
        )
        self._writer = AsyncPartitionedParquetWriter(
            root_path=self.output_path,
            batch_rows=settings.storage.parquet_batch_rows,
            flush_seconds=settings.storage.parquet_flush_seconds,
            queue_capacity=settings.storage.parquet_queue_capacity,
            on_backpressure=self._record_backpressure,
        )
        self._connectors: list[BinanceMarketDataConnector | OKXMarketDataConnector] = []
        if settings.exchanges.binance.enabled:
            self._connectors.append(
                BinanceMarketDataConnector(
                    settings.exchanges.binance,
                    stale_after_ms=settings.health.stale_after_ms,
                    health_registry=self._health,
                    raw_writer=self._writer,
                    event_sink=self._record_event,
                    duplicate_cache_size=settings.health.duplicate_cache_size,
                    duplicate_ttl_seconds=settings.health.duplicate_ttl_seconds,
                )
            )
        if settings.exchanges.okx.enabled:
            self._connectors.append(
                OKXMarketDataConnector(
                    settings.exchanges.okx,
                    stale_after_ms=settings.health.stale_after_ms,
                    health_registry=self._health,
                    raw_writer=self._writer,
                    event_sink=self._record_event,
                    duplicate_cache_size=settings.health.duplicate_cache_size,
                    duplicate_ttl_seconds=settings.health.duplicate_ttl_seconds,
                )
            )

    @property
    def connectors(
        self,
    ) -> tuple[BinanceMarketDataConnector | OKXMarketDataConnector, ...]:
        return tuple(self._connectors)

    @property
    def writer_stats(self) -> ParquetWriterStats:
        return self._writer.stats

    async def _record_event_transaction(
        self,
        event: NormalizedMarketEvent,
    ) -> None:
        await self._writer.write(normalized_event_journal_record(event))
        driver = self._feature_driver
        if driver is None:
            await self._event_bus.publish(event)
        else:
            await driver.publish(event)
        self._event_counts[event.event_type.value] += 1

    async def _record_event(self, event: NormalizedMarketEvent) -> None:
        transaction = asyncio.create_task(
            self._record_event_transaction(event),
            name="journal-and-feature-publish",
        )
        await await_task_completion(transaction)

    async def _finish_feature_ticks(
        self,
        *,
        persist_clean_end: bool,
    ) -> datetime:
        through_timestamp = datetime.now(UTC)
        if self._feature_driver is not None:
            await self._feature_driver.finish(
                through_timestamp=through_timestamp
            )
        if persist_clean_end:
            await self._writer.write(
                feature_timeline_end_journal_record(through_timestamp)
            )
        return through_timestamp

    def _validate_success_invariants(self) -> None:
        raw_writer = self._writer.stats
        if (
            raw_writer.accepted_records != raw_writer.written_records
            or raw_writer.queue_depth != 0
            or raw_writer.last_error is not None
        ):
            raise CollectionError(
                "raw writer did not reconcile accepted and committed records"
            )
        expected_events = sum(self._event_counts.values())
        for name, consumer in self._event_bus.stats.items():
            if (
                consumer.published_events != expected_events
                or consumer.processed_events != expected_events
                or consumer.queue_depth != 0
                or consumer.last_error is not None
            ):
                raise CollectionError(
                    f"event consumer did not reconcile the journal timeline: {name}"
                )
        runtime = self._feature_runtime
        if runtime is None:
            return
        stats = runtime.stats
        symbol_windows = (
            len(self.settings.markets.canonical_symbols)
            * len(self.settings.timing.feature_windows_seconds)
        )
        expected_single = stats.feature_ticks * 2 * symbol_windows
        expected_cross = stats.feature_ticks * symbol_windows
        expected_snapshots = expected_single + expected_cross
        writer = stats.writer
        if (
            stats.normalized_events != expected_events
            or stats.single_venue_snapshots != expected_single
            or stats.cross_venue_snapshots != expected_cross
            or writer.accepted_snapshots != expected_snapshots
            or writer.written_snapshots != expected_snapshots
            or writer.deduplicated_snapshots != 0
            or writer.queue_depth != 0
            or writer.last_error is not None
        ):
            raise CollectionError(
                "feature runtime did not persist complete fixed-shape ticks"
            )

    def _record_backpressure(self, record: RawMarketRecord) -> None:
        key = StreamKey(record.exchange, record.symbol, record.channel)
        self._health.record_drop(key, backpressure=True)

    def health_snapshots(self, *, now: datetime | None = None) -> list[StreamHealthSnapshot]:
        checked_at = now or datetime.now(UTC)
        snapshots: list[StreamHealthSnapshot] = []
        for connector in self._connectors:
            snapshots.extend(connector.health_snapshots(now=checked_at))
        return snapshots

    async def _status_loop(
        self,
        stop_event: asyncio.Event,
        *,
        rss_samples: list[int],
        cpu_started: float,
    ) -> None:
        while not stop_event.is_set():
            checked_at = datetime.now(UTC)
            current_rss = current_rss_bytes()
            rss_samples.append(current_rss)
            snapshots = self.health_snapshots(now=checked_at)
            for snapshot in snapshots:
                health_event = self._health.exchange_health(
                    snapshot.key,
                    now=checked_at,
                )
                await self._record_event(health_event)
            self._logger.info(
                "market-data collection status",
                extra={
                    "event": "collection_status",
                    "normalized_event_counts": dict(self._event_counts),
                    "parquet": asdict(self._writer.stats),
                    "feature_runtime": (
                        None
                        if self._feature_runtime is None
                        else asdict(self._feature_runtime.stats)
                    ),
                    "resource_metrics": {
                        "rss_bytes": current_rss,
                        "process_cpu_seconds": (
                            time.process_time() - cpu_started
                        ),
                    },
                    "streams": [asdict(snapshot) for snapshot in snapshots],
                },
            )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.settings.app.status_interval_seconds,
                )
            except TimeoutError:
                continue

    async def _shutdown_collection(
        self,
        *,
        primary_error: BaseException | None,
        monitor_tasks: list[asyncio.Task[None]],
        status_task: asyncio.Task[None] | None,
        feature_clock_task: asyncio.Task[None] | None,
        timer_task: asyncio.Task[None] | None,
        stop_task: asyncio.Task[bool] | None,
    ) -> datetime | None:
        """Finish the whole shutdown lifecycle inside one shielded task."""

        cleanup_errors: list[BaseException] = []

        def add_cleanup_error(error: BaseException) -> None:
            if error is primary_error or any(
                existing is error for existing in cleanup_errors
            ):
                return
            if primary_error is not None and primary_error.__cause__ is error:
                return
            cleanup_errors.append(error)

        disconnect_results = await asyncio.gather(
            *(connector.disconnect() for connector in self._connectors),
            return_exceptions=True,
        )
        for result in disconnect_results:
            if isinstance(result, BaseException):
                add_cleanup_error(result)

        producer_tasks: list[asyncio.Task[None]] = [
            *monitor_tasks,
            *[
                task
                for task in (status_task, feature_clock_task)
                if task is not None
            ],
        ]
        if producer_tasks:
            shutdown_done, shutdown_pending = await asyncio.wait(
                producer_tasks,
                timeout=self.settings.app.shutdown_timeout_seconds,
            )
            if shutdown_pending:
                for pending_task in shutdown_pending:
                    pending_task.cancel()
                cancelled_done, still_pending = await asyncio.wait(
                    shutdown_pending,
                    timeout=self.settings.app.shutdown_timeout_seconds,
                )
                shutdown_done.update(cancelled_done)
                add_cleanup_error(
                    CollectionError(
                        "collector producer shutdown exceeded its timeout"
                    )
                )
                if still_pending:
                    add_cleanup_error(
                        CollectionError(
                            "collector producer tasks remained active after cancellation"
                        )
                    )
            service_tasks = {
                task
                for task in (status_task, feature_clock_task)
                if task is not None
            }
            for producer_task in shutdown_done:
                if producer_task.cancelled():
                    if producer_task in service_tasks:
                        add_cleanup_error(
                            CollectionError(
                                "collector service was cancelled: "
                                f"{producer_task.get_name()}"
                            )
                        )
                    continue
                task_error = producer_task.exception()
                if task_error is not None:
                    add_cleanup_error(task_error)

        passive_tasks = [
            task for task in (timer_task, stop_task) if task is not None
        ]
        for passive_task in passive_tasks:
            if not passive_task.done():
                passive_task.cancel()
        if passive_tasks:
            await asyncio.gather(*passive_tasks, return_exceptions=True)

        feature_timeline_end_at: datetime | None = None
        clean_end = primary_error is None and not cleanup_errors
        try:
            feature_timeline_end_at = await self._finish_feature_ticks(
                persist_clean_end=clean_end,
            )
        except BaseException as exc:
            add_cleanup_error(exc)
        for close_operation in (
            self._writer.close,
            self._event_bus.close,
            (
                None
                if self._feature_runtime is None
                else self._feature_runtime.close
            ),
        ):
            if close_operation is None:
                continue
            try:
                await close_operation()
            except BaseException as exc:
                add_cleanup_error(exc)
        if len(cleanup_errors) == 1:
            raise cleanup_errors[0]
        if cleanup_errors:
            raise BaseExceptionGroup(
                "collection shutdown failures",
                cleanup_errors,
            )
        return feature_timeline_end_at

    async def run(
        self,
        *,
        stop_event: asyncio.Event,
        duration_seconds: float | None = None,
    ) -> CollectionSummary:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        for label, path in (
            ("raw", self.output_path),
            ("feature", self.feature_output_path),
        ):
            if path is not None and path.exists() and any(path.iterdir()):
                raise ValueError(
                    f"{label} collection output must be empty: {path}"
                )
        started_at = datetime.now(UTC)
        cpu_started = time.process_time()
        rss_samples = [current_rss_bytes()]
        in_progress_manifest = begin_collection_manifest(
            self.output_path,
            started_at=started_at,
            code_version=__version__,
            strategy_version=self.settings.app.strategy_version,
            settings_sha256=settings_fingerprint(self.settings),
        )
        try:
            await self._event_bus.start()
            await self._writer.start()
            if self._feature_runtime is not None:
                await self._feature_runtime.start()
        except BaseException as startup_error:
            startup_cleanup_errors: list[BaseException] = []
            for close_operation in (
                (
                    None
                    if self._feature_runtime is None
                    else self._feature_runtime.close
                ),
                self._writer.close,
                self._event_bus.close,
            ):
                if close_operation is None:
                    continue
                try:
                    await close_operation()
                except BaseException as cleanup_error:
                    if cleanup_error is not startup_error:
                        startup_cleanup_errors.append(cleanup_error)
            if startup_cleanup_errors:
                raise BaseExceptionGroup(
                    "collection startup and cleanup failures",
                    [startup_error, *startup_cleanup_errors],
                ) from None
            raise
        monitor_tasks: list[asyncio.Task[None]] = []
        status_task: asyncio.Task[None] | None = None
        feature_clock_task: asyncio.Task[None] | None = None
        timer_task: asyncio.Task[None] | None = None
        stop_task: asyncio.Task[bool] | None = None
        shutdown_task: asyncio.Task[datetime | None] | None = None
        terminal_snapshots: list[StreamHealthSnapshot] = []
        primary_error: BaseException | None = None
        cleanup_errors: list[BaseException] = []
        feature_timeline_end_at: datetime | None = None
        try:
            await asyncio.gather(*(connector.connect() for connector in self._connectors))
            monitor_tasks = [
                asyncio.create_task(
                    connector.wait(),
                    name=f"{connector.exchange.value.lower()}-connector-monitor",
                )
                for connector in self._connectors
            ]
            status_task = asyncio.create_task(
                self._status_loop(
                    stop_event,
                    rss_samples=rss_samples,
                    cpu_started=cpu_started,
                ),
                name="collection-status",
            )
            if self._feature_driver is not None:
                feature_clock_task = asyncio.create_task(
                    self._feature_driver.run_live_clock(stop_event),
                    name="feature-receive-time-clock",
                )
            stop_task = asyncio.create_task(stop_event.wait(), name="collection-stop")
            waiters: set[asyncio.Task[object]] = {
                *monitor_tasks,
                status_task,
                stop_task,
            }
            if feature_clock_task is not None:
                waiters.add(feature_clock_task)
            if duration_seconds is not None:
                timer_task = asyncio.create_task(
                    asyncio.sleep(duration_seconds),
                    name="collection-duration",
                )
                waiters.add(timer_task)
            done, _ = await asyncio.wait(
                waiters,
                return_when=asyncio.FIRST_COMPLETED,
            )
            connector_done = [task for task in monitor_tasks if task in done]
            if connector_done and not stop_event.is_set():
                for task in connector_done:
                    exception = task.exception()
                    if exception is not None:
                        raise CollectionError(
                            f"connector stopped with {type(exception).__name__}: {exception}"
                        ) from exception
                raise CollectionError("connector stopped unexpectedly")
            service_done = [
                task
                for task in (status_task, feature_clock_task)
                if task is not None and task in done
            ]
            if service_done and not stop_event.is_set():
                for task in service_done:
                    exception = task.exception()
                    if exception is not None:
                        raise CollectionError(
                            "collector service stopped with "
                            f"{type(exception).__name__}: {exception}"
                        ) from exception
                raise CollectionError("collector service stopped unexpectedly")
            terminal_snapshots = self.health_snapshots()
            stop_event.set()
        except BaseException as exc:
            primary_error = exc
        finally:
            if not terminal_snapshots:
                terminal_snapshots = self.health_snapshots()
            stop_event.set()
            shutdown_task = asyncio.create_task(
                self._shutdown_collection(
                    primary_error=primary_error,
                    monitor_tasks=monitor_tasks,
                    status_task=status_task,
                    feature_clock_task=feature_clock_task,
                    timer_task=timer_task,
                    stop_task=stop_task,
                ),
                name="market-data-collector-shutdown",
            )
            try:
                feature_timeline_end_at = await await_task_completion(
                    shutdown_task
                )
            except asyncio.CancelledError as exc:
                if shutdown_task.cancelled():
                    cleanup_errors.append(exc)
                else:
                    feature_timeline_end_at = shutdown_task.result()
                    if primary_error is None:
                        primary_error = exc
            except BaseException as exc:
                cleanup_errors.append(exc)
        errors = [
            error
            for error in (primary_error, *cleanup_errors)
            if error is not None
        ]
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "collection and cleanup failures",
                errors,
            )
        self._validate_success_invariants()
        if feature_timeline_end_at is None:
            raise CollectionError("collection has no feature-timeline terminal")
        raw_audit = await asyncio.to_thread(audit_raw_tree, self.output_path)
        journal_audit = await asyncio.to_thread(
            audit_normalized_journal,
            self.output_path,
            require_normalized_events=False,
        )
        normalized_event_count = sum(self._event_counts.values())
        if (
            journal_audit.normalized_event_count != normalized_event_count
            or journal_audit.terminal_marker_at != feature_timeline_end_at
        ):
            raise CollectionError(
                "normalized journal did not reconcile collection completion"
            )
        finished_at = datetime.now(UTC)
        clean_manifest = complete_collection_manifest(
            self.output_path,
            expected_run_id=in_progress_manifest.run_id,
            terminal_at=finished_at,
            feature_timeline_end_at=feature_timeline_end_at,
            normalized_event_count=normalized_event_count,
            raw_audit=raw_audit,
        )
        rss_samples.append(current_rss_bytes())
        cpu_duration = time.process_time() - cpu_started
        wall_duration = (finished_at - started_at).total_seconds()
        resources = CollectionResourceMetrics(
            initial_rss_bytes=rss_samples[0],
            final_rss_bytes=rss_samples[-1],
            peak_rss_bytes=max(rss_samples),
            rss_growth_bytes=rss_samples[-1] - rss_samples[0],
            process_cpu_seconds=cpu_duration,
            process_cpu_percent_of_one_core=(
                0.0
                if wall_duration == 0
                else cpu_duration / wall_duration * 100.0
            ),
        )
        statuses = Counter(snapshot.status.value for snapshot in terminal_snapshots)
        return CollectionSummary(
            started_at=started_at,
            finished_at=finished_at,
            output_path=self.output_path,
            normalized_event_counts=dict(self._event_counts),
            health_status_counts=dict(statuses),
            parquet=self._writer.stats,
            pipeline=self._event_bus.stats,
            feature_state=self._feature_state.stats,
            feature_output_path=self.feature_output_path,
            feature_runtime=(
                None
                if self._feature_runtime is None
                else self._feature_runtime.stats
            ),
            resources=resources,
            collection_manifest=clean_manifest,
        )

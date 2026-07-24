"""Phase-2 public market-data collection orchestration."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from cvf.config import Settings
from cvf.exchanges.binance import BinanceMarketDataConnector
from cvf.exchanges.okx import OKXMarketDataConnector
from cvf.features import FeatureStatePipeline, FeatureStatePipelineStats, MarketStateStore
from cvf.monitoring import StreamHealthRegistry, StreamHealthSnapshot, StreamKey
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline import ConsumerStats, NormalizedEventBus
from cvf.storage import AsyncPartitionedParquetWriter, ParquetWriterStats, RawMarketRecord


class CollectionError(RuntimeError):
    """Raised when a live connector exits before collection is stopped."""


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
        event_bus: NormalizedEventBus | None = None,
    ) -> None:
        self.settings = settings
        self.output_path = (output_path or settings.storage.raw_data_path).resolve()
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
        self._feature_state = FeatureStatePipeline(MarketStateStore(settings.features))
        self._event_bus.register(
            "feature-state",
            self._feature_state.consume,
            queue_capacity=settings.pipeline.consumer_queue_capacity,
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

    async def _record_event(self, event: NormalizedMarketEvent) -> None:
        self._event_counts[event.event_type.value] += 1
        await self._event_bus.publish(event)

    def _record_backpressure(self, record: RawMarketRecord) -> None:
        key = StreamKey(record.exchange, record.symbol, record.channel)
        self._health.record_drop(key, backpressure=True)

    def health_snapshots(self, *, now: datetime | None = None) -> list[StreamHealthSnapshot]:
        checked_at = now or datetime.now(UTC)
        snapshots: list[StreamHealthSnapshot] = []
        for connector in self._connectors:
            snapshots.extend(connector.health_snapshots(now=checked_at))
        return snapshots

    async def _status_loop(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            snapshots = self.health_snapshots()
            for snapshot in snapshots:
                self._feature_state.store.update_stream_health(snapshot)
            self._logger.info(
                "market-data collection status",
                extra={
                    "event": "collection_status",
                    "normalized_event_counts": dict(self._event_counts),
                    "parquet": asdict(self._writer.stats),
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

    async def run(
        self,
        *,
        stop_event: asyncio.Event,
        duration_seconds: float | None = None,
    ) -> CollectionSummary:
        if duration_seconds is not None and duration_seconds <= 0:
            raise ValueError("duration_seconds must be positive")
        started_at = datetime.now(UTC)
        await self._event_bus.start()
        try:
            await self._writer.start()
        except Exception:
            await self._event_bus.close()
            raise
        monitor_tasks: list[asyncio.Task[None]] = []
        status_task: asyncio.Task[None] | None = None
        timer_task: asyncio.Task[None] | None = None
        stop_task: asyncio.Task[bool] | None = None
        terminal_snapshots: list[StreamHealthSnapshot] = []
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
                self._status_loop(stop_event),
                name="collection-status",
            )
            stop_task = asyncio.create_task(stop_event.wait(), name="collection-stop")
            waiters: set[asyncio.Task[object]] = {
                *monitor_tasks,
                stop_task,
            }
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
            terminal_snapshots = self.health_snapshots()
            stop_event.set()
        finally:
            if not terminal_snapshots:
                terminal_snapshots = self.health_snapshots()
            stop_event.set()
            await asyncio.gather(
                *(connector.disconnect() for connector in self._connectors),
                return_exceptions=False,
            )
            cleanup_tasks: list[asyncio.Task[Any]] = [
                *monitor_tasks,
                *[
                    task
                    for task in (status_task, timer_task, stop_task)
                    if task is not None
                ],
            ]
            for task in cleanup_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                *cleanup_tasks,
                return_exceptions=True,
            )
            try:
                await self._writer.close()
            finally:
                await self._event_bus.close()
        finished_at = datetime.now(UTC)
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
        )

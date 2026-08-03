"""Shared live/replay Phase-3 feature calculation and persistence runtime."""

from __future__ import annotations

import asyncio
import heapq
import time
from collections import Counter, deque
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cvf.clock import DecisionScheduler, DecisionTick, TickKind
from cvf.config import Settings
from cvf.features.cross_venue import CrossVenueFeatureEngine
from cvf.features.models import (
    AlignmentStatus,
    CrossVenueFeatureSnapshot,
    FeatureSnapshot,
    FeatureUnavailableCode,
)
from cvf.features.pipeline import FeatureStatePipeline, FeatureStatePipelineStats
from cvf.features.single_venue import SingleVenueFeatureEngine
from cvf.features.state import MarketStateStore
from cvf.models.enums import EventType, Exchange
from cvf.models.market import ExchangeHealth, OrderBookSnapshot, OrderBookUpdate
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline import NormalizedEventBus
from cvf.storage.features import (
    AsyncFeatureParquetWriter,
    FeatureWriterStats,
)
from cvf.utils.async_lifecycle import await_task_completion
from cvf.utils.fingerprint import model_payload_json, sha256_text


@dataclass(frozen=True, slots=True)
class RuntimeLatency:
    samples: int
    average_ms: float | None
    minimum_ms: float | None
    maximum_ms: float | None


@dataclass(frozen=True, slots=True)
class FeatureRuntimeStats:
    feature_state: FeatureStatePipelineStats
    writer: FeatureWriterStats
    normalized_events: int
    feature_ticks: int
    reserved_signal_boundaries: int
    single_venue_snapshots: int
    cross_venue_snapshots: int
    unavailable_snapshots: int
    open_interest_stale_snapshots: int
    non_aligned_cross_venue_snapshots: int
    unavailable_reason_counts: dict[str, int]
    book_generation_rebuilds: int
    no_lookahead_violations: int
    output_event_type_counts: dict[str, int]
    receive_latency: RuntimeLatency
    calculation_latency: RuntimeLatency
    enqueue_latency: RuntimeLatency


class _LatencyAccumulator:
    def __init__(self) -> None:
        self._samples = 0
        self._total = 0.0
        self._minimum: float | None = None
        self._maximum: float | None = None

    def add(self, value_ms: float) -> None:
        self._samples += 1
        self._total += value_ms
        self._minimum = (
            value_ms if self._minimum is None else min(self._minimum, value_ms)
        )
        self._maximum = (
            value_ms if self._maximum is None else max(self._maximum, value_ms)
        )

    @property
    def summary(self) -> RuntimeLatency:
        return RuntimeLatency(
            samples=self._samples,
            average_ms=(
                None if self._samples == 0 else self._total / self._samples
            ),
            minimum_ms=self._minimum,
            maximum_ms=self._maximum,
        )


@dataclass(order=True, slots=True)
class _QueuedFeatureEvent:
    receive_timestamp: datetime
    exchange: str
    symbol: str
    event_type: str
    sequence_id: str
    raw_payload_reference: str
    content_identity: str
    ordinal: int
    event: NormalizedMarketEvent = field(compare=False)


class ReceiveTimeFeatureDriver:
    """Bound and order one normalized receive-time timeline for live and replay."""

    def __init__(
        self,
        settings: Settings,
        *,
        event_bus: NormalizedEventBus,
        runtime: FeatureRuntime,
    ) -> None:
        self._settings = settings
        self._event_bus = event_bus
        self._runtime = runtime
        self._lateness = timedelta(
            milliseconds=settings.features.receive_time_reorder_ms
        )
        self._capacity = settings.pipeline.consumer_queue_capacity
        self._scheduler: DecisionScheduler | None = None
        self._lock = asyncio.Lock()
        self._events: list[_QueuedFeatureEvent] = []
        self._ordinal = 0
        self._watermark: datetime | None = None
        self._maximum_seen: datetime | None = None
        self._sealed = False
        self._failure: BaseException | None = None

    @staticmethod
    def _utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("feature timeline timestamps must be timezone-aware")
        return value.astimezone(UTC)

    def _queue_item(
        self,
        event: NormalizedMarketEvent,
        timestamp: datetime,
    ) -> _QueuedFeatureEvent:
        self._ordinal += 1
        return _QueuedFeatureEvent(
            receive_timestamp=timestamp,
            exchange=event.exchange.value,
            symbol=event.symbol,
            event_type=event.event_type.value,
            sequence_id="" if event.sequence_id is None else str(event.sequence_id),
            raw_payload_reference=event.raw_payload_reference or "",
            content_identity=sha256_text(model_payload_json(event)),
            ordinal=self._ordinal,
            event=event,
        )

    async def _emit_ticks_through(self, target: datetime) -> None:
        scheduler = self._scheduler
        if scheduler is None or target < scheduler.cursor:
            return
        if not scheduler.has_due_tick(target):
            return
        await self._event_bus.drain()
        for tick in scheduler.advance_to(target):
            await self._runtime.consume_tick(tick)

    def _raise_if_failed(self) -> None:
        if self._failure is not None:
            raise RuntimeError("feature timeline previously failed") from self._failure

    async def _complete_despite_cancellation(
        self,
        operation: Awaitable[None],
    ) -> None:
        task = asyncio.ensure_future(operation)
        await await_task_completion(task)

    async def _run_transition(self, operation: Awaitable[None]) -> None:
        try:
            await self._complete_despite_cancellation(operation)
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._failure = exc
            raise

    async def _advance_locked(self, target: datetime) -> None:
        if self._watermark is not None and target <= self._watermark:
            return
        while self._events and self._events[0].receive_timestamp <= target:
            event_timestamp = self._events[0].receive_timestamp
            if self._scheduler is None:
                self._scheduler = DecisionScheduler(
                    start=event_timestamp,
                    feature_interval=timedelta(
                        seconds=self._settings.timing.feature_update_seconds
                    ),
                    signal_interval=timedelta(
                        seconds=self._settings.timing.signal_check_seconds
                    ),
                )
            before_event = event_timestamp - timedelta(microseconds=1)
            await self._emit_ticks_through(before_event)
            while (
                self._events
                and self._events[0].receive_timestamp == event_timestamp
            ):
                queued = heapq.heappop(self._events)
                await self._event_bus.publish(queued.event)
        await self._emit_ticks_through(target)
        self._watermark = target

    async def _publish_locked(
        self,
        event: NormalizedMarketEvent,
        timestamp: datetime,
    ) -> None:
        await self._advance_locked(timestamp - self._lateness)
        if len(self._events) >= self._capacity:
            raise RuntimeError("feature timeline reorder buffer is full")
        heapq.heappush(self._events, self._queue_item(event, timestamp))
        self._maximum_seen = (
            timestamp
            if self._maximum_seen is None
            else max(self._maximum_seen, timestamp)
        )

    async def publish(self, event: NormalizedMarketEvent) -> None:
        """Buffer an event, reorder within the configured bound, and advance."""

        async with self._lock:
            self._raise_if_failed()
            if self._sealed:
                raise RuntimeError("cannot publish after feature timeline finish")
            timestamp = self._utc(event.local_receive_timestamp)
            if self._watermark is not None and timestamp <= self._watermark:
                raise RuntimeError(
                    "normalized event arrived behind the receive-time watermark"
                )
            await self._run_transition(self._publish_locked(event, timestamp))

    async def advance_to(self, timestamp: datetime) -> None:
        """Advance a live wall-clock watermark without allowing future data."""

        async with self._lock:
            self._raise_if_failed()
            if self._sealed:
                return
            await self._run_transition(self._advance_locked(self._utc(timestamp)))

    async def run_live_clock(self, stop_event: asyncio.Event) -> None:
        """Emit live boundaries during quiet periods using a bounded watermark."""

        polling_seconds = min(
            0.25,
            max(0.01, self._settings.timing.feature_update_seconds / 4),
        )
        while not stop_event.is_set():
            await self.advance_to(datetime.now(UTC) - self._lateness)
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=polling_seconds,
                )
            except TimeoutError:
                continue

    async def finish(self, *, through_timestamp: datetime | None = None) -> None:
        """Flush all buffered events, emit final boundaries, and seal the timeline."""

        async with self._lock:
            self._raise_if_failed()
            if self._sealed:
                return

            async def finish_locked() -> None:
                candidates = [
                    value
                    for value in (self._maximum_seen, through_timestamp)
                    if value is not None
                ]
                if candidates:
                    await self._advance_locked(
                        max(self._utc(value) for value in candidates)
                    )
                await self._event_bus.drain()
                self._sealed = True

            await self._run_transition(finish_locked())


class FeatureRuntime:
    """Own the exact feature engines and writer used by live and replay paths."""

    def __init__(
        self,
        settings: Settings,
        *,
        output_path: Path,
        writer_batch_rows: int | None = None,
        writer_flush_seconds: float | None = None,
    ) -> None:
        self.settings = settings
        self.output_path = output_path.resolve()
        self.state = MarketStateStore(settings.features)
        for exchange in (Exchange.BINANCE, Exchange.OKX):
            for symbol in settings.markets.canonical_symbols:
                self.state.state(exchange, symbol)
        self.feature_state = FeatureStatePipeline(self.state)
        self.writer = AsyncFeatureParquetWriter(
            root_path=self.output_path,
            settings=settings,
            batch_rows=writer_batch_rows,
            flush_seconds=writer_flush_seconds,
            queue_capacity=settings.storage.feature_parquet_queue_capacity,
            deduplication_capacity=settings.storage.feature_deduplication_capacity,
        )
        self._single_engine = SingleVenueFeatureEngine(settings)
        self._cross_engine = CrossVenueFeatureEngine(settings)
        history_items = max(
            2,
            int(
                settings.features.zscore_lookback_seconds
                / settings.timing.feature_update_seconds
            )
            + 2,
        )
        self._history_items = history_items
        self._history: dict[
            tuple[Exchange, str, int],
            deque[FeatureSnapshot],
        ] = {}
        self._universe = frozenset(
            (exchange, symbol)
            for exchange in (Exchange.BINANCE, Exchange.OKX)
            for symbol in settings.markets.canonical_symbols
        )
        self._started = False
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None
        self._normalized_events = 0
        self._feature_ticks = 0
        self._signal_boundaries = 0
        self._single_snapshots = 0
        self._cross_snapshots = 0
        self._unavailable_snapshots = 0
        self._open_interest_stale_snapshots = 0
        self._non_aligned_cross_snapshots = 0
        self._unavailable_reasons: Counter[str] = Counter()
        self._book_generations: dict[tuple[Exchange, str], int] = {}
        self._book_generation_rebuilds = 0
        self._no_lookahead_violations = 0
        self._output_event_types: Counter[str] = Counter()
        self._receive_latency = _LatencyAccumulator()
        self._calculation_latency = _LatencyAccumulator()
        self._enqueue_latency = _LatencyAccumulator()

    @property
    def stats(self) -> FeatureRuntimeStats:
        return FeatureRuntimeStats(
            feature_state=self.feature_state.stats,
            writer=self.writer.stats,
            normalized_events=self._normalized_events,
            feature_ticks=self._feature_ticks,
            reserved_signal_boundaries=self._signal_boundaries,
            single_venue_snapshots=self._single_snapshots,
            cross_venue_snapshots=self._cross_snapshots,
            unavailable_snapshots=self._unavailable_snapshots,
            open_interest_stale_snapshots=self._open_interest_stale_snapshots,
            non_aligned_cross_venue_snapshots=self._non_aligned_cross_snapshots,
            unavailable_reason_counts=dict(
                sorted(self._unavailable_reasons.items())
            ),
            book_generation_rebuilds=self._book_generation_rebuilds,
            no_lookahead_violations=self._no_lookahead_violations,
            output_event_type_counts=dict(sorted(self._output_event_types.items())),
            receive_latency=self._receive_latency.summary,
            calculation_latency=self._calculation_latency.summary,
            enqueue_latency=self._enqueue_latency.summary,
        )

    async def start(self) -> None:
        if self._closed or self._close_task is not None:
            raise RuntimeError("cannot restart a closed feature runtime")
        if not self._started:
            await self.writer.start()
            self._started = True

    async def close(self) -> None:
        if self._closed:
            return
        if self._close_task is None:

            async def close_runtime() -> None:
                await self.writer.close()
                self._closed = True

            self._close_task = asyncio.create_task(
                close_runtime(),
                name="feature-runtime-close",
            )
        await await_task_completion(self._close_task)

    async def consume_event(self, event: NormalizedMarketEvent) -> None:
        if (
            not self._started
            or self._closed
            or self._close_task is not None
        ):
            raise RuntimeError("feature runtime is not active")
        if event.exchange not in (Exchange.BINANCE, Exchange.OKX):
            raise ValueError("feature runtime requires a concrete market-data exchange")
        if event.symbol == "*":
            if not isinstance(event, ExchangeHealth):
                raise ValueError("only exchange-health events may use wildcard symbols")
        elif (event.exchange, event.symbol) not in self._universe:
            raise ValueError(
                "normalized event is outside the configured feature universe: "
                f"{event.exchange.value}:{event.symbol}"
            )
        self._normalized_events += 1
        self._receive_latency.add(
            (
                event.local_receive_timestamp - event.exchange_timestamp
            ).total_seconds()
            * 1000.0
        )
        await self.feature_state.consume(event)
        if isinstance(event, (OrderBookSnapshot, OrderBookUpdate)):
            key = (event.exchange, event.symbol)
            book_state = self.state.state(*key).order_book
            current = book_state.generation
            previous = self._book_generations.get(key)
            if previous is not None and previous != current:
                self._book_generation_rebuilds += 1
                for history_key in tuple(self._history):
                    if history_key[1] == event.symbol:
                        del self._history[history_key]
            self._book_generations[key] = current

    def _history_for(
        self,
        exchange: Exchange,
        symbol: str,
        window_seconds: int,
    ) -> deque[FeatureSnapshot]:
        return self._history.setdefault(
            (exchange, symbol, window_seconds),
            deque(maxlen=self._history_items),
        )

    @staticmethod
    def _source_is_in_the_future(
        snapshot: FeatureSnapshot | CrossVenueFeatureSnapshot,
    ) -> bool:
        newest = snapshot.newest_source_timestamp
        if newest is not None and newest > snapshot.decision_timestamp:
            return True
        if isinstance(snapshot, CrossVenueFeatureSnapshot):
            return any(
                source is not None and source > snapshot.decision_timestamp
                for source in (
                    snapshot.alignment.binance_source_timestamp,
                    snapshot.alignment.okx_source_timestamp,
                )
            )
        return False

    def _record_snapshot(
        self,
        snapshot: FeatureSnapshot | CrossVenueFeatureSnapshot,
    ) -> None:
        self._output_event_types[snapshot.event_type.value] += 1
        if snapshot.event_type is not EventType.MARKET_FEATURE:
            self._no_lookahead_violations += 1
            raise RuntimeError("feature runtime produced a forbidden event type")
        reasons = tuple(reason.code for reason in snapshot.unavailable_reasons)
        if reasons:
            self._unavailable_snapshots += 1
            self._unavailable_reasons.update(reason.value for reason in reasons)
        if FeatureUnavailableCode.OPEN_INTEREST_STALE in reasons:
            self._open_interest_stale_snapshots += 1
        if (
            isinstance(snapshot, CrossVenueFeatureSnapshot)
            and snapshot.alignment.status is not AlignmentStatus.ALIGNED
        ):
            self._non_aligned_cross_snapshots += 1
        if self._source_is_in_the_future(snapshot):
            self._no_lookahead_violations += 1
            raise RuntimeError(
                "future source detected in feature snapshot "
                f"{snapshot.feature_snapshot_id}"
            )

    async def consume_tick(self, tick: DecisionTick) -> None:
        if (
            not self._started
            or self._closed
            or self._close_task is not None
        ):
            raise RuntimeError("feature runtime is not active")
        if tick.kind is TickKind.SIGNAL:
            self._signal_boundaries += 1
            return
        self._feature_ticks += 1
        calculation_started = time.perf_counter()
        singles = self._single_engine.calculate_all(
            self.state.states,
            decision_timestamp=tick.timestamp,
        )
        for snapshot in singles:
            self._history_for(
                snapshot.exchange,
                snapshot.symbol,
                snapshot.window_seconds,
            ).append(snapshot)
        crosses: list[CrossVenueFeatureSnapshot] = []
        for symbol in self.settings.markets.canonical_symbols:
            for window_seconds in self.settings.timing.feature_windows_seconds:
                candidates: list[FeatureSnapshot] = []
                for exchange in (Exchange.BINANCE, Exchange.OKX):
                    candidates.extend(
                        self._history_for(exchange, symbol, window_seconds)
                    )
                crosses.append(
                    self._cross_engine.calculate(
                        candidates,
                        symbol=symbol,
                        decision_timestamp=tick.timestamp,
                        window_seconds=window_seconds,
                    )
                )
        self._calculation_latency.add(
            (time.perf_counter() - calculation_started) * 1000.0
        )

        enqueue_started = time.perf_counter()
        snapshots: Iterable[FeatureSnapshot | CrossVenueFeatureSnapshot] = (
            *singles,
            *crosses,
        )
        for output_snapshot in snapshots:
            self._record_snapshot(output_snapshot)
            await self.writer.write(output_snapshot)
        self._enqueue_latency.add(
            (time.perf_counter() - enqueue_started) * 1000.0
        )
        self._single_snapshots += len(singles)
        self._cross_snapshots += len(crosses)

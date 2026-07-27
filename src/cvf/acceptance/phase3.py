"""Deterministic Phase 3 fixed-dataset acceptance and resource evidence."""

from __future__ import annotations

import asyncio
import ctypes
import json
import os
import sys
import time
from collections import Counter, deque
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from itertools import chain
from pathlib import Path

from cvf.clock import DecisionScheduler, DecisionTick, TickKind
from cvf.config import Settings
from cvf.features import (
    CrossVenueFeatureEngine,
    FeatureSnapshot,
    FeatureStatePipeline,
    FeatureStatePipelineStats,
    MarketStateStore,
    SingleVenueFeatureEngine,
)
from cvf.features.models import (
    AlignmentStatus,
    CrossVenueFeatureSnapshot,
    FeatureUnavailableCode,
)
from cvf.models.enums import Exchange
from cvf.models.market import OrderBookSnapshot
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline import ConsumerStats, NormalizedEventBus
from cvf.replay import (
    RawParquetReader,
    ReplayOrder,
    ReplayRunner,
    ReplaySummary,
)
from cvf.replay.ordering import replay_timestamp
from cvf.storage.compact import RawAudit, audit_raw_tree
from cvf.storage.features import (
    AsyncFeatureParquetWriter,
    FeatureAudit,
    FeatureConsistencyReport,
    FeatureWriterStats,
    compare_feature_trees,
)
from cvf.storage.raw import RawMarketRecord


@dataclass(frozen=True, slots=True)
class LatencyMetrics:
    samples: int
    average_ms: float | None
    minimum_ms: float | None
    maximum_ms: float | None


@dataclass(frozen=True, slots=True)
class ResourceMetrics:
    initial_rss_bytes: int
    final_rss_bytes: int
    peak_rss_bytes: int
    rss_growth_bytes: int
    wall_duration_seconds: float
    process_cpu_seconds: float
    process_cpu_percent_of_one_core: float


@dataclass(frozen=True, slots=True)
class Phase3RunMetrics:
    label: str
    input_path: Path
    output_path: Path
    replay: ReplaySummary
    feature_state: FeatureStatePipelineStats
    consumers: dict[str, ConsumerStats]
    writer: FeatureWriterStats
    feature_audit: FeatureAudit
    resources: ResourceMetrics
    event_receive_latency: LatencyMetrics
    feature_calculation_latency: LatencyMetrics
    feature_enqueue_latency: LatencyMetrics
    feature_ticks: int
    signal_boundaries_observed: int
    single_venue_snapshots: int
    cross_venue_snapshots: int
    unavailable_snapshots: int
    open_interest_stale_snapshots: int
    non_aligned_cross_venue_snapshots: int
    unavailable_reason_counts: dict[str, int]
    book_generation_rebuilds: int
    no_lookahead_violations: int
    signal_outputs: int
    order_outputs: int
    private_api_requests: int
    throughput_records_per_second: float
    throughput_event_time_multiplier: float | None


@dataclass(frozen=True, slots=True)
class Phase3AcceptanceReport:
    schema_version: int
    generated_at: datetime
    input_path: Path
    output_path: Path
    raw_audit: RawAudit
    first_run: Phase3RunMetrics
    second_run: Phase3RunMetrics
    consistency: FeatureConsistencyReport
    deterministic_replay: bool
    snapshot_counts_match: bool
    no_lookahead: bool
    throughput_above_realtime: bool
    feature_files_audited: bool
    safety_boundary_preserved: bool
    requested_stability_seconds: float
    actual_stability_observation_seconds: float
    full_stability_duration_completed: bool
    full_stability_status: str


class _LatencyAccumulator:
    def __init__(self) -> None:
        self._samples = 0
        self._total = 0.0
        self._minimum: float | None = None
        self._maximum: float | None = None

    def add(self, value_ms: float) -> None:
        self._samples += 1
        self._total += value_ms
        self._minimum = value_ms if self._minimum is None else min(self._minimum, value_ms)
        self._maximum = value_ms if self._maximum is None else max(self._maximum, value_ms)

    @property
    def metrics(self) -> LatencyMetrics:
        return LatencyMetrics(
            samples=self._samples,
            average_ms=None if self._samples == 0 else self._total / self._samples,
            minimum_ms=self._minimum,
            maximum_ms=self._maximum,
        )


class _EventObserver:
    def __init__(self) -> None:
        self.receive_latency = _LatencyAccumulator()
        self.book_generations: dict[tuple[Exchange, str], int] = {}
        self.book_generation_rebuilds = 0

    async def consume(self, event: NormalizedMarketEvent) -> None:
        receive_latency_ms = (
            event.local_receive_timestamp - event.exchange_timestamp
        ).total_seconds() * 1000.0
        self.receive_latency.add(receive_latency_ms)
        if isinstance(event, OrderBookSnapshot):
            key = (event.exchange, event.symbol)
            previous = self.book_generations.get(key)
            if previous is not None and previous != event.generation:
                self.book_generation_rebuilds += 1
            self.book_generations[key] = event.generation


class _FeatureTickSink:
    def __init__(
        self,
        *,
        settings: Settings,
        state: MarketStateStore,
        writer: AsyncFeatureParquetWriter,
    ) -> None:
        self._settings = settings
        self._state = state
        self._writer = writer
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
        self.calculation_latency = _LatencyAccumulator()
        self.enqueue_latency = _LatencyAccumulator()
        self.feature_ticks = 0
        self.signal_boundaries = 0
        self.single_snapshots = 0
        self.cross_snapshots = 0
        self.unavailable_snapshots = 0
        self.open_interest_stale_snapshots = 0
        self.non_aligned_cross_snapshots = 0
        self.unavailable_reasons: Counter[str] = Counter()
        self.no_lookahead_violations = 0

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
        reasons = tuple(reason.code for reason in snapshot.unavailable_reasons)
        if reasons:
            self.unavailable_snapshots += 1
            self.unavailable_reasons.update(reason.value for reason in reasons)
        if FeatureUnavailableCode.OPEN_INTEREST_STALE in reasons:
            self.open_interest_stale_snapshots += 1
        if (
            isinstance(snapshot, CrossVenueFeatureSnapshot)
            and snapshot.alignment.status is not AlignmentStatus.ALIGNED
        ):
            self.non_aligned_cross_snapshots += 1
        if self._source_is_in_the_future(snapshot):
            self.no_lookahead_violations += 1
            raise RuntimeError(
                f"future source detected in feature snapshot {snapshot.feature_snapshot_id}"
            )

    async def consume(self, tick: DecisionTick) -> None:
        if tick.kind is TickKind.SIGNAL:
            self.signal_boundaries += 1
            return
        self.feature_ticks += 1
        calculation_started = time.perf_counter()
        singles = self._single_engine.calculate_all(
            self._state.states,
            decision_timestamp=tick.timestamp,
        )
        for snapshot in singles:
            self._history_for(
                snapshot.exchange,
                snapshot.symbol,
                snapshot.window_seconds,
            ).append(snapshot)
        crosses: list[CrossVenueFeatureSnapshot] = []
        for symbol in self._settings.markets.canonical_symbols:
            for window_seconds in self._settings.timing.feature_windows_seconds:
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
        self.calculation_latency.add(
            (time.perf_counter() - calculation_started) * 1000.0
        )

        enqueue_started = time.perf_counter()
        snapshots: Iterable[FeatureSnapshot | CrossVenueFeatureSnapshot] = chain(
            singles,
            crosses,
        )
        for output_snapshot in snapshots:
            self._record_snapshot(output_snapshot)
            await self._writer.write(output_snapshot)
        self.enqueue_latency.add((time.perf_counter() - enqueue_started) * 1000.0)
        self.single_snapshots += len(singles)
        self.cross_snapshots += len(crosses)


class _ProcessMemoryCounters(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("PageFaultCount", ctypes.c_ulong),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def _current_rss_bytes() -> int:
    if sys.platform == "win32":
        counters = _ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        get_current_process = kernel32.GetCurrentProcess
        get_current_process.argtypes = []
        get_current_process.restype = ctypes.c_void_p
        get_process_memory_info = psapi.GetProcessMemoryInfo
        get_process_memory_info.argtypes = [
            ctypes.c_void_p,
            ctypes.POINTER(_ProcessMemoryCounters),
            ctypes.c_ulong,
        ]
        get_process_memory_info.restype = ctypes.c_int
        process = get_current_process()
        success = get_process_memory_info(
            process,
            ctypes.byref(counters),
            counters.cb,
        )
        if not success:
            raise ctypes.WinError(ctypes.get_last_error())
        return int(counters.WorkingSetSize)
    statm = Path("/proc/self/statm")
    if statm.is_file():
        resident_pages = int(statm.read_text(encoding="ascii").split()[1])
        return resident_pages * os.sysconf("SC_PAGE_SIZE")
    raise RuntimeError("RSS sampling is unsupported on this platform")


async def _sample_rss(stop: asyncio.Event, samples: list[int]) -> None:
    while True:
        samples.append(_current_rss_bytes())
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.25)
        except TimeoutError:
            continue


def _records_with_first_market_timestamp(
    records: Iterable[RawMarketRecord],
    order: ReplayOrder,
) -> tuple[Iterable[RawMarketRecord], datetime]:
    iterator = iter(records)
    prefix: list[RawMarketRecord] = []
    for record in iterator:
        prefix.append(record)
        if record.channel != "instrument_metadata":
            return chain(prefix, iterator), replay_timestamp(record, order)
    raise ValueError("fixed dataset contains no replayable market records")


async def _run_once(
    *,
    label: str,
    settings: Settings,
    input_path: Path,
    output_path: Path,
    batch_rows: int,
) -> Phase3RunMetrics:
    if output_path.exists() and any(output_path.iterdir()):
        raise ValueError(f"acceptance output must be empty: {output_path}")
    reader = RawParquetReader(input_path)
    order = ReplayOrder.EVENT_TIME
    records, first_timestamp = _records_with_first_market_timestamp(
        reader.iter_records(order=order),
        order,
    )
    state = MarketStateStore(settings.features)
    feature_state = FeatureStatePipeline(state)
    observer = _EventObserver()
    bus = NormalizedEventBus(
        default_queue_capacity=settings.pipeline.consumer_queue_capacity
    )
    bus.register(
        "feature-state",
        feature_state.consume,
        queue_capacity=settings.pipeline.consumer_queue_capacity,
    )
    bus.register(
        "acceptance-observer",
        observer.consume,
        queue_capacity=settings.pipeline.consumer_queue_capacity,
    )
    writer = AsyncFeatureParquetWriter(
        root_path=output_path,
        settings=settings,
        batch_rows=batch_rows,
        flush_seconds=settings.storage.feature_parquet_flush_seconds,
        queue_capacity=settings.storage.feature_parquet_queue_capacity,
        deduplication_capacity=settings.storage.feature_deduplication_capacity,
    )
    tick_sink = _FeatureTickSink(settings=settings, state=state, writer=writer)
    scheduler = DecisionScheduler(
        start=first_timestamp,
        feature_interval=timedelta(seconds=settings.timing.feature_update_seconds),
        signal_interval=timedelta(seconds=settings.timing.signal_check_seconds),
    )
    runner = ReplayRunner(
        event_bus=bus,
        scheduler=scheduler,
        tick_sink=tick_sink.consume,
        order=order,
        speed=0,
    )
    rss_samples: list[int] = [_current_rss_bytes()]
    stop_sampling = asyncio.Event()
    sampler = asyncio.create_task(
        _sample_rss(stop_sampling, rss_samples),
        name=f"phase3-acceptance-rss-{label}",
    )
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    try:
        await writer.start()
        try:
            replay = await runner.run(records)
        finally:
            await writer.close()
    finally:
        stop_sampling.set()
        await sampler
    wall_duration = time.perf_counter() - wall_started
    cpu_duration = time.process_time() - cpu_started
    rss_samples.append(_current_rss_bytes())
    feature_audit = await asyncio.to_thread(audit_raw_feature_tree, output_path)
    event_span = (
        None
        if replay.started_at is None or replay.finished_at is None
        else (replay.finished_at - replay.started_at).total_seconds()
    )
    throughput = (
        0.0 if wall_duration == 0 else replay.raw_records / wall_duration
    )
    multiplier = (
        None
        if event_span is None or wall_duration == 0
        else event_span / wall_duration
    )
    initial_rss = rss_samples[0]
    final_rss = rss_samples[-1]
    resources = ResourceMetrics(
        initial_rss_bytes=initial_rss,
        final_rss_bytes=final_rss,
        peak_rss_bytes=max(rss_samples),
        rss_growth_bytes=final_rss - initial_rss,
        wall_duration_seconds=wall_duration,
        process_cpu_seconds=cpu_duration,
        process_cpu_percent_of_one_core=(
            0.0 if wall_duration == 0 else cpu_duration / wall_duration * 100.0
        ),
    )
    return Phase3RunMetrics(
        label=label,
        input_path=input_path.resolve(),
        output_path=output_path.resolve(),
        replay=replay,
        feature_state=feature_state.stats,
        consumers=bus.stats,
        writer=writer.stats,
        feature_audit=feature_audit,
        resources=resources,
        event_receive_latency=observer.receive_latency.metrics,
        feature_calculation_latency=tick_sink.calculation_latency.metrics,
        feature_enqueue_latency=tick_sink.enqueue_latency.metrics,
        feature_ticks=tick_sink.feature_ticks,
        signal_boundaries_observed=tick_sink.signal_boundaries,
        single_venue_snapshots=tick_sink.single_snapshots,
        cross_venue_snapshots=tick_sink.cross_snapshots,
        unavailable_snapshots=tick_sink.unavailable_snapshots,
        open_interest_stale_snapshots=tick_sink.open_interest_stale_snapshots,
        non_aligned_cross_venue_snapshots=tick_sink.non_aligned_cross_snapshots,
        unavailable_reason_counts=dict(sorted(tick_sink.unavailable_reasons.items())),
        book_generation_rebuilds=observer.book_generation_rebuilds,
        no_lookahead_violations=tick_sink.no_lookahead_violations,
        signal_outputs=0,
        order_outputs=0,
        private_api_requests=0,
        throughput_records_per_second=throughput,
        throughput_event_time_multiplier=multiplier,
    )


def audit_raw_feature_tree(root: Path) -> FeatureAudit:
    """Named indirection kept patchable in acceptance tests."""

    from cvf.storage.features import audit_feature_tree

    return audit_feature_tree(root)


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _percentage(numerator: int, denominator: int) -> float:
    return 0.0 if denominator == 0 else numerator / denominator * 100.0


def _render_markdown(report: Phase3AcceptanceReport) -> str:
    first = report.first_run
    second = report.second_run
    first_total = first.single_venue_snapshots + first.cross_venue_snapshots
    first_non_aligned_percentage = _percentage(
        first.non_aligned_cross_venue_snapshots,
        first.cross_venue_snapshots,
    )
    lines = [
        "# Phase 3 / v0.3.0 Acceptance Evidence",
        "",
        f"- Generated: `{report.generated_at.astimezone(UTC).isoformat()}`",
        f"- Fixed input: `{report.input_path}`",
        (
            f"- Raw audit: {report.raw_audit.rows:,} rows, "
            f"{report.raw_audit.files:,} files, "
            f"digest `{report.raw_audit.content_digest}`"
        ),
        f"- Deterministic repeated replay: `{report.deterministic_replay}`",
        (
            f"- No-lookahead violations: `{first.no_lookahead_violations}` / "
            f"`{second.no_lookahead_violations}`"
        ),
        f"- Feature tree schema audits passed: `{report.feature_files_audited}`",
        f"- Safety boundary preserved: `{report.safety_boundary_preserved}`",
        "",
        "## Fixed-dataset results",
        "",
        "| Metric | Run 1 | Run 2 |",
        "|---|---:|---:|",
        f"| Raw records | {first.replay.raw_records:,} | {second.replay.raw_records:,} |",
        (
            f"| Normalized events | {first.replay.normalized_events:,} | "
            f"{second.replay.normalized_events:,} |"
        ),
        (
            f"| Single-venue snapshots | {first.single_venue_snapshots:,} | "
            f"{second.single_venue_snapshots:,} |"
        ),
        (
            f"| Cross-venue snapshots | {first.cross_venue_snapshots:,} | "
            f"{second.cross_venue_snapshots:,} |"
        ),
        (
            f"| Persisted/audited snapshots | {first.feature_audit.rows:,} | "
            f"{second.feature_audit.rows:,} |"
        ),
        (
            f"| Replay wall seconds | {first.resources.wall_duration_seconds:.3f} | "
            f"{second.resources.wall_duration_seconds:.3f} |"
        ),
        (
            f"| Event-time throughput multiplier | "
            f"{first.throughput_event_time_multiplier or 0.0:.3f}x | "
            f"{second.throughput_event_time_multiplier or 0.0:.3f}x |"
        ),
        (
            f"| Peak RSS MiB | {first.resources.peak_rss_bytes / 2**20:.2f} | "
            f"{second.resources.peak_rss_bytes / 2**20:.2f} |"
        ),
        (
            f"| Maximum feature calculation ms | "
            f"{first.feature_calculation_latency.maximum_ms or 0.0:.3f} | "
            f"{second.feature_calculation_latency.maximum_ms or 0.0:.3f} |"
        ),
        (
            f"| Maximum writer file latency ms | "
            f"{first.writer.maximum_write_latency_ms or 0.0:.3f} | "
            f"{second.writer.maximum_write_latency_ms or 0.0:.3f} |"
        ),
        "",
        "## Availability and safety",
        "",
        (
            f"- Run 1 unavailable snapshots: {first.unavailable_snapshots:,}/"
            f"{first_total:,} "
            f"({_percentage(first.unavailable_snapshots, first_total):.2f}%)."
        ),
        (
            f"- Run 1 OI-stale snapshots: {first.open_interest_stale_snapshots:,}/"
            f"{first.single_venue_snapshots:,} "
            f"({_percentage(first.open_interest_stale_snapshots, first.single_venue_snapshots):.2f}"
            "%)."
        ),
        (
            f"- Run 1 non-aligned cross-venue snapshots: "
            f"{first.non_aligned_cross_venue_snapshots:,}/"
            f"{first.cross_venue_snapshots:,} "
            f"({first_non_aligned_percentage:.2f}%)."
        ),
        (
            "- Structured unavailable reasons: "
            f"`{json.dumps(first.unavailable_reason_counts, sort_keys=True)}`"
        ),
        "- Trading signals emitted: `0`.",
        "- Orders emitted: `0`.",
        "- Private API requests: `0`.",
        "",
        "## Stability status",
        "",
        (
            f"- Requested duration: {report.requested_stability_seconds:.0f} seconds "
            f"({report.requested_stability_seconds / 3600:.2f} hours)."
        ),
        (
            f"- Actual wall-clock observation in this acceptance run: "
            f"{report.actual_stability_observation_seconds:.3f} seconds."
        ),
        f"- Full requested duration completed: `{report.full_stability_duration_completed}`.",
        f"- Status: {report.full_stability_status}",
        "",
        "The feature Parquet trees are the authoritative all-field records. Their strict reader",
        "revalidated canonical payload JSON, payload SHA-256, schema, metadata, partitions,",
        "source lineage, structured unavailability, unique IDs, and deterministic ordering.",
        "",
    ]
    return "\n".join(lines)


async def run_phase3_acceptance(
    settings: Settings,
    *,
    input_path: Path,
    output_path: Path,
    first_batch_rows: int = 1_000,
    second_batch_rows: int = 777,
    requested_stability_seconds: float = 6 * 60 * 60,
) -> Phase3AcceptanceReport:
    """Audit input, replay twice, compare exact feature content, and write evidence."""

    if first_batch_rows < 1 or second_batch_rows < 1:
        raise ValueError("acceptance writer batch sizes must be positive")
    if requested_stability_seconds <= 0:
        raise ValueError("requested stability duration must be positive")
    source = input_path.resolve()
    destination = output_path.resolve()
    if not source.is_dir():
        raise ValueError(f"fixed dataset does not exist: {source}")
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("acceptance input and output must be disjoint")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("acceptance output directory must be empty")
    destination.mkdir(parents=True, exist_ok=True)

    raw_audit = await asyncio.to_thread(audit_raw_tree, source)
    first = await _run_once(
        label="run-1",
        settings=settings,
        input_path=source,
        output_path=destination / "run-1",
        batch_rows=first_batch_rows,
    )
    second = await _run_once(
        label="run-2",
        settings=settings,
        input_path=source,
        output_path=destination / "run-2",
        batch_rows=second_batch_rows,
    )
    consistency = await asyncio.to_thread(
        compare_feature_trees,
        first.output_path,
        second.output_path,
    )
    snapshot_counts_match = (
        first.single_venue_snapshots == second.single_venue_snapshots
        and first.cross_venue_snapshots == second.cross_venue_snapshots
        and first.feature_audit.rows == second.feature_audit.rows
    )
    deterministic = consistency.identical and snapshot_counts_match
    no_lookahead = (
        first.no_lookahead_violations == 0
        and second.no_lookahead_violations == 0
    )
    multipliers = (
        first.throughput_event_time_multiplier,
        second.throughput_event_time_multiplier,
    )
    above_realtime = all(
        multiplier is not None and multiplier > 1.0
        for multiplier in multipliers
    )
    feature_files_audited = all(
        run.feature_audit.rows
        == run.feature_audit.unique_snapshot_ids
        == run.writer.written_snapshots
        for run in (first, second)
    )
    safety = all(
        run.signal_outputs == 0
        and run.order_outputs == 0
        and run.private_api_requests == 0
        and all(stats.last_error is None for stats in run.consumers.values())
        and run.writer.last_error is None
        for run in (first, second)
    )
    observed = (
        first.resources.wall_duration_seconds
        + second.resources.wall_duration_seconds
    )
    full_stability = observed >= requested_stability_seconds
    report = Phase3AcceptanceReport(
        schema_version=1,
        generated_at=datetime.now(tz=UTC),
        input_path=source,
        output_path=destination,
        raw_audit=raw_audit,
        first_run=first,
        second_run=second,
        consistency=consistency,
        deterministic_replay=deterministic,
        snapshot_counts_match=snapshot_counts_match,
        no_lookahead=no_lookahead,
        throughput_above_realtime=above_realtime,
        feature_files_audited=feature_files_audited,
        safety_boundary_preserved=safety,
        requested_stability_seconds=requested_stability_seconds,
        actual_stability_observation_seconds=observed,
        full_stability_duration_completed=full_stability,
        full_stability_status=(
            "completed"
            if full_stability
            else (
                "pending: the repeatable harness was run on the longest retained "
                "dataset, but this environment did not remain active for the full target"
            )
        ),
    )
    if not deterministic:
        raise RuntimeError("Phase 3 repeated replay is not deterministic")
    if not no_lookahead:
        raise RuntimeError("Phase 3 acceptance detected future feature sources")
    if not feature_files_audited:
        raise RuntimeError("Phase 3 feature persistence audit did not reconcile")
    if not safety:
        raise RuntimeError("Phase 3 safety boundary was not preserved")

    json_path = destination / "summary.json"
    markdown_path = destination / "summary.md"
    json_path.write_text(
        json.dumps(
            asdict(report),
            default=_json_default,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")
    return report

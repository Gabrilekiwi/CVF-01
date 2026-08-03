"""Deterministic Phase 3 fixed-dataset acceptance and resource evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Final, Literal

from pydantic import TypeAdapter

from cvf import __version__
from cvf.config import Settings
from cvf.features import FeatureStatePipelineStats
from cvf.features.runtime import (
    FeatureRuntime,
    ReceiveTimeFeatureDriver,
    RuntimeLatency,
)
from cvf.monitoring.process import current_rss_bytes
from cvf.pipeline import ConsumerStats, NormalizedEventBus
from cvf.replay import (
    RawParquetReader,
    RawScanFilter,
    ReplayOrder,
    ReplayRunner,
    ReplaySourceMode,
    ReplaySummary,
    resolve_replay_source,
)
from cvf.storage.compact import RawAudit, audit_raw_tree
from cvf.storage.features import (
    FeatureAudit,
    FeatureConsistencyReport,
    FeatureWriterStats,
    compare_feature_audits,
)
from cvf.storage.raw import NORMALIZED_EVENT_JOURNAL_CHANNEL
from cvf.utils.fingerprint import settings_fingerprint


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
class SafetyBoundaryEvidence:
    """Runtime evidence for the deliberately feature-only acceptance graph."""

    execution_mode: Literal["OFFLINE_FIXED_DATASET_REPLAY"]
    input_transport: Literal["LOCAL_PARQUET"]
    component_types: tuple[str, ...]
    component_graph_matches_expected: bool
    output_event_type_counts: dict[str, int]
    forbidden_output_events_observed: int
    network_request_instrumentation_enabled: bool
    claim_scope: str


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
    safety_evidence: SafetyBoundaryEvidence
    replay_order: ReplayOrder
    replay_source_mode: ReplaySourceMode
    writer_batch_rows: int
    writer_flush_seconds: float
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
    requested_live_stability_seconds: float
    fixed_replay_observation_seconds: float
    live_stability_duration_completed: bool
    live_stability_status: str


@dataclass(frozen=True, slots=True)
class _Phase3RunCheckpoint:
    schema_version: int
    stage: Literal["REPLAY_COMPLETE", "AUDIT_COMPLETE"]
    package_source_sha256: str
    settings_sha256: str
    metrics: Phase3RunMetrics


_RUN_CHECKPOINT_ADAPTER: Final[TypeAdapter[_Phase3RunCheckpoint]] = TypeAdapter(
    _Phase3RunCheckpoint
)


def _latency_metrics(value: RuntimeLatency) -> LatencyMetrics:
    return LatencyMetrics(
        samples=value.samples,
        average_ms=value.average_ms,
        minimum_ms=value.minimum_ms,
        maximum_ms=value.maximum_ms,
    )


_EXPECTED_ACCEPTANCE_COMPONENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        "cvf.features.runtime.FeatureRuntime",
        "cvf.features.runtime.ReceiveTimeFeatureDriver",
        "cvf.features.pipeline.FeatureStatePipeline",
        "cvf.features.state.MarketStateStore",
        "cvf.pipeline.event_bus.NormalizedEventBus",
        "cvf.replay.raw_reader.RawParquetReader",
        "cvf.replay.runner.ReplayRunner",
        "cvf.storage.features.AsyncFeatureParquetWriter",
    }
)


def _qualified_type(value: object) -> str:
    value_type = type(value)
    return f"{value_type.__module__}.{value_type.__qualname__}"


def _safety_boundary_evidence(
    *,
    components: Iterable[object],
    output_event_type_counts: Mapping[str, int],
) -> SafetyBoundaryEvidence:
    component_types = tuple(sorted({_qualified_type(value) for value in components}))
    output_counts = dict(sorted(output_event_type_counts.items()))
    forbidden_outputs = sum(
        count
        for event_type, count in output_counts.items()
        if event_type != "MARKET_FEATURE"
    )
    return SafetyBoundaryEvidence(
        execution_mode="OFFLINE_FIXED_DATASET_REPLAY",
        input_transport="LOCAL_PARQUET",
        component_types=component_types,
        component_graph_matches_expected=(
            frozenset(component_types) == _EXPECTED_ACCEPTANCE_COMPONENT_TYPES
        ),
        output_event_type_counts=output_counts,
        forbidden_output_events_observed=forbidden_outputs,
        network_request_instrumentation_enabled=False,
        claim_scope=(
            "The recorded graph is local-Parquet, feature-only, and contains no "
            "exchange connector, signal, execution, order, or account component. "
            "Network requests are not instrumented, so this report does not present "
            "a fabricated private-request counter."
        ),
    )


async def _sample_rss(stop: asyncio.Event, samples: list[int]) -> None:
    while True:
        samples.append(current_rss_bytes())
        if stop.is_set():
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=0.25)
        except TimeoutError:
            continue


async def _run_once(
    *,
    label: str,
    settings: Settings,
    input_path: Path,
    output_path: Path,
    batch_rows: int,
    writer_flush_seconds: float,
    replay_order: ReplayOrder,
    replay_source_mode: ReplaySourceMode,
    expected_normalized_event_count: int | None,
    on_replay_complete: Callable[[Phase3RunMetrics], None] | None = None,
) -> Phase3RunMetrics:
    if output_path.exists() and any(output_path.iterdir()):
        raise ValueError(f"acceptance output must be empty: {output_path}")
    reader = RawParquetReader(input_path)
    order = replay_order
    if replay_source_mode is ReplaySourceMode.AUTO:
        raise ValueError("Phase 3 replay source mode must be resolved before a run")
    if replay_source_mode is ReplaySourceMode.JOURNAL:
        filters = RawScanFilter(
            channels=frozenset({NORMALIZED_EVENT_JOURNAL_CHANNEL})
        )
    else:
        filters = RawScanFilter(
            excluded_channels=frozenset({NORMALIZED_EVENT_JOURNAL_CHANNEL})
        )
    records = reader.iter_records(filters=filters, order=order)
    runtime = FeatureRuntime(
        settings,
        output_path=output_path,
        writer_batch_rows=batch_rows,
        writer_flush_seconds=writer_flush_seconds,
    )
    bus = NormalizedEventBus(
        default_queue_capacity=settings.pipeline.consumer_queue_capacity
    )
    bus.register(
        "feature-runtime",
        runtime.consume_event,
        queue_capacity=settings.pipeline.consumer_queue_capacity,
    )
    driver = ReceiveTimeFeatureDriver(
        settings,
        event_bus=bus,
        runtime=runtime,
    )
    runner = ReplayRunner(
        event_bus=bus,
        event_sink=driver.publish,
        finish_sink=lambda watermark: driver.finish(
            through_timestamp=watermark
        ),
        order=order,
        speed=0,
    )
    rss_samples: list[int] = [current_rss_bytes()]
    stop_sampling = asyncio.Event()
    sampler = asyncio.create_task(
        _sample_rss(stop_sampling, rss_samples),
        name=f"phase3-acceptance-rss-{label}",
    )
    wall_started = time.perf_counter()
    cpu_started = time.process_time()
    try:
        await runtime.start()
        try:
            replay = await runner.run(records)
            if replay.raw_records == 0 or replay.normalized_events == 0:
                raise RuntimeError(
                    "Phase 3 replay requires nonempty raw and normalized input"
                )
            if (
                replay_source_mode is ReplaySourceMode.JOURNAL
                and replay.feature_timeline_end_records != 1
            ):
                raise RuntimeError(
                    "Phase 3 journal replay requires exactly one clean "
                    "feature-timeline end marker"
                )
            if (
                expected_normalized_event_count is not None
                and replay.normalized_events != expected_normalized_event_count
            ):
                raise RuntimeError(
                    "Phase 3 journal replay count diverged from its "
                    "validated collection manifest"
                )
        finally:
            await runtime.close()
    finally:
        stop_sampling.set()
        await sampler
    wall_duration = time.perf_counter() - wall_started
    cpu_duration = time.process_time() - cpu_started
    rss_samples.append(current_rss_bytes())
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
    safety_evidence = _safety_boundary_evidence(
        components=(
            reader,
            runtime,
            runtime.state,
            runtime.feature_state,
            bus,
            runtime.writer,
            driver,
            runner,
        ),
        output_event_type_counts=runtime.stats.output_event_type_counts,
    )
    runtime_stats = runtime.stats
    replay_metrics = Phase3RunMetrics(
        label=label,
        input_path=input_path.resolve(),
        output_path=output_path.resolve(),
        replay=replay,
        feature_state=runtime_stats.feature_state,
        consumers=bus.stats,
        writer=runtime_stats.writer,
        feature_audit=FeatureAudit(
            rows=0,
            files=0,
            unique_snapshot_ids=0,
            partitions=0,
            content_digest="0" * 64,
            scopes=(),
            code_versions=(),
            config_hashes=(),
            unavailable_reason_counts={},
            unavailable_snapshots=0,
            earliest_decision_timestamp=None,
            latest_decision_timestamp=None,
        ),
        resources=resources,
        event_receive_latency=_latency_metrics(runtime_stats.receive_latency),
        feature_calculation_latency=_latency_metrics(
            runtime_stats.calculation_latency
        ),
        feature_enqueue_latency=_latency_metrics(runtime_stats.enqueue_latency),
        feature_ticks=runtime_stats.feature_ticks,
        signal_boundaries_observed=runtime_stats.reserved_signal_boundaries,
        single_venue_snapshots=runtime_stats.single_venue_snapshots,
        cross_venue_snapshots=runtime_stats.cross_venue_snapshots,
        unavailable_snapshots=runtime_stats.unavailable_snapshots,
        open_interest_stale_snapshots=(
            runtime_stats.open_interest_stale_snapshots
        ),
        non_aligned_cross_venue_snapshots=(
            runtime_stats.non_aligned_cross_venue_snapshots
        ),
        unavailable_reason_counts=runtime_stats.unavailable_reason_counts,
        book_generation_rebuilds=runtime_stats.book_generation_rebuilds,
        no_lookahead_violations=runtime_stats.no_lookahead_violations,
        safety_evidence=safety_evidence,
        replay_order=order,
        replay_source_mode=replay_source_mode,
        writer_batch_rows=batch_rows,
        writer_flush_seconds=writer_flush_seconds,
        throughput_records_per_second=throughput,
        throughput_event_time_multiplier=multiplier,
    )
    if on_replay_complete is not None:
        on_replay_complete(replay_metrics)
    feature_audit = await asyncio.to_thread(audit_raw_feature_tree, output_path)
    return replace(replay_metrics, feature_audit=feature_audit)


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


def _package_source_sha256() -> str:
    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted(package_root.rglob("*.py")):
        digest.update(path.relative_to(package_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _write_run_checkpoint(
    path: Path,
    *,
    stage: Literal["REPLAY_COMPLETE", "AUDIT_COMPLETE"],
    settings: Settings,
    package_source_sha256: str,
    metrics: Phase3RunMetrics,
) -> None:
    checkpoint = _Phase3RunCheckpoint(
        schema_version=3,
        stage=stage,
        package_source_sha256=package_source_sha256,
        settings_sha256=settings_fingerprint(settings),
        metrics=metrics,
    )
    temporary = path.with_name(f".{path.name}.tmp")
    try:
        temporary.write_bytes(_RUN_CHECKPOINT_ADAPTER.dump_json(checkpoint, indent=2) + b"\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


async def _load_run_checkpoint(
    path: Path,
    *,
    settings: Settings,
    package_source_sha256: str,
    expected_label: str,
    expected_input_path: Path,
    expected_output_path: Path,
    expected_batch_rows: int,
    expected_flush_seconds: float,
    expected_replay_order: ReplayOrder,
    expected_replay_source_mode: ReplaySourceMode,
) -> Phase3RunMetrics:
    checkpoint = _RUN_CHECKPOINT_ADAPTER.validate_json(path.read_bytes())
    if checkpoint.schema_version != 3:
        raise ValueError(f"unsupported Phase 3 checkpoint schema: {path}")
    if checkpoint.package_source_sha256 != package_source_sha256:
        raise ValueError(f"Phase 3 checkpoint code does not match current source: {path}")
    if checkpoint.settings_sha256 != settings_fingerprint(settings):
        raise ValueError(f"Phase 3 checkpoint settings do not match: {path}")
    metrics = checkpoint.metrics
    if (
        metrics.label != expected_label
        or metrics.input_path.resolve() != expected_input_path
        or metrics.output_path.resolve() != expected_output_path
        or metrics.writer_batch_rows != expected_batch_rows
        or metrics.writer_flush_seconds != expected_flush_seconds
        or metrics.replay_order is not expected_replay_order
        or metrics.replay_source_mode is not expected_replay_source_mode
    ):
        raise ValueError(f"Phase 3 checkpoint run parameters do not match: {path}")
    current_audit = await asyncio.to_thread(
        audit_raw_feature_tree,
        expected_output_path,
    )
    if checkpoint.stage == "REPLAY_COMPLETE":
        metrics = replace(metrics, feature_audit=current_audit)
        _write_run_checkpoint(
            path,
            stage="AUDIT_COMPLETE",
            settings=settings,
            package_source_sha256=package_source_sha256,
            metrics=metrics,
        )
    elif checkpoint.stage == "AUDIT_COMPLETE" and current_audit != metrics.feature_audit:
        raise ValueError(f"Phase 3 checkpoint feature tree changed: {path}")
    if metrics.feature_audit.code_versions != (__version__,):
        raise ValueError(f"Phase 3 checkpoint package version does not match: {path}")
    return metrics


async def _run_or_resume(
    *,
    label: str,
    settings: Settings,
    input_path: Path,
    destination: Path,
    batch_rows: int,
    writer_flush_seconds: float,
    replay_order: ReplayOrder,
    replay_source_mode: ReplaySourceMode,
    expected_normalized_event_count: int | None,
    resume: bool,
    package_source_sha256: str,
) -> Phase3RunMetrics:
    output_path = destination / label
    checkpoint_path = destination / f"{label}-metrics.json"
    if resume and checkpoint_path.is_file():
        checkpoint = await _load_run_checkpoint(
            checkpoint_path,
            settings=settings,
            package_source_sha256=package_source_sha256,
            expected_label=label,
            expected_input_path=input_path,
            expected_output_path=output_path.resolve(),
            expected_batch_rows=batch_rows,
            expected_flush_seconds=writer_flush_seconds,
            expected_replay_order=replay_order,
            expected_replay_source_mode=replay_source_mode,
        )
        if (
            expected_normalized_event_count is not None
            and checkpoint.replay.normalized_events
            != expected_normalized_event_count
        ):
            raise ValueError(
                "Phase 3 checkpoint journal count does not match "
                "the validated collection manifest"
            )
        return checkpoint
    if output_path.exists() and any(output_path.iterdir()):
        raise ValueError(
            f"partial Phase 3 output has no reusable checkpoint: {output_path}"
        )
    def save_replay_checkpoint(metrics: Phase3RunMetrics) -> None:
        _write_run_checkpoint(
            checkpoint_path,
            stage="REPLAY_COMPLETE",
            settings=settings,
            package_source_sha256=package_source_sha256,
            metrics=metrics,
        )

    metrics = await _run_once(
        label=label,
        settings=settings,
        input_path=input_path,
        output_path=output_path,
        batch_rows=batch_rows,
        writer_flush_seconds=writer_flush_seconds,
        replay_order=replay_order,
        replay_source_mode=replay_source_mode,
        expected_normalized_event_count=expected_normalized_event_count,
        on_replay_complete=save_replay_checkpoint,
    )
    _write_run_checkpoint(
        checkpoint_path,
        stage="AUDIT_COMPLETE",
        settings=settings,
        package_source_sha256=package_source_sha256,
        metrics=metrics,
    )
    return metrics


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
        f"# Phase 3 / v{__version__} Acceptance Evidence",
        "",
        f"- Generated: `{report.generated_at.astimezone(UTC).isoformat()}`",
        f"- Fixed input: `{report.input_path}`",
        f"- Replay order: `{first.replay_order.value}`",
        f"- Replay source mode: `{first.replay_source_mode.value}`",
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
        (
            f"| Offline writer flush seconds | {first.writer_flush_seconds:.3f} | "
            f"{second.writer_flush_seconds:.3f} |"
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
        (
            "- Observed persisted output event types: "
            f"`{json.dumps(first.safety_evidence.output_event_type_counts, sort_keys=True)}`."
        ),
        (
            "- Forbidden output events observed: "
            f"`{first.safety_evidence.forbidden_output_events_observed}`."
        ),
        (
            "- Feature-only runtime graph matched the fail-closed allowlist: "
            f"`{first.safety_evidence.component_graph_matches_expected}`."
        ),
        (
            "- Runtime component inventory: "
            f"`{json.dumps(first.safety_evidence.component_types)}`."
        ),
        (
            "- Safety evidence scope: "
            f"{first.safety_evidence.claim_scope}"
        ),
        "",
        "## Stability status",
        "",
        (
            "- Requested continuous-live stability duration: "
            f"{report.requested_live_stability_seconds:.0f} seconds "
            f"({report.requested_live_stability_seconds / 3600:.2f} hours)."
        ),
        (
            "- Actual fixed-replay process observation in this acceptance run: "
            f"{report.fixed_replay_observation_seconds:.3f} seconds."
        ),
        (
            "- Continuous-live stability duration completed: "
            f"`{report.live_stability_duration_completed}`."
        ),
        f"- Status: {report.live_stability_status}",
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
    writer_flush_seconds: float = 60,
    replay_order: ReplayOrder = ReplayOrder.RECEIVE_TIME,
    replay_source_mode: ReplaySourceMode = ReplaySourceMode.AUTO,
    requested_live_stability_seconds: float = 6 * 60 * 60,
    resume: bool = False,
) -> Phase3AcceptanceReport:
    """Audit input, replay twice, compare exact feature content, and write evidence."""

    if first_batch_rows < 1 or second_batch_rows < 1:
        raise ValueError("acceptance writer batch sizes must be positive")
    if writer_flush_seconds <= 0:
        raise ValueError("acceptance writer flush interval must be positive")
    if requested_live_stability_seconds <= 0:
        raise ValueError("requested live stability duration must be positive")
    if replay_order is not ReplayOrder.RECEIVE_TIME:
        raise ValueError(
            "Phase 3 acceptance requires receive-time order to match live boundaries"
        )
    source = input_path.resolve()
    destination = output_path.resolve()
    if not source.is_dir():
        raise ValueError(f"fixed dataset does not exist: {source}")
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("acceptance input and output must be disjoint")
    if not resume and destination.exists() and any(destination.iterdir()):
        raise ValueError("acceptance output directory must be empty")
    source_resolution = await asyncio.to_thread(
        resolve_replay_source,
        source,
        replay_source_mode,
    )
    resolved_source_mode = source_resolution.mode
    clean_collection = source_resolution.clean_collection
    raw_audit = (
        await asyncio.to_thread(audit_raw_tree, source)
        if clean_collection is None
        else clean_collection.raw_audit
    )
    expected_normalized_event_count = (
        None
        if clean_collection is None
        else clean_collection.normalized_event_count
    )
    package_source_sha256 = _package_source_sha256()
    if clean_collection is not None:
        capture_manifest = clean_collection.manifest
        expected_settings_sha256 = settings_fingerprint(settings)
        lineage_mismatches = [
            label
            for label, matches in (
                ("code_version", capture_manifest.code_version == __version__),
                (
                    "code_sha256",
                    capture_manifest.code_sha256 == package_source_sha256,
                ),
                (
                    "strategy_version",
                    capture_manifest.strategy_version
                    == settings.app.strategy_version,
                ),
                (
                    "settings_sha256",
                    capture_manifest.settings_sha256
                    == expected_settings_sha256,
                ),
            )
            if not matches
        ]
        if lineage_mismatches:
            raise RuntimeError(
                "journal capture lineage differs from the acceptance runtime: "
                + ", ".join(lineage_mismatches)
            )
    destination.mkdir(parents=True, exist_ok=True)
    first = await _run_or_resume(
        label="run-1",
        settings=settings,
        input_path=source,
        destination=destination,
        batch_rows=first_batch_rows,
        writer_flush_seconds=writer_flush_seconds,
        replay_order=replay_order,
        replay_source_mode=resolved_source_mode,
        expected_normalized_event_count=expected_normalized_event_count,
        resume=resume,
        package_source_sha256=package_source_sha256,
    )
    second = await _run_or_resume(
        label="run-2",
        settings=settings,
        input_path=source,
        destination=destination,
        batch_rows=second_batch_rows,
        writer_flush_seconds=writer_flush_seconds,
        replay_order=replay_order,
        replay_source_mode=resolved_source_mode,
        expected_normalized_event_count=expected_normalized_event_count,
        resume=resume,
        package_source_sha256=package_source_sha256,
    )
    consistency = compare_feature_audits(
        first.output_path,
        second.output_path,
        left=first.feature_audit,
        right=second.feature_audit,
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
        run.safety_evidence.component_graph_matches_expected
        and run.safety_evidence.forbidden_output_events_observed == 0
        and all(stats.last_error is None for stats in run.consumers.values())
        and run.writer.last_error is None
        for run in (first, second)
    )
    observed = (
        first.resources.wall_duration_seconds
        + second.resources.wall_duration_seconds
    )
    report = Phase3AcceptanceReport(
        schema_version=3,
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
        requested_live_stability_seconds=requested_live_stability_seconds,
        fixed_replay_observation_seconds=observed,
        live_stability_duration_completed=False,
        live_stability_status=(
            "pending: a continuous live public-feed soak with reconnect and "
            "resynchronization opportunities has not been executed"
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

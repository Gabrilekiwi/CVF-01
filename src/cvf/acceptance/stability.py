"""Repeat fixed-dataset acceptance for an honest wall-clock stability target."""

from __future__ import annotations

import json
import os
import shutil
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path

from cvf.acceptance.phase3 import Phase3AcceptanceReport, run_phase3_acceptance
from cvf.config import Settings


@dataclass(frozen=True, slots=True)
class Phase3StabilityReport:
    schema_version: int
    generated_at: datetime
    input_path: Path
    output_path: Path
    fixed_replay_target_seconds: float
    fixed_replay_actual_wall_seconds: float
    fixed_replay_target_reached: bool
    continuous_live_soak_completed: bool
    status: str
    retain_feature_trees: bool
    iterations: tuple[Phase3AcceptanceReport, ...]
    all_deterministic: bool
    all_no_lookahead: bool
    all_feature_files_audited: bool
    all_safety_boundaries_preserved: bool
    total_raw_records: int
    total_normalized_events: int
    total_feature_snapshots: int
    total_replay_skipped_records: int
    total_feature_state_rejections: int
    total_book_generation_rebuilds: int
    peak_rss_bytes: int
    maximum_consumer_queue_depth: int
    maximum_event_processing_latency_ms: float | None
    maximum_feature_calculation_latency_ms: float | None
    maximum_writer_latency_ms: float | None


def _json_default(value: object) -> object:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _maximum_optional(values: list[float | None]) -> float | None:
    available = [value for value in values if value is not None]
    return None if not available else max(available)


def _native_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name == "nt" and not resolved.startswith("\\\\?\\"):
        return f"\\\\?\\{resolved}"
    return resolved


def _remove_feature_trees(
    report: Phase3AcceptanceReport,
    *,
    stability_root: Path,
) -> None:
    for run in (report.first_run, report.second_run):
        target = run.output_path.resolve()
        if stability_root not in target.parents:
            raise RuntimeError(f"refusing to remove output outside stability root: {target}")
        if target.is_dir():
            shutil.rmtree(_native_path(target))


def _render_markdown(report: Phase3StabilityReport) -> str:
    lines = [
        "# Phase 3 stability evidence",
        "",
        f"- Generated: `{report.generated_at.astimezone(UTC).isoformat()}`",
        f"- Fixed input: `{report.input_path}`",
        (
            "- Fixed-replay stress target: "
            f"`{report.fixed_replay_target_seconds:.3f}` seconds"
        ),
        (
            "- Fixed-replay actual wall time: "
            f"`{report.fixed_replay_actual_wall_seconds:.3f}` seconds"
        ),
        (
            "- Fixed-replay target reached: "
            f"`{report.fixed_replay_target_reached}`"
        ),
        (
            "- Continuous live-feed soak completed: "
            f"`{report.continuous_live_soak_completed}`"
        ),
        f"- Status: {report.status}",
        f"- Iterations: `{len(report.iterations)}`",
        f"- All deterministic: `{report.all_deterministic}`",
        f"- All no-lookahead: `{report.all_no_lookahead}`",
        f"- All feature audits passed: `{report.all_feature_files_audited}`",
        f"- All safety boundaries preserved: `{report.all_safety_boundaries_preserved}`",
        "",
        "## Aggregates",
        "",
        f"- Raw records across both runs: `{report.total_raw_records:,}`",
        f"- Normalized events across both runs: `{report.total_normalized_events:,}`",
        f"- Feature snapshots across both runs: `{report.total_feature_snapshots:,}`",
        f"- Replay-skipped records: `{report.total_replay_skipped_records:,}`",
        f"- Feature-state rejections: `{report.total_feature_state_rejections:,}`",
        f"- Book generation rebuilds: `{report.total_book_generation_rebuilds:,}`",
        f"- Peak RSS: `{report.peak_rss_bytes / 2**20:.2f}` MiB",
        f"- Maximum consumer queue depth: `{report.maximum_consumer_queue_depth:,}`",
        (
            "- Maximum event processing latency: "
            f"`{report.maximum_event_processing_latency_ms or 0.0:.3f}` ms"
        ),
        (
            "- Maximum feature calculation latency: "
            f"`{report.maximum_feature_calculation_latency_ms or 0.0:.3f}` ms"
        ),
        (
            "- Maximum writer latency: "
            f"`{report.maximum_writer_latency_ms or 0.0:.3f}` ms"
        ),
        "",
        "Each iteration replays the same audited public dataset twice with different",
        "writer batch sizes and compares exact logical feature content. This detects",
        "process-lifetime resource drift, but it does not substitute for a continuous",
        "six-hour live public-feed collection with real reconnect opportunities.",
        "",
    ]
    return "\n".join(lines)


async def run_phase3_stability(
    settings: Settings,
    *,
    input_path: Path,
    output_path: Path,
    target_seconds: float = 6 * 60 * 60,
    maximum_iterations: int | None = None,
    retain_feature_trees: bool = False,
) -> Phase3StabilityReport:
    """Repeat fixed-data acceptance as stress evidence, never as a live-soak substitute."""

    if target_seconds <= 0:
        raise ValueError("stability target must be positive")
    if maximum_iterations is not None and maximum_iterations < 1:
        raise ValueError("maximum_iterations must be positive")
    source = input_path.resolve()
    destination = output_path.resolve()
    if not source.is_dir():
        raise ValueError(f"fixed dataset does not exist: {source}")
    if source == destination or source in destination.parents or destination in source.parents:
        raise ValueError("stability input and output must be disjoint")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("stability output directory must be empty")
    destination.mkdir(parents=True, exist_ok=True)

    started = time.perf_counter()
    iterations: list[Phase3AcceptanceReport] = []
    while not iterations or time.perf_counter() - started < target_seconds:
        if maximum_iterations is not None and len(iterations) >= maximum_iterations:
            break
        iteration_number = len(iterations) + 1
        acceptance_report = await run_phase3_acceptance(
            settings,
            input_path=source,
            output_path=destination / "iterations" / f"{iteration_number:04d}",
            requested_live_stability_seconds=target_seconds,
        )
        iterations.append(acceptance_report)
        if not retain_feature_trees:
            _remove_feature_trees(acceptance_report, stability_root=destination)

    actual_wall_seconds = time.perf_counter() - started
    runs = [
        run
        for iteration in iterations
        for run in (iteration.first_run, iteration.second_run)
    ]
    fixed_replay_target_reached = actual_wall_seconds >= target_seconds
    stability_report = Phase3StabilityReport(
        schema_version=2,
        generated_at=datetime.now(tz=UTC),
        input_path=source,
        output_path=destination,
        fixed_replay_target_seconds=target_seconds,
        fixed_replay_actual_wall_seconds=actual_wall_seconds,
        fixed_replay_target_reached=fixed_replay_target_reached,
        continuous_live_soak_completed=False,
        status=(
            (
                "fixed-replay stress target reached; continuous live-feed soak "
                "with reconnect/resynchronization remains pending"
            )
            if fixed_replay_target_reached
            else (
                "fixed-replay stress stopped at the configured iteration cap; "
                "continuous live-feed soak remains pending"
            )
        ),
        retain_feature_trees=retain_feature_trees,
        iterations=tuple(iterations),
        all_deterministic=all(item.deterministic_replay for item in iterations),
        all_no_lookahead=all(item.no_lookahead for item in iterations),
        all_feature_files_audited=all(
            item.feature_files_audited for item in iterations
        ),
        all_safety_boundaries_preserved=all(
            item.safety_boundary_preserved for item in iterations
        ),
        total_raw_records=sum(run.replay.raw_records for run in runs),
        total_normalized_events=sum(run.replay.normalized_events for run in runs),
        total_feature_snapshots=sum(run.feature_audit.rows for run in runs),
        total_replay_skipped_records=sum(run.replay.skipped_records for run in runs),
        total_feature_state_rejections=sum(
            run.feature_state.rejected_events for run in runs
        ),
        total_book_generation_rebuilds=sum(
            run.book_generation_rebuilds for run in runs
        ),
        peak_rss_bytes=max(run.resources.peak_rss_bytes for run in runs),
        maximum_consumer_queue_depth=max(
            stats.maximum_queue_depth
            for run in runs
            for stats in run.consumers.values()
        ),
        maximum_event_processing_latency_ms=_maximum_optional(
            [
                stats.maximum_processing_latency_ms
                for run in runs
                for stats in run.consumers.values()
            ]
        ),
        maximum_feature_calculation_latency_ms=_maximum_optional(
            [run.feature_calculation_latency.maximum_ms for run in runs]
        ),
        maximum_writer_latency_ms=_maximum_optional(
            [run.writer.maximum_write_latency_ms for run in runs]
        ),
    )
    (destination / "stability-summary.json").write_text(
        json.dumps(
            asdict(stability_report),
            default=_json_default,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (destination / "stability-summary.md").write_text(
        _render_markdown(stability_report),
        encoding="utf-8",
    )
    return stability_report

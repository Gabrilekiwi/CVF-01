"""Fail-closed replay source selection for legacy raw and clean journals."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from cvf.replay.raw_reader import ReplaySourceMode
from cvf.storage.collection_manifest import (
    CleanCollectionValidation,
    inspect_collection_evidence,
    validate_clean_collection,
)


@dataclass(frozen=True, slots=True)
class ReplaySourceResolution:
    """Resolved mode plus any validated clean-run evidence."""

    mode: ReplaySourceMode
    clean_collection: CleanCollectionValidation | None


@dataclass(frozen=True, slots=True)
class ReplayRuntimeLineage:
    """Current runtime identity required to reproduce a journal capture."""

    code_version: str
    code_sha256: str
    strategy_version: str
    settings_sha256: str


def validate_replay_capture_lineage(
    resolution: ReplaySourceResolution,
    runtime: ReplayRuntimeLineage,
) -> None:
    """Reject a clean journal captured by a different runtime lineage."""

    if resolution.clean_collection is None:
        return
    capture = resolution.clean_collection.manifest
    mismatches = [
        label
        for label, matches in (
            ("code_version", capture.code_version == runtime.code_version),
            ("code_sha256", capture.code_sha256 == runtime.code_sha256),
            (
                "strategy_version",
                capture.strategy_version == runtime.strategy_version,
            ),
            (
                "settings_sha256",
                capture.settings_sha256 == runtime.settings_sha256,
            ),
        )
        if not matches
    ]
    if mismatches:
        raise RuntimeError(
            "journal capture lineage differs from the replay runtime: "
            + ", ".join(mismatches)
        )


def resolve_replay_source(
    root: Path,
    requested_mode: ReplaySourceMode,
) -> ReplaySourceResolution:
    """Resolve AUTO and enforce the manifest contract before replay output starts.

    Explicit RAW is the recovery path and deliberately excludes any journal rows.
    AUTO remains backward-compatible only for trees with neither journal nor
    manifest evidence. Any such evidence opts the tree into strict CLEAN_END
    validation.
    """

    evidence = inspect_collection_evidence(root)
    if requested_mode is ReplaySourceMode.RAW:
        return ReplaySourceResolution(
            mode=ReplaySourceMode.RAW,
            clean_collection=None,
        )
    if requested_mode is ReplaySourceMode.AUTO and not evidence.any:
        return ReplaySourceResolution(
            mode=ReplaySourceMode.RAW,
            clean_collection=None,
        )
    clean_collection = validate_clean_collection(root)
    return ReplaySourceResolution(
        mode=ReplaySourceMode.JOURNAL,
        clean_collection=clean_collection,
    )

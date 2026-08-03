"""Atomic clean-run manifests for raw collection trees.

The manifest proves that one collection run reached an audited clean shutdown.
It deliberately does not claim a per-raw-record normalization outcome ledger or
the durability guarantees of a crash-safe write-ahead log.
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal
from urllib.parse import quote
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator

from cvf.models.common import FrozenModel
from cvf.utils.fingerprint import model_payload_json

if TYPE_CHECKING:
    from cvf.storage.compact import RawAudit


COLLECTION_MANIFEST_FILENAME: Final = "_collection_manifest.json"
COMPACTION_IN_PROGRESS_FILENAME: Final = "_compaction_in_progress"
COLLECTION_MANIFEST_SCHEMA_VERSION: Final[Literal[1]] = 1
COLLECTION_MANIFEST_CLAIM_SCOPE: Final = (
    "Audited clean-run completeness after producer join and writer closure; "
    "not a per-raw 0/1/N normalization outcome ledger or crash-safe WAL."
)
_MANIFEST_TEMP_GLOB: Final = f".{COLLECTION_MANIFEST_FILENAME}.*.tmp"


class CollectionTerminal(StrEnum):
    """Lifecycle state persisted by a collection run."""

    IN_PROGRESS = "IN_PROGRESS"
    CLEAN_END = "CLEAN_END"


class RawLogicalAudit(FrozenModel):
    """Raw audit fields that remain invariant across lossless compaction."""

    rows: int = Field(ge=0)
    unique_record_ids: int = Field(ge=0)
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_bytes: int = Field(ge=0)
    partitions: int = Field(ge=0)

    @classmethod
    def from_raw_audit(cls, audit: RawAudit) -> RawLogicalAudit:
        return cls(
            rows=audit.rows,
            unique_record_ids=audit.unique_record_ids,
            content_digest=audit.content_digest,
            payload_bytes=audit.payload_bytes,
            partitions=audit.partitions,
        )


class CollectionManifest(FrozenModel):
    """One atomically replaced collection lifecycle record."""

    schema_version: Literal[1] = COLLECTION_MANIFEST_SCHEMA_VERSION
    run_id: UUID
    started_at: datetime
    terminal: CollectionTerminal
    terminal_at: datetime | None = None
    feature_timeline_end_at: datetime | None = None
    code_version: str
    code_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    strategy_version: str
    settings_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    normalized_event_count: int | None = Field(default=None, ge=0)
    raw_audit: RawLogicalAudit | None = None
    claim_scope: Literal[
        "Audited clean-run completeness after producer join and writer closure; "
        "not a per-raw 0/1/N normalization outcome ledger or crash-safe WAL."
    ] = COLLECTION_MANIFEST_CLAIM_SCOPE

    @field_validator("started_at", "terminal_at", "feature_timeline_end_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("collection manifest timestamps must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("code_version", "strategy_version")
    @classmethod
    def versions_are_not_empty(cls, value: str) -> str:
        if not value or value.isspace():
            raise ValueError("collection manifest versions cannot be empty")
        return value

    @model_validator(mode="after")
    def lifecycle_fields_match_terminal(self) -> CollectionManifest:
        terminal_fields = (
            self.terminal_at,
            self.feature_timeline_end_at,
            self.normalized_event_count,
            self.raw_audit,
        )
        if self.terminal is CollectionTerminal.IN_PROGRESS:
            if any(value is not None for value in terminal_fields):
                raise ValueError(
                    "IN_PROGRESS collection manifest cannot contain terminal evidence"
                )
            return self
        if any(value is None for value in terminal_fields):
            raise ValueError(
                "CLEAN_END collection manifest requires complete terminal evidence"
            )
        assert self.terminal_at is not None
        assert self.feature_timeline_end_at is not None
        if self.terminal_at < self.started_at:
            raise ValueError("collection terminal precedes collection start")
        if self.feature_timeline_end_at < self.started_at:
            raise ValueError("feature timeline end precedes collection start")
        if self.feature_timeline_end_at > self.terminal_at:
            raise ValueError("feature timeline end follows collection terminal")
        return self


@dataclass(frozen=True, slots=True)
class CollectionEvidence:
    """Filesystem evidence that opts a tree into the strict journal contract."""

    journal: bool
    manifest: bool
    incomplete: bool = False

    @property
    def any(self) -> bool:
        return self.journal or self.manifest or self.incomplete


@dataclass(frozen=True, slots=True)
class CleanCollectionValidation:
    """Validated clean-run evidence used by replay and compaction."""

    manifest: CollectionManifest
    raw_audit: RawAudit
    normalized_event_count: int
    terminal_marker_count: int


@dataclass(frozen=True, slots=True)
class NormalizedJournalAudit:
    """Validated journal cardinality and terminal watermark."""

    normalized_event_count: int
    terminal_marker_count: int
    terminal_marker_at: datetime


class CollectionManifestError(ValueError):
    """Raised when collection completion evidence is absent or inconsistent."""


def collection_manifest_path(root: Path) -> Path:
    return root.resolve() / COLLECTION_MANIFEST_FILENAME


def compaction_in_progress_path(root: Path) -> Path:
    return root.resolve() / COMPACTION_IN_PROGRESS_FILENAME


def inspect_collection_evidence(root: Path) -> CollectionEvidence:
    """Detect journal/manifest evidence, including an interrupted temp manifest."""

    resolved = root.resolve()
    if not resolved.exists():
        return CollectionEvidence(
            journal=False,
            manifest=False,
            incomplete=False,
        )
    partition_name = (
        f"channel={quote('_normalized_event', safe='-_.')}"
    )
    journal = any(
        candidate.is_dir()
        for candidate in resolved.rglob(partition_name)
    )
    manifest = (
        (resolved / COLLECTION_MANIFEST_FILENAME).exists()
        or any(resolved.glob(_MANIFEST_TEMP_GLOB))
    )
    return CollectionEvidence(
        journal=journal,
        manifest=manifest,
        incomplete=compaction_in_progress_path(resolved).exists(),
    )


def package_source_sha256() -> str:
    """Hash the installed ``cvf`` Python source independently of file metadata."""

    package_root = Path(__file__).resolve().parents[1]
    digest = hashlib.sha256()
    for path in sorted(package_root.rglob("*.py")):
        digest.update(path.relative_to(package_root).as_posix().encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _atomic_write_manifest(root: Path, manifest: CollectionManifest) -> None:
    resolved = root.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    destination = resolved / COLLECTION_MANIFEST_FILENAME
    temporary = resolved / (
        f".{COLLECTION_MANIFEST_FILENAME}.{uuid4().hex}.tmp"
    )
    payload = f"{model_payload_json(manifest)}\n".encode()
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _claim_collection_manifest(root: Path, manifest: CollectionManifest) -> None:
    """Create the authoritative initial manifest with an exclusive claim."""

    resolved = root.resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    if any(resolved.iterdir()):
        raise CollectionManifestError(
            f"collection output is not empty: {resolved}"
        )
    destination = resolved / COLLECTION_MANIFEST_FILENAME
    payload = f"{model_payload_json(manifest)}\n".encode()
    try:
        with destination.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise CollectionManifestError(
            f"collection manifest already exists: {destination}"
        ) from exc


def begin_collection_manifest(
    root: Path,
    *,
    started_at: datetime,
    code_version: str,
    strategy_version: str,
    settings_sha256: str,
    code_sha256: str | None = None,
    run_id: UUID | None = None,
) -> CollectionManifest:
    """Atomically establish ``IN_PROGRESS`` before any producer is started."""

    resolved = root.resolve()
    manifest = CollectionManifest(
        run_id=run_id or uuid4(),
        started_at=started_at,
        terminal=CollectionTerminal.IN_PROGRESS,
        code_version=code_version,
        code_sha256=code_sha256 or package_source_sha256(),
        strategy_version=strategy_version,
        settings_sha256=settings_sha256,
    )
    _claim_collection_manifest(resolved, manifest)
    return manifest


def read_collection_manifest(root: Path) -> CollectionManifest:
    """Read and strictly validate the sole authoritative manifest."""

    sentinel = compaction_in_progress_path(root)
    if sentinel.exists():
        raise CollectionManifestError(
            f"raw compaction did not reach audited completion: {sentinel}"
        )
    path = collection_manifest_path(root)
    if not path.is_file():
        raise CollectionManifestError(f"collection manifest is missing: {path}")
    try:
        return CollectionManifest.model_validate_json(path.read_bytes())
    except Exception as exc:
        raise CollectionManifestError(
            f"collection manifest is corrupt: {path}"
        ) from exc


def complete_collection_manifest(
    root: Path,
    *,
    expected_run_id: UUID,
    terminal_at: datetime,
    feature_timeline_end_at: datetime,
    normalized_event_count: int,
    raw_audit: RawAudit,
) -> CollectionManifest:
    """Atomically replace ``IN_PROGRESS`` with audited ``CLEAN_END``."""

    current = read_collection_manifest(root)
    if current.run_id != expected_run_id:
        raise CollectionManifestError("collection manifest run_id changed during capture")
    if current.terminal is not CollectionTerminal.IN_PROGRESS:
        raise CollectionManifestError(
            "collection manifest is not in the IN_PROGRESS state"
        )
    clean = current.model_copy(
        update={
            "terminal": CollectionTerminal.CLEAN_END,
            "terminal_at": terminal_at,
            "feature_timeline_end_at": feature_timeline_end_at,
            "normalized_event_count": normalized_event_count,
            "raw_audit": RawLogicalAudit.from_raw_audit(raw_audit),
        }
    )
    clean = CollectionManifest.model_validate(clean.model_dump(mode="json"))
    _atomic_write_manifest(root, clean)
    return clean


def preserve_clean_collection_manifest(
    root: Path,
    *,
    manifest: CollectionManifest,
    raw_audit: RawAudit,
) -> None:
    """Write a source clean manifest only when compacted logical audit still matches."""

    if manifest.terminal is not CollectionTerminal.CLEAN_END:
        raise CollectionManifestError("only a CLEAN_END manifest can be preserved")
    logical_audit = RawLogicalAudit.from_raw_audit(raw_audit)
    if manifest.raw_audit != logical_audit:
        raise CollectionManifestError(
            "compacted raw logical audit does not match the clean manifest"
        )
    _atomic_write_manifest(root, manifest)


def audit_normalized_journal(
    root: Path,
    *,
    require_normalized_events: bool = True,
) -> NormalizedJournalAudit:
    """Validate every journal row and require exactly one terminal marker."""

    from cvf.replay.normalizer import RawRecordNormalizer
    from cvf.replay.ordering import ReplayOrder
    from cvf.replay.raw_reader import RawParquetReader, RawScanFilter
    from cvf.storage.raw import (
        FEATURE_TIMELINE_END_MESSAGE_KIND,
        NORMALIZED_EVENT_JOURNAL_CHANNEL,
        feature_timeline_end_timestamp,
    )

    resolved = root.resolve()
    reader = RawParquetReader(resolved)
    if not reader.has_channel(NORMALIZED_EVENT_JOURNAL_CHANNEL):
        raise CollectionManifestError(
            "collection manifest exists but normalized journal is missing"
        )
    records = reader.iter_records(
        filters=RawScanFilter(
            channels=frozenset({NORMALIZED_EVENT_JOURNAL_CHANNEL})
        ),
        order=ReplayOrder.RECEIVE_TIME,
    )
    normalizer = RawRecordNormalizer()
    normalized_event_count = 0
    terminal_marker_count = 0
    terminal_marker_at: datetime | None = None
    latest_event_receive_at: datetime | None = None
    try:
        for record in records:
            if record.message_kind == FEATURE_TIMELINE_END_MESSAGE_KIND:
                terminal_marker_count += 1
                if terminal_marker_count > 1:
                    raise CollectionManifestError(
                        "normalized journal requires exactly one terminal marker"
                    )
                terminal_marker_at = feature_timeline_end_timestamp(record)
                continue
            if terminal_marker_count:
                raise CollectionManifestError(
                    "normalized journal contains a record after its terminal marker"
                )
            try:
                events = normalizer.normalize(record)
            except Exception as exc:
                raise CollectionManifestError(
                    "normalized journal contains an invalid event record"
                ) from exc
            if len(events) != 1:
                raise CollectionManifestError(
                    "each normalized journal row must contain exactly one event"
                )
            event_receive_at = events[0].local_receive_timestamp.astimezone(UTC)
            latest_event_receive_at = (
                event_receive_at
                if latest_event_receive_at is None
                else max(latest_event_receive_at, event_receive_at)
            )
            normalized_event_count += 1
    finally:
        records.close()

    if require_normalized_events and normalized_event_count == 0:
        raise CollectionManifestError("normalized journal is empty")
    if terminal_marker_count != 1 or terminal_marker_at is None:
        raise CollectionManifestError(
            "normalized journal requires exactly one terminal marker"
        )
    if (
        latest_event_receive_at is not None
        and latest_event_receive_at > terminal_marker_at
    ):
        raise CollectionManifestError(
            "normalized journal event exceeds its terminal watermark"
        )
    return NormalizedJournalAudit(
        normalized_event_count=normalized_event_count,
        terminal_marker_count=terminal_marker_count,
        terminal_marker_at=terminal_marker_at,
    )


def validate_clean_collection(
    root: Path,
    *,
    raw_audit: RawAudit | None = None,
) -> CleanCollectionValidation:
    """Validate manifest, raw logical audit, exact journal count, and one end marker."""

    from cvf.storage.compact import audit_raw_tree

    resolved = root.resolve()
    unfinished = sorted(resolved.rglob("*.tmp"))
    if unfinished:
        raise CollectionManifestError(
            f"collection contains unfinished temporary files: {unfinished[0]}"
        )
    manifest = read_collection_manifest(resolved)
    if manifest.terminal is not CollectionTerminal.CLEAN_END:
        raise CollectionManifestError(
            "collection manifest has no CLEAN_END terminal"
        )
    audited = audit_raw_tree(resolved) if raw_audit is None else raw_audit
    logical_audit = RawLogicalAudit.from_raw_audit(audited)
    if manifest.raw_audit != logical_audit:
        raise CollectionManifestError(
            "collection manifest raw logical audit mismatch"
        )
    journal_audit = audit_normalized_journal(resolved)
    if (
        manifest.normalized_event_count
        != journal_audit.normalized_event_count
    ):
        raise CollectionManifestError(
            "collection manifest normalized journal count mismatch"
        )
    if manifest.feature_timeline_end_at != journal_audit.terminal_marker_at:
        raise CollectionManifestError(
            "collection manifest terminal marker timestamp mismatch"
        )
    return CleanCollectionValidation(
        manifest=manifest,
        raw_audit=audited,
        normalized_event_count=journal_audit.normalized_event_count,
        terminal_marker_count=journal_audit.terminal_marker_count,
    )

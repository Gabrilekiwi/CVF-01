"""Clean-run collection manifests and fail-closed replay source selection."""

from __future__ import annotations

import json
import os
import shutil
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

import cvf.storage.compact as raw_compaction
from cvf import __version__
from cvf.config import Settings, load_settings
from cvf.main import run_replay
from cvf.models import AggressorSide, Exchange, Trade
from cvf.replay import (
    RawParquetReader,
    ReplayOrder,
    ReplaySourceMode,
    resolve_replay_source,
)
from cvf.storage import (
    COLLECTION_MANIFEST_FILENAME,
    COMPACTION_IN_PROGRESS_FILENAME,
    AsyncPartitionedParquetWriter,
    CollectionManifest,
    CollectionManifestError,
    CollectionTerminal,
    RawMarketRecord,
    audit_raw_tree,
    begin_collection_manifest,
    compact_raw_tree,
    complete_collection_manifest,
    read_collection_manifest,
    validate_clean_collection,
)
from cvf.storage.raw import (
    feature_timeline_end_journal_record,
    normalized_event_journal_record,
)
from cvf.utils.fingerprint import settings_fingerprint

NOW = datetime(2026, 7, 30, 12, 0, tzinfo=UTC)


@pytest.fixture
def scratch_root() -> Iterator[Path]:
    path = Path("data/processed") / f"manifest-{uuid4().hex[:8]}"
    path.mkdir()
    try:
        yield path
    finally:
        resolved = str(path.resolve())
        native_path = (
            f"\\\\?\\{resolved}"
            if os.name == "nt" and not resolved.startswith("\\\\?\\")
            else resolved
        )
        shutil.rmtree(native_path)


def _source_record(sequence: int = 1) -> RawMarketRecord:
    return RawMarketRecord(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        channel="aggTrade",
        message_kind="market_data",
        transport="websocket",
        exchange_timestamp=NOW,
        local_receive_timestamp=NOW,
        normalization_timestamp=NOW,
        sequence_id=sequence,
        connection_generation=1,
        raw_payload=b'{"source":true}',
    )


def _trade(source: RawMarketRecord) -> Trade:
    return Trade(
        exchange=source.exchange,
        symbol=source.symbol,
        exchange_timestamp=NOW,
        local_receive_timestamp=NOW,
        normalization_timestamp=NOW,
        sequence_id=source.sequence_id,
        raw_payload_reference=source.raw_payload_reference,
        trade_id=str(source.sequence_id),
        price=Decimal("100"),
        quantity=Decimal("1"),
        aggressor_side=AggressorSide.BUY,
    )


def _begin(root: Path, settings: Settings) -> CollectionManifest:
    return begin_collection_manifest(
        root,
        started_at=NOW - timedelta(seconds=1),
        code_version=__version__,
        strategy_version=settings.app.strategy_version,
        settings_sha256=settings_fingerprint(settings),
    )


def test_clean_manifest_rejects_feature_timeline_before_collection_start(
    scratch_root: Path,
) -> None:
    root = scratch_root / "timeline-before-start"
    root.mkdir()
    manifest = _begin(root, load_settings(environ={}))

    with pytest.raises(ValueError, match="timeline end precedes collection start"):
        complete_collection_manifest(
            root,
            expected_run_id=manifest.run_id,
            terminal_at=NOW,
            feature_timeline_end_at=NOW - timedelta(seconds=2),
            normalized_event_count=0,
            raw_audit=audit_raw_tree(root),
        )


async def _write_records(
    root: Path,
    records: list[RawMarketRecord],
) -> None:
    writer = AsyncPartitionedParquetWriter(
        root_path=root,
        batch_rows=1,
        flush_seconds=60,
        queue_capacity=10,
    )
    await writer.start()
    for record in records:
        await writer.write(record)
    await writer.close()


async def _write_clean_mixed_collection(
    root: Path,
    settings: Settings,
    *,
    terminal_markers: int = 1,
) -> None:
    in_progress = _begin(root, settings)
    source = _source_record()
    terminal = NOW + timedelta(seconds=1)
    await _write_records(
        root,
        [
            source,
            normalized_event_journal_record(_trade(source)),
            *[
                feature_timeline_end_journal_record(terminal)
                for _ in range(terminal_markers)
            ],
        ],
    )
    complete_collection_manifest(
        root,
        expected_run_id=in_progress.run_id,
        terminal_at=terminal + timedelta(seconds=1),
        feature_timeline_end_at=terminal,
        normalized_event_count=1,
        raw_audit=audit_raw_tree(root),
    )


@pytest.mark.asyncio
async def test_auto_keeps_legacy_tree_without_journal_or_manifest_raw(
    scratch_root: Path,
) -> None:
    root = scratch_root / "legacy"
    await _write_records(root, [_source_record()])

    resolution = resolve_replay_source(root, ReplaySourceMode.AUTO)

    assert resolution.mode is ReplaySourceMode.RAW
    assert resolution.clean_collection is None


@pytest.mark.asyncio
async def test_auto_selects_valid_clean_mixed_journal(scratch_root: Path) -> None:
    root = scratch_root / "clean"
    await _write_clean_mixed_collection(root, load_settings(environ={}))

    resolution = resolve_replay_source(root, ReplaySourceMode.AUTO)

    assert resolution.mode is ReplaySourceMode.JOURNAL
    assert resolution.clean_collection is not None
    assert resolution.clean_collection.normalized_event_count == 1
    assert resolution.clean_collection.terminal_marker_count == 1
    explicit = resolve_replay_source(root, ReplaySourceMode.JOURNAL)
    assert explicit.mode is ReplaySourceMode.JOURNAL


@pytest.mark.asyncio
async def test_auto_rejects_partial_journal_without_manifest_before_output(
    scratch_root: Path,
) -> None:
    root = scratch_root / "partial"
    source = _source_record()
    await _write_records(root, [normalized_event_journal_record(_trade(source))])
    output = scratch_root / "must-not-exist"

    with pytest.raises(CollectionManifestError, match="missing"):
        await run_replay(
            load_settings(environ={}),
            input_path=root,
            start=None,
            end=None,
            exchanges=None,
            symbols=None,
            channels=None,
            order=ReplayOrder.RECEIVE_TIME,
            source_mode=ReplaySourceMode.AUTO,
            speed=0,
            feature_output_path=output,
        )

    assert not output.exists()


@pytest.mark.parametrize(
    ("lineage_field", "mismatch"),
    [
        ("code_version", "capture-code-version"),
        ("code_sha256", "0" * 64),
        ("strategy_version", "capture-strategy-version"),
        ("settings_sha256", "f" * 64),
    ],
)
@pytest.mark.parametrize(
    "source_mode",
    [ReplaySourceMode.JOURNAL, ReplaySourceMode.AUTO],
)
@pytest.mark.asyncio
async def test_journal_replay_rejects_capture_lineage_mismatch_before_output(
    scratch_root: Path,
    lineage_field: str,
    mismatch: str,
    source_mode: ReplaySourceMode,
) -> None:
    root = scratch_root / "capture"
    await _write_clean_mixed_collection(root, load_settings(environ={}))
    manifest_path = root / COLLECTION_MANIFEST_FILENAME
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload[lineage_field] = mismatch
    manifest_path.write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    output = scratch_root / "output"

    with pytest.raises(RuntimeError, match=lineage_field):
        await run_replay(
            load_settings(environ={}),
            input_path=root,
            start=None,
            end=None,
            exchanges=None,
            symbols=None,
            channels=None,
            order=ReplayOrder.RECEIVE_TIME,
            source_mode=source_mode,
            speed=0,
            feature_output_path=output,
        )

    assert not output.exists()


@pytest.mark.parametrize(
    ("source_mode", "with_evidence"),
    [
        (ReplaySourceMode.RAW, True),
        (ReplaySourceMode.AUTO, False),
    ],
)
@pytest.mark.asyncio
async def test_non_journal_replay_does_not_require_capture_lineage(
    scratch_root: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_mode: ReplaySourceMode,
    with_evidence: bool,
) -> None:
    root = scratch_root / "source"
    root.mkdir()
    if with_evidence:
        (root / COLLECTION_MANIFEST_FILENAME).write_text(
            "{not-a-valid-manifest",
            encoding="utf-8",
        )

    def fail_if_called() -> str:
        raise AssertionError("non-journal replay must not compute capture lineage")

    monkeypatch.setattr("cvf.main.package_source_sha256", fail_if_called)

    with pytest.raises(RuntimeError, match="at least one selected raw row"):
        await run_replay(
            load_settings(environ={}),
            input_path=root,
            start=None,
            end=None,
            exchanges=None,
            symbols=None,
            channels=None,
            order=ReplayOrder.RECEIVE_TIME,
            source_mode=source_mode,
            speed=0,
            feature_output_path=scratch_root / "output",
        )


def test_auto_rejects_in_progress_manifest(scratch_root: Path) -> None:
    root = scratch_root / "in-progress"
    _begin(root, load_settings(environ={}))

    with pytest.raises(CollectionManifestError, match="no CLEAN_END"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


def test_auto_rejects_corrupt_manifest(scratch_root: Path) -> None:
    root = scratch_root / "corrupt"
    root.mkdir()
    (root / COLLECTION_MANIFEST_FILENAME).write_text(
        "{not-json",
        encoding="utf-8",
    )

    with pytest.raises(CollectionManifestError, match="corrupt"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


@pytest.mark.asyncio
async def test_auto_rejects_raw_audit_mismatch(scratch_root: Path) -> None:
    root = scratch_root / "mismatch"
    settings = load_settings(environ={})
    await _write_clean_mixed_collection(root, settings)
    await _write_records(root, [_source_record(sequence=2)])

    with pytest.raises(CollectionManifestError, match="logical audit mismatch"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


@pytest.mark.asyncio
async def test_auto_rejects_manifest_journal_count_mismatch(
    scratch_root: Path,
) -> None:
    root = scratch_root / "count-mismatch"
    await _write_clean_mixed_collection(root, load_settings(environ={}))
    manifest_path = root / COLLECTION_MANIFEST_FILENAME
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["normalized_event_count"] = 2
    manifest_path.write_text(
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(CollectionManifestError, match="journal count mismatch"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


@pytest.mark.asyncio
async def test_auto_rejects_empty_normalized_journal(
    scratch_root: Path,
) -> None:
    root = scratch_root / "empty-journal"
    settings = load_settings(environ={})
    in_progress = _begin(root, settings)
    terminal = NOW + timedelta(seconds=1)
    await _write_records(root, [feature_timeline_end_journal_record(terminal)])
    complete_collection_manifest(
        root,
        expected_run_id=in_progress.run_id,
        terminal_at=terminal + timedelta(seconds=1),
        feature_timeline_end_at=terminal,
        normalized_event_count=0,
        raw_audit=audit_raw_tree(root),
    )

    with pytest.raises(CollectionManifestError, match="journal is empty"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


@pytest.mark.asyncio
async def test_auto_rejects_more_than_one_terminal_marker(
    scratch_root: Path,
) -> None:
    root = scratch_root / "two-terminals"
    await _write_clean_mixed_collection(
        root,
        load_settings(environ={}),
        terminal_markers=2,
    )

    with pytest.raises(CollectionManifestError, match="exactly one"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


@pytest.mark.asyncio
async def test_auto_rejects_event_beyond_terminal_watermark_and_closes_sort_index(
    scratch_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = scratch_root / "beyond-terminal"
    settings = load_settings(environ={})
    in_progress = _begin(root, settings)
    source = _source_record()
    terminal = NOW + timedelta(seconds=1)
    future_event = _trade(source).model_copy(
        update={
            "local_receive_timestamp": terminal + timedelta(seconds=1),
            "normalization_timestamp": terminal + timedelta(seconds=1),
        }
    )
    await _write_records(
        root,
        [
            normalized_event_journal_record(future_event),
            feature_timeline_end_journal_record(terminal),
        ],
    )
    complete_collection_manifest(
        root,
        expected_run_id=in_progress.run_id,
        terminal_at=terminal + timedelta(seconds=2),
        feature_timeline_end_at=terminal,
        normalized_event_count=1,
        raw_audit=audit_raw_tree(root),
    )
    index_path = scratch_root / "invalid-journal-sort.sqlite3"
    monkeypatch.setattr(
        RawParquetReader,
        "_temporary_index_path",
        staticmethod(lambda: index_path),
    )

    with pytest.raises(CollectionManifestError, match="terminal watermark"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)

    assert not index_path.exists()
    assert not Path(f"{index_path}-journal").exists()
    assert not Path(f"{index_path}-wal").exists()
    assert not Path(f"{index_path}-shm").exists()


@pytest.mark.asyncio
async def test_auto_rejects_tampered_journal_connection_generation(
    scratch_root: Path,
) -> None:
    root = scratch_root / "generation-mismatch"
    settings = load_settings(environ={})
    in_progress = _begin(root, settings)
    source = _source_record()
    terminal = NOW + timedelta(seconds=1)
    journal = normalized_event_journal_record(_trade(source)).model_copy(
        update={"connection_generation": 9}
    )
    await _write_records(
        root,
        [journal, feature_timeline_end_journal_record(terminal)],
    )
    complete_collection_manifest(
        root,
        expected_run_id=in_progress.run_id,
        terminal_at=terminal + timedelta(seconds=1),
        feature_timeline_end_at=terminal,
        normalized_event_count=1,
        raw_audit=audit_raw_tree(root),
    )

    with pytest.raises(CollectionManifestError, match="invalid event record"):
        resolve_replay_source(root, ReplaySourceMode.AUTO)


@pytest.mark.asyncio
async def test_explicit_raw_is_recovery_and_excludes_invalid_journal(
    scratch_root: Path,
) -> None:
    root = scratch_root / "recovery"
    source = _source_record()
    await _write_records(
        root,
        [
            source,
            normalized_event_journal_record(_trade(source)),
        ],
    )

    resolution = resolve_replay_source(root, ReplaySourceMode.RAW)

    assert resolution.mode is ReplaySourceMode.RAW
    assert resolution.clean_collection is None


@pytest.mark.asyncio
async def test_compaction_preserves_and_revalidates_clean_manifest(
    scratch_root: Path,
) -> None:
    source = scratch_root / "source"
    output = scratch_root / "compacted"
    await _write_clean_mixed_collection(source, load_settings(environ={}))
    source_manifest = read_collection_manifest(source)

    report = compact_raw_tree(source, output, target_rows=2)
    validation = validate_clean_collection(output)

    assert report.before.content_digest == report.after.content_digest
    assert validation.manifest == source_manifest
    assert validation.raw_audit == report.after
    assert validation.manifest.terminal is CollectionTerminal.CLEAN_END


@pytest.mark.asyncio
async def test_interrupted_compaction_is_never_auto_replayed_as_legacy_raw(
    scratch_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = scratch_root / "legacy-source"
    output = scratch_root / "partial-compaction"
    await _write_records(
        source,
        [_source_record(1), _source_record(2)],
    )
    original_write_rows = raw_compaction._write_rows
    calls = 0

    def fail_after_first_output(
        root: Path,
        partition: tuple[str, str, str, str],
        rows: list[dict[str, object]],
    ) -> None:
        nonlocal calls
        calls += 1
        original_write_rows(root, partition, rows)
        if calls == 1:
            raise RuntimeError("injected compaction interruption")

    monkeypatch.setattr(raw_compaction, "_write_rows", fail_after_first_output)
    with pytest.raises(RuntimeError, match="injected compaction interruption"):
        compact_raw_tree(source, output, target_rows=1)

    assert (output / COMPACTION_IN_PROGRESS_FILENAME).is_file()
    with pytest.raises(CollectionManifestError, match="compaction"):
        resolve_replay_source(output, ReplaySourceMode.AUTO)
    with pytest.raises(ValueError, match="must be empty"):
        compact_raw_tree(source, output, target_rows=1)


def test_begin_manifest_exclusively_claims_an_empty_output(
    scratch_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = scratch_root / "race"
    root.mkdir()
    settings = load_settings(environ={})
    barrier = threading.Barrier(2)
    original_iterdir = Path.iterdir

    def synchronized_iterdir(path: Path):
        entries = list(original_iterdir(path))
        if path.resolve() == root.resolve():
            barrier.wait(timeout=5)
        return iter(entries)

    monkeypatch.setattr(Path, "iterdir", synchronized_iterdir)

    def claim() -> CollectionManifest:
        return _begin(root, settings)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = [
            future
            for future in (
                executor.submit(claim),
                executor.submit(claim),
            )
        ]
        outcomes: list[CollectionManifest | BaseException] = []
        for future in results:
            try:
                outcomes.append(future.result(timeout=10))
            except BaseException as exc:
                outcomes.append(exc)

    assert sum(isinstance(item, CollectionManifest) for item in outcomes) == 1
    assert sum(isinstance(item, CollectionManifestError) for item in outcomes) == 1
    assert read_collection_manifest(root).terminal is CollectionTerminal.IN_PROGRESS


def test_manifest_claim_scope_is_explicitly_limited(
    scratch_root: Path,
) -> None:
    manifest = _begin(scratch_root / "claim", load_settings(environ={}))

    assert "clean-run completeness" in manifest.claim_scope
    assert "not a per-raw 0/1/N" in manifest.claim_scope
    assert "crash-safe WAL" in manifest.claim_scope

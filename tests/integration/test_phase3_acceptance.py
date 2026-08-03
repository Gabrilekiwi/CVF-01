"""End-to-end checks for the fixed-dataset Phase 3 acceptance harness."""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from cvf.acceptance import run_phase3_acceptance, run_phase3_stability
from cvf.config import load_settings
from cvf.models import Exchange
from cvf.replay import RawRecordNormalizer, ReplayOrder
from cvf.storage import (
    AsyncPartitionedParquetWriter,
    RawMarketRecord,
    audit_raw_tree,
    begin_collection_manifest,
    complete_collection_manifest,
)
from cvf.storage.raw import (
    feature_timeline_end_journal_record,
    normalized_event_journal_record,
)

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)


@contextmanager
def scratch_directory() -> Iterator[Path]:
    path = Path("data/processed") / f"p3a-{uuid4().hex[:8]}"
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


def agg_trade(sequence: int, offset_seconds: int) -> RawMarketRecord:
    exchanged = NOW + timedelta(seconds=offset_seconds)
    received = exchanged + timedelta(milliseconds=10)
    payload = {
        "e": "aggTrade",
        "E": int(exchanged.timestamp() * 1000),
        "a": sequence,
        "s": "BTCUSDT",
        "p": "100.0",
        "q": "2.0",
        "f": sequence,
        "l": sequence,
        "T": int(exchanged.timestamp() * 1000),
        "m": False,
    }
    return RawMarketRecord(
        exchange=Exchange.BINANCE,
        symbol="BTC-USDT-PERP",
        channel="aggTrade",
        message_kind="market_data",
        transport="websocket",
        exchange_timestamp=exchanged,
        local_receive_timestamp=received,
        normalization_timestamp=received + timedelta(milliseconds=1),
        sequence_id=sequence,
        connection_generation=1,
        raw_payload=json.dumps(payload, separators=(",", ":")).encode(),
    )


@pytest.mark.asyncio
async def test_phase3_acceptance_replays_twice_and_writes_evidence() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        output = temporary / "acceptance"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(agg_trade(1, 0))
        await writer.write(agg_trade(2, 1))
        await writer.close()

        report = await run_phase3_acceptance(
            load_settings(environ={}),
            input_path=raw,
            output_path=output,
            first_batch_rows=2,
            second_batch_rows=3,
            requested_live_stability_seconds=3_600,
        )

        assert report.raw_audit.rows == 2
        assert report.deterministic_replay
        assert report.snapshot_counts_match
        assert report.consistency.identical
        assert report.no_lookahead
        assert report.feature_files_audited
        assert report.safety_boundary_preserved
        assert report.first_run.feature_ticks == 1
        assert report.first_run.single_venue_snapshots == 12
        assert report.first_run.cross_venue_snapshots == 6
        assert report.first_run.feature_audit.rows == 18
        assert report.first_run.feature_state.accepted_events == 2
        assert report.first_run.writer_flush_seconds == 60
        assert report.first_run.replay_order is ReplayOrder.RECEIVE_TIME
        assert report.first_run.safety_evidence.component_graph_matches_expected
        assert report.first_run.safety_evidence.output_event_type_counts == {
            "MARKET_FEATURE": 18
        }
        assert report.first_run.safety_evidence.forbidden_output_events_observed == 0
        assert not report.first_run.safety_evidence.network_request_instrumentation_enabled
        assert not report.live_stability_duration_completed
        assert report.live_stability_status.startswith("pending:")
        assert (output / "summary.json").is_file()
        assert (output / "summary.md").is_file()
        assert (output / "run-1-metrics.json").is_file()
        assert (output / "run-2-metrics.json").is_file()

        run_one_checkpoint = output / "run-1-metrics.json"
        checkpoint_payload = json.loads(run_one_checkpoint.read_text(encoding="utf-8"))
        checkpoint_payload["stage"] = "REPLAY_COMPLETE"
        run_one_checkpoint.write_text(
            json.dumps(checkpoint_payload),
            encoding="utf-8",
        )
        resumed = await run_phase3_acceptance(
            load_settings(environ={}),
            input_path=raw,
            output_path=output,
            first_batch_rows=2,
            second_batch_rows=3,
            requested_live_stability_seconds=3_600,
            resume=True,
        )

        assert resumed.first_run == report.first_run
        assert resumed.second_run == report.second_run
        recovered_payload = json.loads(run_one_checkpoint.read_text(encoding="utf-8"))
        assert recovered_payload["stage"] == "AUDIT_COMPLETE"


@pytest.mark.asyncio
async def test_phase3_stability_records_an_honest_capped_observation() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        output = temporary / "stability"
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=2,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(agg_trade(1, 0))
        await writer.write(agg_trade(2, 1))
        await writer.close()

        report = await run_phase3_stability(
            load_settings(environ={}),
            input_path=raw,
            output_path=output,
            target_seconds=3_600,
            maximum_iterations=1,
        )

        assert len(report.iterations) == 1
        assert not report.fixed_replay_target_reached
        assert not report.continuous_live_soak_completed
        assert "continuous live-feed soak remains pending" in report.status
        assert report.all_deterministic
        assert report.all_no_lookahead
        assert report.all_feature_files_audited
        assert report.all_safety_boundaries_preserved
        assert report.total_raw_records == 4
        assert report.total_normalized_events == 4
        assert report.total_feature_snapshots == 36
        assert not (output / "iterations" / "0001" / "run-1").exists()
        assert not (output / "iterations" / "0001" / "run-2").exists()
        assert (output / "stability-summary.json").is_file()
        assert (output / "stability-summary.md").is_file()


@pytest.mark.asyncio
async def test_journal_acceptance_rejects_capture_lineage_mismatch_before_output() -> None:
    with scratch_directory() as temporary:
        raw = temporary / "raw"
        output = temporary / "must-not-exist"
        settings = load_settings(environ={})
        started = begin_collection_manifest(
            raw,
            started_at=NOW - timedelta(seconds=1),
            code_version="0.3.1",
            strategy_version=settings.app.strategy_version,
            settings_sha256="0" * 64,
        )
        source = agg_trade(1, 0)
        event = RawRecordNormalizer().normalize(source)[0]
        terminal = event.local_receive_timestamp + timedelta(seconds=1)
        writer = AsyncPartitionedParquetWriter(
            root_path=raw,
            batch_rows=3,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        for record in (
            source,
            normalized_event_journal_record(event),
            feature_timeline_end_journal_record(terminal),
        ):
            await writer.write(record)
        await writer.close()
        complete_collection_manifest(
            raw,
            expected_run_id=started.run_id,
            terminal_at=terminal + timedelta(seconds=1),
            feature_timeline_end_at=terminal,
            normalized_event_count=1,
            raw_audit=audit_raw_tree(raw),
        )

        with pytest.raises(RuntimeError, match="settings_sha256"):
            await run_phase3_acceptance(
                settings,
                input_path=raw,
                output_path=output,
            )

        assert not output.exists()

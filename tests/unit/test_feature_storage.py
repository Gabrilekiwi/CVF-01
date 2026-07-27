"""Phase 3D feature Parquet persistence, lineage, and consistency."""

from __future__ import annotations

import asyncio
import shutil
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

from cvf.config import Settings, load_settings
from cvf.features import CrossVenueFeatureEngine
from cvf.features.models import (
    CrossVenueFeatureSnapshot,
    FeatureSnapshot,
    FeatureUnavailableCode,
    FeatureUnavailableReason,
)
from cvf.main import main
from cvf.models.enums import Exchange
from cvf.storage import (
    FEATURE_PARQUET_SCHEMA,
    AsyncFeatureParquetWriter,
    FeatureParquetError,
    FeatureParquetReader,
    FeatureScanFilter,
    FeatureWriteStatus,
    audit_feature_tree,
    compare_feature_trees,
)

NOW = datetime(2026, 7, 27, 12, 0, tzinfo=UTC)
SYMBOL = "BTC-USDT-PERP"


@contextmanager
def scratch_directory() -> Iterator[Path]:
    path = Path("data/processed") / f"features-{uuid4().hex[:10]}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def settings(**storage_updates: object) -> Settings:
    base = load_settings(environ={})
    storage = base.storage.model_copy(update=storage_updates)
    features = base.features.model_copy(
        update={"cross_venue_zscore_minimum_samples": 2}
    )
    return base.model_copy(update={"storage": storage, "features": features})


def single(
    exchange: Exchange,
    *,
    at: datetime = NOW,
    identifier: int | None = None,
    symbol: str = SYMBOL,
    window_seconds: int = 5,
    warm: bool = True,
    healthy: bool = True,
) -> FeatureSnapshot:
    if identifier is None:
        identifier = (
            int(at.timestamp()) * 10
            + (1 if exchange is Exchange.BINANCE else 2)
            + window_seconds
        )
    reasons: tuple[FeatureUnavailableReason, ...] = ()
    if not warm or not healthy:
        reasons = (
            FeatureUnavailableReason(
                code=(
                    FeatureUnavailableCode.NOT_WARM
                    if not warm
                    else FeatureUnavailableCode.HEALTH_BLOCKED
                ),
                detail="test fixture availability",
            ),
        )
    return FeatureSnapshot(
        exchange=exchange,
        symbol=symbol,
        exchange_timestamp=at,
        local_receive_timestamp=at,
        normalization_timestamp=at,
        sequence_id=identifier,
        raw_payload_reference=f"raw://{UUID(int=identifier)}",
        feature_snapshot_id=UUID(int=identifier),
        strategy_version="0.1.0",
        calculation_timestamp=at,
        decision_timestamp=at,
        window_seconds=window_seconds,
        book_generation=3,
        source_sequence_id=identifier,
        source_event_count=1,
        oldest_source_timestamp=at - timedelta(milliseconds=10),
        newest_source_timestamp=at,
        data_age_ms=0,
        is_warm=warm,
        is_healthy=healthy,
        unavailable_reasons=reasons,
    )


def cross(
    config: Settings,
    *,
    at: datetime = NOW,
    symbol: str = SYMBOL,
    window_seconds: int = 5,
) -> CrossVenueFeatureSnapshot:
    return CrossVenueFeatureEngine(config).calculate(
        [
            single(
                Exchange.BINANCE,
                at=at,
                identifier=int(at.timestamp()) * 10 + 1 + window_seconds,
                symbol=symbol,
                window_seconds=window_seconds,
            ),
            single(
                Exchange.OKX,
                at=at,
                identifier=int(at.timestamp()) * 10 + 2 + window_seconds,
                symbol=symbol,
                window_seconds=window_seconds,
            ),
        ],
        symbol=symbol,
        decision_timestamp=at,
        window_seconds=window_seconds,
    )


async def write_tree(
    root: Path,
    values: Sequence[FeatureSnapshot | CrossVenueFeatureSnapshot],
    *,
    config: Settings | None = None,
    batch_rows: int = 100,
) -> AsyncFeatureParquetWriter:
    writer = AsyncFeatureParquetWriter(
        root_path=root,
        settings=config or settings(),
        batch_rows=batch_rows,
        flush_seconds=60,
        queue_capacity=100,
    )
    await writer.start()
    for value in values:
        await writer.write(value)
    await writer.close()
    return writer


def parquet_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet"))


def rewrite_column(path: Path, name: str, values: list[object]) -> None:
    table = pq.ParquetFile(path).read()
    index = table.schema.get_field_index(name)
    replacement = pa.array(values, type=FEATURE_PARQUET_SCHEMA.field(name).type)
    pq.write_table(
        table.set_column(index, FEATURE_PARQUET_SCHEMA.field(name), replacement),
        path,
    )


@pytest.mark.asyncio
async def test_writes_required_versioned_hive_partition_layout() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])

        path = parquet_files(temporary)[0]
        assert (
            "feature_schema=v1" in path.parts
            and "date=2026-07-27" in path.parts
            and "symbol=BTC-USDT-PERP" in path.parts
            and "scope=BINANCE" in path.parts
        )


@pytest.mark.asyncio
async def test_round_trips_single_venue_snapshot_exactly() -> None:
    with scratch_directory() as temporary:
        expected = single(Exchange.BINANCE)
        await write_tree(temporary, [expected])

        actual = next(FeatureParquetReader(temporary).iter_records())
        assert actual.snapshot == expected
        assert actual.snapshot.raw_payload_reference == expected.raw_payload_reference
        assert len(actual.config_hash) == 64


@pytest.mark.asyncio
async def test_round_trips_cross_venue_snapshot_and_source_ids() -> None:
    with scratch_directory() as temporary:
        config = settings()
        expected = cross(config)
        await write_tree(temporary, [expected], config=config)

        actual = next(FeatureParquetReader(temporary).iter_records())
        assert actual.snapshot == expected
        assert isinstance(actual.snapshot, CrossVenueFeatureSnapshot)
        assert actual.snapshot.source_snapshot_ids == expected.source_snapshot_ids
        assert actual.config_hash == expected.config_hash
        assert actual.code_version == expected.code_version


@pytest.mark.asyncio
async def test_writer_deduplicates_identical_snapshot_id_and_payload() -> None:
    with scratch_directory() as temporary:
        value = single(Exchange.BINANCE)
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            batch_rows=10,
            flush_seconds=60,
            queue_capacity=10,
        )
        await writer.start()
        first = await writer.write(value)
        second = await writer.write(value)
        await writer.close()

        assert first is FeatureWriteStatus.ACCEPTED
        assert second is FeatureWriteStatus.DEDUPLICATED
        assert writer.stats.accepted_snapshots == 1
        assert writer.stats.deduplicated_snapshots == 1
        assert audit_feature_tree(temporary).rows == 1


@pytest.mark.asyncio
async def test_duplicate_id_with_different_payload_fails_closed() -> None:
    with scratch_directory() as temporary:
        original = single(Exchange.BINANCE)
        conflict = original.model_copy(
            update={"decision_timestamp": NOW + timedelta(seconds=1)}
        )
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
        )
        await writer.start()
        await writer.write(original)
        with pytest.raises(FeatureParquetError, match="different content"):
            await writer.write(conflict)
        await writer.close()


@pytest.mark.asyncio
async def test_deduplication_cache_has_hard_capacity() -> None:
    with scratch_directory() as temporary:
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            deduplication_capacity=1,
        )
        await writer.start()
        await writer.write(single(Exchange.BINANCE, identifier=101))
        await writer.write(single(Exchange.OKX, identifier=102))
        await writer.close()

        assert writer.stats.deduplication_cache_size == 1


@pytest.mark.asyncio
async def test_partial_batch_flushes_on_close() -> None:
    with scratch_directory() as temporary:
        writer = await write_tree(
            temporary,
            [single(Exchange.BINANCE)],
            batch_rows=100,
        )

        assert writer.stats.written_snapshots == 1
        assert writer.stats.flush_count == 1
        assert writer.stats.average_write_latency_ms is not None
        assert writer.stats.last_write_latency_ms is not None
        assert writer.stats.maximum_write_latency_ms is not None
        assert writer.stats.average_write_latency_ms >= 0
        assert writer.stats.last_write_latency_ms >= 0
        assert writer.stats.maximum_write_latency_ms >= writer.stats.last_write_latency_ms


@pytest.mark.asyncio
async def test_partial_batch_flushes_on_interval() -> None:
    with scratch_directory() as temporary:
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            batch_rows=100,
            flush_seconds=0.01,
            queue_capacity=10,
        )
        await writer.start()
        await writer.write(single(Exchange.BINANCE))

        async def wait_until_written() -> None:
            while writer.stats.written_snapshots == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_until_written(), timeout=1)
        assert writer.stats.flush_count == 1
        await writer.close()


@pytest.mark.asyncio
async def test_writer_lifecycle_rejects_invalid_operations() -> None:
    with scratch_directory() as temporary:
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
        )
        with pytest.raises(FeatureParquetError, match="start"):
            await writer.write(single(Exchange.BINANCE))
        await writer.start()
        await writer.close()
        await writer.close()
        with pytest.raises(FeatureParquetError, match="after close"):
            await writer.write(single(Exchange.BINANCE))
        with pytest.raises(FeatureParquetError, match="restart"):
            await writer.start()


@pytest.mark.asyncio
async def test_queue_backpressure_is_observable() -> None:
    with scratch_directory() as temporary:
        blocked: list[UUID] = []
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            batch_rows=2,
            flush_seconds=60,
            queue_capacity=1,
            on_backpressure=lambda value: blocked.append(value.feature_snapshot_id),
        )
        await writer.start()
        await writer.write(single(Exchange.BINANCE, identifier=201))
        await writer.write(single(Exchange.OKX, identifier=202))
        await writer.write(single(Exchange.BINANCE, identifier=203))
        await writer.close()

        assert writer.stats.backpressure_events == len(blocked)
        assert writer.stats.backpressure_events >= 1


@pytest.mark.asyncio
async def test_partition_files_are_atomic_and_leave_no_temp_siblings() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])

        assert parquet_files(temporary)
        assert not list(temporary.rglob("*.tmp"))


@pytest.mark.asyncio
async def test_rows_are_deterministically_sorted_inside_partition() -> None:
    with scratch_directory() as temporary:
        later = single(
            Exchange.BINANCE,
            at=NOW + timedelta(seconds=1),
            identifier=302,
        )
        earlier = single(Exchange.BINANCE, identifier=301)
        await write_tree(temporary, [later, earlier])

        records = list(FeatureParquetReader(temporary).iter_records())
        assert [record.feature_snapshot_id for record in records] == [
            earlier.feature_snapshot_id,
            later.feature_snapshot_id,
        ]


@pytest.mark.asyncio
async def test_reader_filters_inclusive_decision_time_range() -> None:
    with scratch_directory() as temporary:
        first = single(Exchange.BINANCE, identifier=401)
        second = single(
            Exchange.BINANCE,
            at=NOW + timedelta(seconds=1),
            identifier=402,
        )
        await write_tree(temporary, [first, second])

        selected = list(
            FeatureParquetReader(temporary).iter_records(
                filters=FeatureScanFilter(start=NOW, end=NOW)
            )
        )
        assert [record.feature_snapshot_id for record in selected] == [
            first.feature_snapshot_id
        ]


@pytest.mark.asyncio
async def test_reader_filters_scope() -> None:
    with scratch_directory() as temporary:
        await write_tree(
            temporary,
            [
                single(Exchange.BINANCE, identifier=501),
                single(Exchange.OKX, identifier=502),
            ],
        )

        selected = list(
            FeatureParquetReader(temporary).iter_records(
                filters=FeatureScanFilter(scopes=frozenset({Exchange.OKX}))
            )
        )
        assert [record.scope for record in selected] == [Exchange.OKX]


@pytest.mark.asyncio
async def test_reader_filters_symbol_and_window() -> None:
    with scratch_directory() as temporary:
        await write_tree(
            temporary,
            [
                single(Exchange.BINANCE, identifier=601),
                single(
                    Exchange.BINANCE,
                    identifier=602,
                    symbol="ETH-USDT-PERP",
                    window_seconds=15,
                ),
            ],
        )

        selected = list(
            FeatureParquetReader(temporary).iter_records(
                filters=FeatureScanFilter(
                    symbols=frozenset({"ETH-USDT-PERP"}),
                    windows=frozenset({15}),
                )
            )
        )
        assert len(selected) == 1
        assert selected[0].snapshot.symbol == "ETH-USDT-PERP"
        assert selected[0].snapshot.window_seconds == 15


@pytest.mark.asyncio
async def test_reader_filters_warm_and_healthy_flags() -> None:
    with scratch_directory() as temporary:
        await write_tree(
            temporary,
            [
                single(Exchange.BINANCE, identifier=701),
                single(Exchange.OKX, identifier=702, warm=False),
                single(Exchange.BINANCE, identifier=703, healthy=False),
            ],
        )

        selected = list(
            FeatureParquetReader(temporary).iter_records(
                filters=FeatureScanFilter(is_warm=True, is_healthy=True)
            )
        )
        assert [record.feature_snapshot_id for record in selected] == [UUID(int=701)]


@pytest.mark.asyncio
async def test_reader_filters_schema_snapshot_id_and_unavailable_reason() -> None:
    with scratch_directory() as temporary:
        available = single(Exchange.BINANCE, identifier=711)
        unavailable = single(Exchange.OKX, identifier=712, warm=False)
        await write_tree(temporary, [available, unavailable])

        selected = list(
            FeatureParquetReader(temporary).iter_records(
                filters=FeatureScanFilter(
                    schema_versions=frozenset({1}),
                    snapshot_ids=frozenset({unavailable.feature_snapshot_id}),
                    unavailable_codes=frozenset(
                        {FeatureUnavailableCode.NOT_WARM}
                    ),
                )
            )
        )
        assert [record.feature_snapshot_id for record in selected] == [
            unavailable.feature_snapshot_id
        ]
        assert (
            list(
                FeatureParquetReader(temporary).iter_records(
                    filters=FeatureScanFilter(schema_versions=frozenset({2}))
                )
            )
            == []
        )


def test_scan_filter_rejects_naive_reversed_and_invalid_values() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        FeatureScanFilter(start=datetime(2026, 7, 27))
    with pytest.raises(ValueError, match="precede"):
        FeatureScanFilter(start=NOW, end=NOW - timedelta(seconds=1))
    with pytest.raises(ValueError, match="positive"):
        FeatureScanFilter(windows=frozenset({0}))
    with pytest.raises(ValueError, match="schema versions"):
        FeatureScanFilter(schema_versions=frozenset({0}))
    with pytest.raises(ValueError, match="feature-producing"):
        FeatureScanFilter(scopes=frozenset({Exchange.SIMULATED}))


@pytest.mark.asyncio
async def test_reader_rejects_payload_hash_tampering() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        path = parquet_files(temporary)[0]
        rewrite_column(path, "payload_json", ["{}"])

        with pytest.raises(ValueError, match="digest mismatch"):
            list(FeatureParquetReader(temporary).iter_records())


@pytest.mark.asyncio
async def test_reader_rejects_metadata_payload_lineage_mismatch() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        path = parquet_files(temporary)[0]
        rewrite_column(path, "source_event_count", [999])

        with pytest.raises(ValueError, match="lineage mismatch"):
            list(FeatureParquetReader(temporary).iter_records())


@pytest.mark.asyncio
async def test_reader_rejects_wrong_partition_directory() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        original = parquet_files(temporary)[0]
        wrong = (
            temporary
            / "feature_schema=v1"
            / "date=2026-07-27"
            / "symbol=BTC-USDT-PERP"
            / "scope=OKX"
        )
        wrong.mkdir(parents=True)
        moved = wrong / original.name
        original.replace(moved)

        with pytest.raises(ValueError, match="partition mismatch"):
            list(FeatureParquetReader(temporary).iter_records())


@pytest.mark.asyncio
async def test_reader_rejects_schema_drift() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        path = parquet_files(temporary)[0]
        table = pq.ParquetFile(path).read().drop(["payload_sha256"])
        pq.write_table(table, path)

        with pytest.raises(ValueError, match="schema mismatch"):
            list(FeatureParquetReader(temporary).iter_records())


@pytest.mark.asyncio
async def test_audit_rejects_duplicate_ids_across_files() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        original = parquet_files(temporary)[0]
        shutil.copyfile(original, original.with_name(f"duplicate-{original.name}"))

        with pytest.raises(ValueError, match="duplicate persisted"):
            audit_feature_tree(temporary)


@pytest.mark.asyncio
async def test_audit_reports_lineage_scope_and_time_bounds() -> None:
    with scratch_directory() as temporary:
        config = settings()
        values: list[FeatureSnapshot | CrossVenueFeatureSnapshot] = [
            single(Exchange.BINANCE, identifier=801),
            single(
                Exchange.OKX,
                at=NOW + timedelta(seconds=1),
                identifier=802,
            ),
            cross(config, at=NOW + timedelta(seconds=2)),
        ]
        await write_tree(temporary, values, config=config)

        audit = audit_feature_tree(temporary)
        assert audit.rows == audit.unique_snapshot_ids == 3
        assert audit.partitions == 3
        assert audit.scopes == ("BINANCE", "CROSS_VENUE", "OKX")
        assert audit.code_versions == ("0.2.1",)
        assert audit.config_hashes
        assert audit.unavailable_snapshots == 1
        assert audit.unavailable_reason_counts == {
            "FEATURE_INPUT_MISSING": 6,
            "INSUFFICIENT_HISTORY": 1,
        }
        assert audit.earliest_decision_timestamp == NOW
        assert audit.latest_decision_timestamp == NOW + timedelta(seconds=2)


@pytest.mark.asyncio
async def test_audit_summarizes_structured_unavailable_reasons() -> None:
    with scratch_directory() as temporary:
        await write_tree(
            temporary,
            [single(Exchange.BINANCE, identifier=851, warm=False)],
        )

        audit = audit_feature_tree(temporary)
        assert audit.unavailable_snapshots == 1
        assert audit.unavailable_reason_counts == {"NOT_WARM": 1}


@pytest.mark.asyncio
async def test_filtered_audit_counts_only_selected_records_and_files() -> None:
    with scratch_directory() as temporary:
        await write_tree(
            temporary,
            [
                single(Exchange.BINANCE, identifier=901),
                single(Exchange.OKX, identifier=902),
            ],
        )

        audit = audit_feature_tree(
            temporary,
            filters=FeatureScanFilter(scopes=frozenset({Exchange.OKX})),
        )
        assert audit.rows == 1
        assert audit.files == 1
        assert audit.scopes == ("OKX",)


@pytest.mark.asyncio
async def test_live_replay_trees_match_despite_arrival_and_batch_order() -> None:
    with scratch_directory() as temporary:
        live = temporary / "live"
        replay = temporary / "replay"
        values = [
            single(Exchange.BINANCE, identifier=1001),
            single(Exchange.OKX, identifier=1002),
            single(
                Exchange.BINANCE,
                at=NOW + timedelta(seconds=1),
                identifier=1003,
            ),
        ]
        await write_tree(live, values, batch_rows=2)
        await write_tree(replay, list(reversed(values)), batch_rows=100)

        report = compare_feature_trees(live, replay)
        assert report.identical is True
        assert report.left.content_digest == report.right.content_digest
        assert report.left.files != report.right.files


@pytest.mark.asyncio
async def test_independent_live_and_replay_generation_persists_identically() -> None:
    with scratch_directory() as temporary:
        config = settings()
        live = temporary / "live"
        replay = temporary / "replay"
        live_singles = [
            single(Exchange.BINANCE, identifier=1051),
            single(Exchange.OKX, identifier=1052),
        ]
        replay_singles = [
            single(Exchange.BINANCE, identifier=1051),
            single(Exchange.OKX, identifier=1052),
        ]
        live_cross = CrossVenueFeatureEngine(config).calculate(
            live_singles,
            symbol=SYMBOL,
            decision_timestamp=NOW,
            window_seconds=5,
        )
        replay_cross = CrossVenueFeatureEngine(config).calculate(
            list(reversed(replay_singles)),
            symbol=SYMBOL,
            decision_timestamp=NOW,
            window_seconds=5,
        )
        await write_tree(
            live,
            [*live_singles, live_cross],
            config=config,
            batch_rows=1,
        )
        await write_tree(
            replay,
            [replay_cross, *reversed(replay_singles)],
            config=config,
            batch_rows=100,
        )

        assert live_cross == replay_cross
        assert compare_feature_trees(live, replay).identical is True


@pytest.mark.asyncio
async def test_consistency_report_detects_logical_difference() -> None:
    with scratch_directory() as temporary:
        left = temporary / "left"
        right = temporary / "right"
        await write_tree(left, [single(Exchange.BINANCE, identifier=1101)])
        await write_tree(right, [single(Exchange.BINANCE, identifier=1102)])

        report = compare_feature_trees(left, right)
        assert report.identical is False
        assert report.left.content_digest != report.right.content_digest


@pytest.mark.asyncio
async def test_consistency_is_exact_and_has_no_implicit_float_tolerance() -> None:
    with scratch_directory() as temporary:
        left = temporary / "left"
        right = temporary / "right"
        baseline = single(Exchange.BINANCE, identifier=1151)
        changed = single(Exchange.BINANCE, identifier=1152).model_copy(
            update={"data_age_ms": 1e-15}
        )
        await write_tree(left, [baseline])
        await write_tree(right, [changed])

        assert compare_feature_trees(left, right).identical is False


@pytest.mark.asyncio
async def test_cross_venue_config_hash_mismatch_is_rejected() -> None:
    with scratch_directory() as temporary:
        original = settings()
        changed = settings(feature_parquet_batch_rows=9_999)
        value = cross(original)
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=changed,
        )
        await writer.start()
        with pytest.raises(FeatureParquetError, match="config hash"):
            await writer.write(value)
        await writer.close()


@pytest.mark.asyncio
async def test_cross_venue_code_version_mismatch_is_rejected() -> None:
    with scratch_directory() as temporary:
        config = settings()
        value = cross(config).model_copy(update={"code_version": "9.9.9"})
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
        )
        await writer.start()
        with pytest.raises(FeatureParquetError, match="code version"):
            await writer.write(value)
        await writer.close()


@pytest.mark.asyncio
async def test_persisted_payload_contains_no_trading_signal_or_order() -> None:
    with scratch_directory() as temporary:
        config = settings()
        await write_tree(
            temporary,
            [single(Exchange.BINANCE), cross(config)],
            config=config,
        )

        table = pq.ParquetFile(parquet_files(temporary)[0]).read()
        payloads = [str(value) for value in table["payload_json"].to_pylist()]
        assert all('"event_type":"MARKET_FEATURE"' in payload for payload in payloads)
        assert all('"signal"' not in payload and '"order"' not in payload for payload in payloads)


@pytest.mark.asyncio
async def test_audit_features_cli_accepts_valid_tree() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])

        assert (
            main(
                [
                    "audit-features",
                    "--input",
                    str(temporary),
                    "--scope",
                    "BINANCE",
                    "--symbol",
                    SYMBOL,
                    "--window",
                    "5",
                ]
            )
            == 0
        )


@pytest.mark.asyncio
async def test_compare_features_cli_accepts_logically_identical_trees() -> None:
    with scratch_directory() as temporary:
        left = temporary / "left"
        right = temporary / "right"
        value = single(Exchange.BINANCE)
        await write_tree(left, [value], batch_rows=1)
        await write_tree(right, [value], batch_rows=10)

        assert (
            main(
                [
                    "compare-features",
                    "--left",
                    str(left),
                    "--right",
                    str(right),
                ]
            )
            == 0
        )


@pytest.mark.asyncio
async def test_compare_features_cli_fails_on_logical_mismatch() -> None:
    with scratch_directory() as temporary:
        left = temporary / "left"
        right = temporary / "right"
        await write_tree(left, [single(Exchange.BINANCE, identifier=1201)])
        await write_tree(right, [single(Exchange.BINANCE, identifier=1202)])

        assert (
            main(
                [
                    "compare-features",
                    "--left",
                    str(left),
                    "--right",
                    str(right),
                ]
            )
            == 1
        )

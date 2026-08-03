"""Phase 3D feature Parquet persistence, lineage, and consistency."""

from __future__ import annotations

import asyncio
import os
import shutil
import sqlite3
import subprocess
import sys
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pytest

import cvf.storage.features as feature_storage
from cvf import __version__
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
        strategy_version=__version__,
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
        assert (
            await writer.write(single(Exchange.BINANCE, identifier=101))
            is FeatureWriteStatus.DEDUPLICATED
        )
        await writer.close()

        assert writer.stats.deduplication_cache_size == 1
        assert audit_feature_tree(temporary).rows == 2


@pytest.mark.asyncio
async def test_new_writer_rebuilds_restart_safe_deduplication_index() -> None:
    with scratch_directory() as temporary:
        config = settings()
        first = single(Exchange.BINANCE, identifier=111)
        second = single(Exchange.OKX, identifier=112)
        initial = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
            deduplication_capacity=1,
        )
        await initial.start()
        await initial.write(first)
        await initial.write(second)
        await initial.close()

        index_path = temporary / ".feature-deduplication-v1.sqlite3"
        assert index_path.is_file()
        index_path.unlink()

        restarted = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
            deduplication_capacity=1,
        )
        await restarted.start()
        assert (
            await restarted.write(first)
            is FeatureWriteStatus.DEDUPLICATED
        )
        conflict = second.model_copy(
            update={"decision_timestamp": NOW + timedelta(seconds=1)}
        )
        with pytest.raises(FeatureParquetError, match="different content"):
            await restarted.write(conflict)
        assert (
            await restarted.write(single(Exchange.BINANCE, identifier=113))
            is FeatureWriteStatus.ACCEPTED
        )
        await restarted.close()

        audit = audit_feature_tree(temporary)
        assert audit.rows == audit.unique_snapshot_ids == 3
        assert audit.files == 3
        assert restarted.stats.deduplication_cache_size == 1


@pytest.mark.asyncio
async def test_feature_root_claim_is_exclusive_across_writer_instances() -> None:
    with scratch_directory() as temporary:
        config = settings()
        first = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
        )
        contender = AsyncFeatureParquetWriter(
            root_path=temporary / "feature_schema=v1",
            settings=config,
        )

        await first.start()
        assert first._root_claim is not None
        with pytest.raises(FeatureParquetError, match="already claimed"):
            await contender.start()
        assert contender._root_claim is None

        await first.close()
        assert first._root_claim is None

        await contender.start()
        assert contender._root_claim is not None
        await contender.close()
        assert contender._root_claim is None


@pytest.mark.asyncio
async def test_feature_root_claim_is_exclusive_across_processes() -> None:
    child_script = """
import asyncio
import sys
from pathlib import Path

from cvf.config import load_settings
from cvf.storage.features import AsyncFeatureParquetWriter


async def main() -> None:
    writer = AsyncFeatureParquetWriter(
        root_path=Path(sys.argv[1]),
        settings=load_settings(environ={}),
    )
    await writer.start()
    print("READY", flush=True)
    await asyncio.to_thread(sys.stdin.readline)
    await writer.close()
    print("CLOSED", flush=True)


asyncio.run(main())
"""
    with scratch_directory() as temporary:
        environment = os.environ.copy()
        source_root = str(Path("src").resolve())
        existing_pythonpath = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            source_root
            if not existing_pythonpath
            else source_root + os.pathsep + existing_pythonpath
        )
        process = subprocess.Popen(
            [sys.executable, "-c", child_script, str(temporary.resolve())],
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdin is not None
        assert process.stdout is not None
        assert process.stderr is not None
        contender = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
        )
        try:
            ready = await asyncio.wait_for(
                asyncio.to_thread(process.stdout.readline),
                timeout=10,
            )
            assert ready.strip() == "READY"

            with pytest.raises(FeatureParquetError, match="already claimed"):
                await contender.start()
            assert contender._root_claim is None

            process.stdin.write("\n")
            process.stdin.flush()
            returncode = await asyncio.wait_for(
                asyncio.to_thread(process.wait),
                timeout=10,
            )
            assert returncode == 0
            assert process.stdout.readline().strip() == "CLOSED"

            await contender.start()
            assert contender._root_claim is not None
            await contender.close()
            assert contender._root_claim is None
        finally:
            if contender._task is not None and not contender._closed:
                await contender.close()
            if process.poll() is None:
                process.stdin.close()
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(process.wait),
                        timeout=5,
                    )
                except TimeoutError:
                    process.kill()
                    await asyncio.to_thread(process.wait)
            process.stdout.close()
            process.stderr.close()


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
async def test_backpressure_callback_failure_rolls_back_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        value = single(Exchange.BINANCE, identifier=211)

        def fail_callback(_snapshot: FeatureSnapshot) -> None:
            raise RuntimeError("observer failed")

        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            on_backpressure=fail_callback,
        )
        await writer.start()
        original_full = writer._queue.full
        monkeypatch.setattr(writer._queue, "full", lambda: True)
        with pytest.raises(FeatureParquetError, match="observer failed"):
            await writer.write(value)
        monkeypatch.setattr(writer._queue, "full", original_full)
        writer._on_backpressure = None

        assert await writer.write(value) is FeatureWriteStatus.ACCEPTED
        await writer.close()
        assert audit_feature_tree(temporary).rows == 1


@pytest.mark.asyncio
async def test_cancelled_queued_write_rolls_back_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        config = settings()
        write_started = threading.Event()
        release_write = threading.Event()
        original_write = feature_storage._write_partition_file

        def block_first_write(
            root: Path,
            partition: tuple[str, str, str],
            envelopes: Sequence[feature_storage._FeatureEnvelope],
        ) -> Path:
            if not write_started.is_set():
                write_started.set()
                if not release_write.wait(timeout=5):
                    raise TimeoutError("test did not release feature write")
            return original_write(root, partition, envelopes)

        monkeypatch.setattr(
            feature_storage,
            "_write_partition_file",
            block_first_write,
        )
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=1,
        )
        await writer.start()
        await writer.write(single(Exchange.BINANCE, identifier=221))
        assert await asyncio.to_thread(write_started.wait, 2)
        await writer.write(single(Exchange.OKX, identifier=222))

        cancelled_value = single(Exchange.BINANCE, identifier=223)
        pending = asyncio.create_task(writer.write(cancelled_value))
        while writer.stats.backpressure_events == 0:
            await asyncio.sleep(0)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending

        release_write.set()
        await writer.close()
        monkeypatch.setattr(
            feature_storage,
            "_write_partition_file",
            original_write,
        )

        restarted = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
        )
        await restarted.start()
        assert (
            await restarted.write(cancelled_value)
            is FeatureWriteStatus.ACCEPTED
        )
        await restarted.close()
        assert audit_feature_tree(temporary).rows == 3


@pytest.mark.asyncio
async def test_duplicate_waiter_takes_over_cancelled_pending_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        write_started = threading.Event()
        release_write = threading.Event()
        original_write = feature_storage._write_partition_file

        def block_first_write(
            root: Path,
            partition: tuple[str, str, str],
            envelopes: Sequence[feature_storage._FeatureEnvelope],
        ) -> Path:
            if not write_started.is_set():
                write_started.set()
                if not release_write.wait(timeout=5):
                    raise TimeoutError("test did not release feature write")
            return original_write(root, partition, envelopes)

        monkeypatch.setattr(
            feature_storage,
            "_write_partition_file",
            block_first_write,
        )
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=1,
        )
        await writer.start()
        first = single(Exchange.BINANCE, identifier=225)
        second = single(Exchange.OKX, identifier=226)
        contested = single(Exchange.BINANCE, identifier=227)
        await writer.write(first)
        assert await asyncio.to_thread(write_started.wait, 2)
        await writer.write(second)

        owner = asyncio.create_task(writer.write(contested))
        while writer.stats.backpressure_events == 0:
            await asyncio.sleep(0)
        duplicate = asyncio.create_task(writer.write(contested))
        await asyncio.sleep(0)
        assert not duplicate.done()

        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert not duplicate.done()

        release_write.set()
        assert await duplicate is FeatureWriteStatus.ACCEPTED
        await writer.close()

        audit = audit_feature_tree(temporary)
        assert audit.rows == audit.unique_snapshot_ids == 3
        assert writer.stats.accepted_snapshots == writer.stats.written_snapshots == 3
        assert writer.stats.deduplicated_snapshots == 0


@pytest.mark.asyncio
async def test_cancelled_index_rebuild_finishes_before_start_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        rebuild_started = threading.Event()
        release_rebuild = threading.Event()
        original_rebuild = feature_storage._rebuild_feature_index

        def blocking_rebuild(root: Path, index_path: Path) -> None:
            rebuild_started.set()
            if not release_rebuild.wait(timeout=5):
                raise TimeoutError("test did not release feature index rebuild")
            original_rebuild(root, index_path)

        monkeypatch.setattr(
            feature_storage,
            "_rebuild_feature_index",
            blocking_rebuild,
        )
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
        )
        starting = asyncio.create_task(writer.start())
        assert await asyncio.to_thread(rebuild_started.wait, 2)
        starting.cancel()
        await asyncio.sleep(0)
        starting.cancel()
        await asyncio.sleep(0)

        assert not starting.done()
        assert writer._task is None
        assert writer._index is None
        assert writer._root_claim is not None
        contender = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
        )
        with pytest.raises(FeatureParquetError, match="already claimed"):
            await contender.start()
        assert contender._root_claim is None

        release_rebuild.set()
        with pytest.raises(asyncio.CancelledError):
            await starting

        assert writer._task is None
        assert writer._index is None
        assert writer._root_claim is None
        assert not list(temporary.glob(".*.tmp"))

        monkeypatch.setattr(
            feature_storage,
            "_rebuild_feature_index",
            original_rebuild,
        )
        await writer.start()
        assert writer._root_claim is not None
        await writer.close()
        assert writer._root_claim is None


@pytest.mark.asyncio
async def test_cancelled_close_waits_for_shared_feature_close_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        write_started = threading.Event()
        release_write = threading.Event()
        original_write = feature_storage._write_partition_file

        def block_write(
            root: Path,
            partition: tuple[str, str, str],
            envelopes: Sequence[feature_storage._FeatureEnvelope],
        ) -> Path:
            write_started.set()
            if not release_write.wait(timeout=5):
                raise TimeoutError("test did not release feature write")
            return original_write(root, partition, envelopes)

        monkeypatch.setattr(feature_storage, "_write_partition_file", block_write)
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
            batch_rows=1,
            flush_seconds=60,
            queue_capacity=2,
        )
        await writer.start()
        await writer.write(single(Exchange.BINANCE, identifier=224))
        assert await asyncio.to_thread(write_started.wait, 2)

        cancelled_close = asyncio.create_task(writer.close())
        while writer._close_task is None:
            await asyncio.sleep(0)
        saved_close_task = writer._close_task
        concurrent_close = asyncio.create_task(writer.close())
        cancelled_close.cancel()
        await asyncio.sleep(0)

        assert not cancelled_close.done()
        assert writer._close_task is saved_close_task
        assert writer._closed is False
        assert writer._task is not None and not writer._task.done()
        assert writer._index is not None
        assert writer._index.in_transaction is False
        assert writer._root_claim is not None

        release_write.set()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_close
        await concurrent_close

        assert writer._closed is True
        assert saved_close_task.done()
        assert writer._task.done()
        assert writer._index is None
        assert writer._root_claim is None
        assert writer.stats.accepted_snapshots == writer.stats.written_snapshots == 1
        assert audit_feature_tree(temporary).rows == 1


@pytest.mark.asyncio
async def test_many_records_use_bounded_index_transactions_and_restart_safely(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        monkeypatch.setattr(feature_storage, "_INDEX_TRANSACTION_ROWS", 17)
        config = settings()
        values = [
            single(
                Exchange.BINANCE if identifier % 2 else Exchange.OKX,
                identifier=identifier,
            )
            for identifier in range(2_000, 2_150)
        ]
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=500,
            flush_seconds=60,
            queue_capacity=200,
            deduplication_capacity=8,
        )
        await writer.start()
        assert writer._index is not None
        assert writer._index.in_transaction is False

        for value in values:
            assert await writer.write(value) is FeatureWriteStatus.ACCEPTED
        assert writer._index.in_transaction is False
        assert writer._index.execute(
            """
            SELECT COUNT(*)
            FROM feature_snapshot_index
            WHERE persistence_state = 'PENDING'
            """
        ).fetchone() == (len(values),)

        transaction_statements: list[str] = []
        writer._index.set_trace_callback(transaction_statements.append)
        await writer.close()
        assert writer._index is None
        assert transaction_statements.count("COMMIT") >= 10
        index = sqlite3.connect(temporary / ".feature-deduplication-v1.sqlite3")
        try:
            states = index.execute(
                """
                SELECT persistence_state, COUNT(*)
                FROM feature_snapshot_index
                GROUP BY persistence_state
                """
            ).fetchall()
        finally:
            index.close()
        assert states == [("COMMITTED", len(values))]

        restarted = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=500,
            deduplication_capacity=8,
        )
        await restarted.start()
        assert restarted._index is not None
        assert restarted._index.in_transaction is False
        assert (
            await restarted.write(values[0])
            is FeatureWriteStatus.DEDUPLICATED
        )
        assert restarted._index.in_transaction is False
        await restarted.close()
        assert audit_feature_tree(temporary).rows == len(values)


@pytest.mark.asyncio
async def test_rebuild_discards_crash_stale_sqlite_wal() -> None:
    with scratch_directory() as temporary:
        config = settings()
        truth = single(Exchange.BINANCE, identifier=228)
        stale = single(Exchange.OKX, identifier=229)
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
        )
        await writer.start()
        assert await writer.write(truth) is FeatureWriteStatus.ACCEPTED
        stale_envelope = writer._envelope(stale)
        await writer.close()

        index_path = temporary / ".feature-deduplication-v1.sqlite3"
        child = """
import os
import sqlite3
import sys

connection = sqlite3.connect(sys.argv[1])
connection.execute("PRAGMA journal_mode=WAL")
connection.execute("PRAGMA wal_autocheckpoint=0")
connection.execute("DELETE FROM feature_snapshot_index")
connection.execute(
    \"\"\"
    INSERT INTO feature_snapshot_index (
        feature_snapshot_id,
        payload_sha256,
        code_version,
        config_hash,
        persistence_state
    ) VALUES (?, ?, ?, ?, 'PENDING')
    \"\"\",
    tuple(sys.argv[2:6]),
)
connection.commit()
os._exit(0)
"""
        subprocess.run(
            [
                sys.executable,
                "-c",
                child,
                str(index_path.resolve()),
                str(stale.feature_snapshot_id),
                *stale_envelope.content_identity,
            ],
            check=True,
        )
        assert Path(f"{index_path}-wal").is_file()

        restarted = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
        )
        await restarted.start()
        assert (
            await restarted.write(truth)
            is FeatureWriteStatus.DEDUPLICATED
        )
        assert (
            await restarted.write(stale)
            is FeatureWriteStatus.ACCEPTED
        )
        await restarted.close()

        audit = audit_feature_tree(temporary)
        assert audit.rows == audit.unique_snapshot_ids == 2
        index = sqlite3.connect(index_path)
        try:
            rows = index.execute(
                """
                SELECT feature_snapshot_id, persistence_state
                FROM feature_snapshot_index
                ORDER BY feature_snapshot_id
                """
            ).fetchall()
        finally:
            index.close()
        assert rows == sorted(
            [
                (str(truth.feature_snapshot_id), "COMMITTED"),
                (str(stale.feature_snapshot_id), "COMMITTED"),
            ]
        )


@pytest.mark.asyncio
async def test_worker_replace_failure_is_propagated_and_recoverable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with scratch_directory() as temporary:
        config = settings()
        value = single(Exchange.BINANCE, identifier=231)
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
        )
        await writer.start()
        original_replace = feature_storage.os.replace

        def fail_parquet_replace(source: str, destination: str) -> None:
            if str(destination).endswith(".parquet"):
                raise OSError("simulated replace failure")
            original_replace(source, destination)

        monkeypatch.setattr(
            feature_storage.os,
            "replace",
            fail_parquet_replace,
        )
        assert await writer.write(value) is FeatureWriteStatus.ACCEPTED
        while writer.stats.last_error is None:
            await asyncio.sleep(0)
        with pytest.raises(FeatureParquetError, match="replace failure"):
            await writer.write(single(Exchange.OKX, identifier=232))
        with pytest.raises(FeatureParquetError, match="replace failure"):
            await writer.close()
        assert writer._root_claim is None
        assert not parquet_files(temporary)
        assert not list(temporary.rglob("*.tmp"))

        monkeypatch.setattr(
            feature_storage.os,
            "replace",
            original_replace,
        )
        restarted = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=config,
            batch_rows=1,
        )
        await restarted.start()
        assert await restarted.write(value) is FeatureWriteStatus.ACCEPTED
        await restarted.close()
        assert audit_feature_tree(temporary).rows == 1


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


def test_reader_and_audit_reject_nonexistent_or_uninitialized_roots() -> None:
    with scratch_directory() as temporary:
        missing = temporary / "missing"
        with pytest.raises(ValueError, match="does not exist"):
            list(FeatureParquetReader(missing).iter_records())
        with pytest.raises(ValueError, match="missing feature_schema=v1"):
            audit_feature_tree(temporary)


def test_reader_rejects_empty_feature_schema_tree() -> None:
    with scratch_directory() as temporary:
        (temporary / "feature_schema=v1").mkdir()
        with pytest.raises(ValueError, match="no Parquet data"):
            list(FeatureParquetReader(temporary).iter_records())


@pytest.mark.asyncio
async def test_writer_rejects_a_root_that_already_contains_raw_parquet() -> None:
    with scratch_directory() as temporary:
        raw_partition = temporary / "date=2026-07-27" / "exchange=BINANCE"
        raw_partition.mkdir(parents=True)
        pq.write_table(
            pa.table({"raw_payload": [b"{}"]}),
            raw_partition / "raw.parquet",
        )
        writer = AsyncFeatureParquetWriter(
            root_path=temporary,
            settings=settings(),
        )

        with pytest.raises(FeatureParquetError, match="outside schema tree"):
            await writer.start()
        assert writer._root_claim is None


@pytest.mark.asyncio
async def test_reader_rejects_unknown_schema_directory() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        (temporary / "feature_schema=v2").mkdir()

        with pytest.raises(ValueError, match="unknown feature schema"):
            list(FeatureParquetReader(temporary).iter_records())


@pytest.mark.asyncio
async def test_reader_rejects_parquet_outside_schema_tree() -> None:
    with scratch_directory() as temporary:
        await write_tree(temporary, [single(Exchange.BINANCE)])
        shutil.copyfile(parquet_files(temporary)[0], temporary / "stray.parquet")

        with pytest.raises(ValueError, match="outside schema tree"):
            audit_feature_tree(temporary)


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
        empty_audit = audit_feature_tree(
            temporary,
            filters=FeatureScanFilter(schema_versions=frozenset({2})),
        )
        assert empty_audit.rows == empty_audit.files == 0
        assert empty_audit.content_digest == "0" * 64


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
        assert audit.code_versions == (__version__,)
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

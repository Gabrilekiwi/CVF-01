"""Collection orchestration without external network access."""

from __future__ import annotations

import asyncio
import shutil
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

import pytest

from cvf.collector import MarketDataCollector
from cvf.config import Settings, load_settings


@contextmanager
def scratch_directory():
    path = Path("data/processed") / f"collector-{uuid4().hex[:8]}"
    path.mkdir()
    try:
        yield path
    finally:
        shutil.rmtree(path)


def collection_settings() -> Settings:
    settings = load_settings(environ={})
    exchanges = settings.exchanges.model_copy(
        update={
            "binance": settings.exchanges.binance.model_copy(
                update={"enabled": False}
            ),
            "okx": settings.exchanges.okx.model_copy(update={"enabled": False}),
        }
    )
    return settings.model_copy(update={"exchanges": exchanges})


@pytest.mark.asyncio
async def test_duration_stops_and_closes_empty_collection_cleanly() -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)
        stop_event = asyncio.Event()

        summary = await collector.run(
            stop_event=stop_event,
            duration_seconds=0.01,
        )

        assert stop_event.is_set()
        assert summary.output_path == output.resolve()
        assert summary.duration_seconds >= 0
        assert summary.normalized_event_counts == {}
        assert summary.parquet.written_records == 0
        assert summary.parquet.last_error is None


@pytest.mark.asyncio
async def test_rejects_nonpositive_duration_before_starting() -> None:
    with scratch_directory() as output:
        collector = MarketDataCollector(collection_settings(), output_path=output)

        with pytest.raises(ValueError, match="positive"):
            await collector.run(
                stop_event=asyncio.Event(),
                duration_seconds=0,
            )

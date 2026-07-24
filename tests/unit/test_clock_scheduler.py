"""Deterministic replay clock and UTC decision-boundary tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from cvf.clock import DecisionScheduler, ReplayClock, TickKind

START = datetime(2026, 7, 24, 12, 0, 0, 500_000, tzinfo=UTC)


@pytest.mark.asyncio
async def test_replay_clock_advances_without_wall_clock_waiting() -> None:
    clock = ReplayClock(START)
    target = START + timedelta(hours=1)
    await clock.sleep_until(target)
    assert clock.now() == target

    with pytest.raises(ValueError, match="backwards"):
        clock.advance_to(START)


def test_scheduler_emits_all_boundaries_with_stable_tie_order() -> None:
    scheduler = DecisionScheduler(
        start=START,
        feature_interval=timedelta(seconds=1),
        signal_interval=timedelta(seconds=5),
    )

    ticks = scheduler.advance_to(
        datetime(2026, 7, 24, 12, 0, 10, 200_000, tzinfo=UTC)
    )

    feature_ticks = [tick for tick in ticks if tick.kind is TickKind.FEATURE]
    signal_ticks = [tick for tick in ticks if tick.kind is TickKind.SIGNAL]
    assert len(feature_ticks) == 10
    assert len(signal_ticks) == 2
    at_five = [
        tick.kind
        for tick in ticks
        if tick.timestamp == datetime(2026, 7, 24, 12, 0, 5, tzinfo=UTC)
    ]
    assert at_five == [TickKind.FEATURE, TickKind.SIGNAL]


def test_scheduler_rejects_time_travel() -> None:
    scheduler = DecisionScheduler(
        start=START,
        feature_interval=timedelta(seconds=1),
        signal_interval=timedelta(seconds=5),
    )
    with pytest.raises(ValueError, match="backwards"):
        scheduler.advance_to(START - timedelta(microseconds=1))

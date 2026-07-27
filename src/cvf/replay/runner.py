"""Replay raw records through deterministic time and normalized-event boundaries."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from cvf.clock.replay import ReplayClock
from cvf.clock.scheduler import DecisionScheduler, DecisionTick
from cvf.pipeline.event_bus import NormalizedEventBus
from cvf.replay.normalizer import RawRecordNormalizer
from cvf.replay.ordering import ReplayOrder, replay_timestamp
from cvf.storage.raw import RawMarketRecord

type TickSink = Callable[[DecisionTick], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    raw_records: int
    normalized_events: int
    skipped_records: int
    event_counts: dict[str, int]
    connection_generations: dict[str, int]
    started_at: datetime | None
    finished_at: datetime | None


class ReplayRunner:
    """Own the virtual clock and publish replayed events to the live event bus."""

    def __init__(
        self,
        *,
        event_bus: NormalizedEventBus,
        normalizer: RawRecordNormalizer | None = None,
        scheduler: DecisionScheduler | None = None,
        tick_sink: TickSink | None = None,
        order: ReplayOrder = ReplayOrder.EVENT_TIME,
        speed: float = 0,
    ) -> None:
        if speed < 0:
            raise ValueError("replay speed cannot be negative")
        self._event_bus = event_bus
        self._normalizer = normalizer or RawRecordNormalizer()
        self._scheduler = scheduler
        self._tick_sink = tick_sink
        self._order = order
        self._speed = speed

    async def run(self, records: Iterable[RawMarketRecord]) -> ReplaySummary:
        await self._event_bus.start()
        raw_count = 0
        normalized_count = 0
        skipped_count = 0
        event_counts: Counter[str] = Counter()
        generations: dict[str, int] = {}
        started_at: datetime | None = None
        finished_at: datetime | None = None
        clock: ReplayClock | None = None
        previous_timestamp: datetime | None = None

        async def emit_ticks(target: datetime) -> None:
            if self._scheduler is None:
                return
            await self._event_bus.drain()
            for tick in self._scheduler.advance_to(target):
                if self._tick_sink is not None:
                    await self._tick_sink(tick)

        try:
            for record in records:
                if record.channel == "instrument_metadata":
                    raw_count += 1
                    generation_key = (
                        f"{record.exchange.value}:{record.channel}:{record.symbol}"
                    )
                    generations[generation_key] = record.connection_generation
                    self._normalizer.normalize(record)
                    skipped_count += 1
                    continue
                timestamp = replay_timestamp(record, self._order)
                if clock is None:
                    clock = ReplayClock(timestamp)
                    started_at = timestamp
                elif previous_timestamp is not None and timestamp > previous_timestamp:
                    await emit_ticks(timestamp - timedelta(microseconds=1))
                    if self._speed > 0:
                        delay = (
                            timestamp - previous_timestamp
                        ).total_seconds() / self._speed
                        if delay > 0:
                            await asyncio.sleep(delay)
                clock.advance_to(timestamp)
                previous_timestamp = timestamp
                finished_at = timestamp
                raw_count += 1
                generation_key = f"{record.exchange.value}:{record.channel}:{record.symbol}"
                generations[generation_key] = record.connection_generation
                events = self._normalizer.normalize(record)
                if not events:
                    skipped_count += 1
                for event in events:
                    await self._event_bus.publish(event)
                    normalized_count += 1
                    event_counts[event.event_type.value] += 1
            if previous_timestamp is not None:
                await emit_ticks(previous_timestamp)
        finally:
            await self._event_bus.close()
        return ReplaySummary(
            raw_records=raw_count,
            normalized_events=normalized_count,
            skipped_records=skipped_count,
            event_counts=dict(event_counts),
            connection_generations=generations,
            started_at=None if started_at is None else started_at.astimezone(UTC),
            finished_at=None if finished_at is None else finished_at.astimezone(UTC),
        )

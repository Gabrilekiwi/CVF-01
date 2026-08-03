"""Replay raw records through deterministic time and normalized-event boundaries."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol, runtime_checkable

from cvf.clock.replay import ReplayClock
from cvf.clock.scheduler import DecisionScheduler, DecisionTick
from cvf.normalization.common import NormalizedMarketEvent
from cvf.pipeline.event_bus import NormalizedEventBus
from cvf.replay.normalizer import RawRecordNormalizer
from cvf.replay.ordering import ReplayOrder, replay_timestamp
from cvf.storage.raw import (
    FEATURE_TIMELINE_END_MESSAGE_KIND,
    RawMarketRecord,
    feature_timeline_end_timestamp,
)

type TickSink = Callable[[DecisionTick], Awaitable[None]]
type EventSink = Callable[[NormalizedMarketEvent], Awaitable[None]]
type FinishSink = Callable[[datetime | None], Awaitable[None]]


@runtime_checkable
class _ClosableIterator(Protocol):
    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ReplaySummary:
    raw_records: int
    normalized_events: int
    skipped_records: int
    event_counts: dict[str, int]
    connection_generations: dict[str, int]
    started_at: datetime | None
    finished_at: datetime | None
    feature_timeline_end_at: datetime | None = None
    feature_timeline_end_records: int = 0


class ReplayRunner:
    """Own the virtual clock and publish replayed events to the live event bus."""

    def __init__(
        self,
        *,
        event_bus: NormalizedEventBus,
        normalizer: RawRecordNormalizer | None = None,
        scheduler: DecisionScheduler | None = None,
        tick_sink: TickSink | None = None,
        event_sink: EventSink | None = None,
        finish_sink: FinishSink | None = None,
        order: ReplayOrder = ReplayOrder.EVENT_TIME,
        speed: float = 0,
    ) -> None:
        if speed < 0:
            raise ValueError("replay speed cannot be negative")
        if finish_sink is not None and event_sink is None:
            raise ValueError("replay finish_sink requires an event_sink")
        if event_sink is not None and (scheduler is not None or tick_sink is not None):
            raise ValueError(
                "external replay event timeline cannot use runner tick scheduling"
            )
        self._event_bus = event_bus
        self._normalizer = normalizer or RawRecordNormalizer()
        self._scheduler = scheduler
        self._tick_sink = tick_sink
        self._event_sink = event_sink
        self._finish_sink = finish_sink
        self._order = order
        self._speed = speed

    async def run(self, records: Iterable[RawMarketRecord]) -> ReplaySummary:
        iterator = iter(records)
        raw_count = 0
        normalized_count = 0
        skipped_count = 0
        event_counts: Counter[str] = Counter()
        generations: dict[str, int] = {}
        started_at: datetime | None = None
        finished_at: datetime | None = None
        normalized_started_at: datetime | None = None
        normalized_finished_at: datetime | None = None
        feature_timeline_end_at: datetime | None = None
        feature_timeline_end_records = 0
        clock: ReplayClock | None = None
        previous_timestamp: datetime | None = None
        primary_error: BaseException | None = None
        cleanup_errors: list[BaseException] = []

        async def emit_ticks(target: datetime) -> None:
            if self._scheduler is None:
                return
            if not self._scheduler.has_due_tick(target):
                return
            await self._event_bus.drain()
            for tick in self._scheduler.advance_to(target):
                if self._tick_sink is not None:
                    await self._tick_sink(tick)

        try:
            await self._event_bus.start()
            for record in iterator:
                if (
                    feature_timeline_end_at is not None
                    and record.message_kind != FEATURE_TIMELINE_END_MESSAGE_KIND
                ):
                    raise ValueError(
                        "raw record appears after the feature-timeline end marker"
                    )
                if record.channel == "instrument_metadata":
                    raw_count += 1
                    generation_key = (
                        f"{record.exchange.value}:{record.channel}:{record.symbol}"
                    )
                    generations[generation_key] = record.connection_generation
                    self._normalizer.normalize(record)
                    skipped_count += 1
                    continue
                marker_timestamp = feature_timeline_end_timestamp(record)
                if marker_timestamp is not None:
                    raw_count += 1
                    generation_key = (
                        f"{record.exchange.value}:{record.channel}:{record.symbol}"
                    )
                    generations[generation_key] = record.connection_generation
                    feature_timeline_end_records += 1
                    if feature_timeline_end_records > 1:
                        raise ValueError(
                            "replay input contains multiple feature-timeline end markers"
                        )
                    feature_timeline_end_at = marker_timestamp
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
                    normalized_at = event.local_receive_timestamp.astimezone(UTC)
                    normalized_started_at = (
                        normalized_at
                        if normalized_started_at is None
                        else min(normalized_started_at, normalized_at)
                    )
                    normalized_finished_at = (
                        normalized_at
                        if normalized_finished_at is None
                        else max(normalized_finished_at, normalized_at)
                    )
                    if self._event_sink is None:
                        await self._event_bus.publish(event)
                    else:
                        await self._event_sink(event)
                    normalized_count += 1
                    event_counts[event.event_type.value] += 1
            if previous_timestamp is not None:
                await emit_ticks(previous_timestamp)
            if (
                feature_timeline_end_at is not None
                and normalized_finished_at is not None
                and normalized_finished_at > feature_timeline_end_at
            ):
                raise ValueError(
                    "normalized journal event exceeds its feature-timeline "
                    "end marker"
                )
            if self._finish_sink is not None:
                await self._finish_sink(feature_timeline_end_at)
        except BaseException as exc:
            primary_error = exc
        finally:
            if isinstance(iterator, _ClosableIterator):
                try:
                    iterator.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            try:
                await self._event_bus.close()
            except BaseException as exc:
                if exc is not primary_error:
                    cleanup_errors.append(exc)
        errors = [
            error
            for error in (primary_error, *cleanup_errors)
            if error is not None
        ]
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise BaseExceptionGroup(
                "replay and cleanup failures",
                errors,
            )
        return ReplaySummary(
            raw_records=raw_count,
            normalized_events=normalized_count,
            skipped_records=skipped_count,
            event_counts=dict(event_counts),
            connection_generations=generations,
            started_at=(
                normalized_started_at
                if self._event_sink is not None
                else (
                    None
                    if started_at is None
                    else started_at.astimezone(UTC)
                )
            ),
            finished_at=(
                (
                    feature_timeline_end_at
                    if feature_timeline_end_at is not None
                    else normalized_finished_at
                )
                if self._event_sink is not None
                else (
                    None
                    if finished_at is None
                    else finished_at.astimezone(UTC)
                )
            ),
            feature_timeline_end_at=feature_timeline_end_at,
            feature_timeline_end_records=feature_timeline_end_records,
        )

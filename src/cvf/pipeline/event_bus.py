"""Ordered, bounded fan-out for normalized market events."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Final

from cvf.normalization.common import NormalizedMarketEvent

type EventConsumer = Callable[[NormalizedMarketEvent], Awaitable[None]]

_STOP: Final = object()


class EventBusError(RuntimeError):
    """Raised when the bus lifecycle is invalid or a consumer fails."""


@dataclass(frozen=True, slots=True)
class ConsumerStats:
    """Point-in-time metrics for one independent consumer."""

    published_events: int
    processed_events: int
    backpressure_events: int
    queue_depth: int
    maximum_queue_depth: int
    last_processing_latency_ms: float | None
    maximum_processing_latency_ms: float | None
    last_error: str | None


class _ConsumerRuntime:
    def __init__(self, name: str, handler: EventConsumer, queue_capacity: int) -> None:
        self.name = name
        self.handler = handler
        self.queue: asyncio.Queue[NormalizedMarketEvent | object] = asyncio.Queue(
            maxsize=queue_capacity
        )
        self.task: asyncio.Task[None] | None = None
        self.published_events = 0
        self.processed_events = 0
        self.backpressure_events = 0
        self.maximum_queue_depth = 0
        self.last_processing_latency_ms: float | None = None
        self.maximum_processing_latency_ms: float | None = None
        self.last_error: str | None = None

    @property
    def stats(self) -> ConsumerStats:
        return ConsumerStats(
            published_events=self.published_events,
            processed_events=self.processed_events,
            backpressure_events=self.backpressure_events,
            queue_depth=self.queue.qsize(),
            maximum_queue_depth=self.maximum_queue_depth,
            last_processing_latency_ms=self.last_processing_latency_ms,
            maximum_processing_latency_ms=self.maximum_processing_latency_ms,
            last_error=self.last_error,
        )

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        try:
            while True:
                item = await self.queue.get()
                if item is _STOP:
                    return
                assert not isinstance(item, object) or hasattr(item, "event_type")
                started = loop.time()
                await self.handler(item)  # type: ignore[arg-type]
                latency_ms = (loop.time() - started) * 1000.0
                self.last_processing_latency_ms = latency_ms
                current_maximum = self.maximum_processing_latency_ms
                self.maximum_processing_latency_ms = (
                    latency_ms if current_maximum is None else max(current_maximum, latency_ms)
                )
                self.processed_events += 1
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            raise


class NormalizedEventBus:
    """Fan each event out to ordered per-consumer workers with bounded queues."""

    def __init__(self, *, default_queue_capacity: int = 10_000) -> None:
        if default_queue_capacity < 1:
            raise ValueError("default_queue_capacity must be positive")
        self._default_queue_capacity = default_queue_capacity
        self._consumers: dict[str, _ConsumerRuntime] = {}
        self._started = False
        self._closed = False

    def register(
        self,
        name: str,
        handler: EventConsumer,
        *,
        queue_capacity: int | None = None,
    ) -> None:
        """Register a consumer before the bus starts."""

        if self._started or self._closed:
            raise EventBusError("consumers can only be registered before the bus starts")
        if not name or name.isspace():
            raise ValueError("consumer name cannot be empty")
        if name in self._consumers:
            raise ValueError(f"duplicate consumer name: {name}")
        capacity = queue_capacity or self._default_queue_capacity
        if capacity < 1:
            raise ValueError("queue_capacity must be positive")
        self._consumers[name] = _ConsumerRuntime(name, handler, capacity)

    @property
    def stats(self) -> dict[str, ConsumerStats]:
        return {name: runtime.stats for name, runtime in self._consumers.items()}

    async def start(self) -> None:
        if self._closed:
            raise EventBusError("cannot restart a closed event bus")
        if self._started:
            return
        self._started = True
        for runtime in self._consumers.values():
            runtime.task = asyncio.create_task(
                runtime.run(),
                name=f"normalized-event-consumer-{runtime.name}",
            )

    def _raise_consumer_failure(self) -> None:
        for runtime in self._consumers.values():
            task = runtime.task
            if task is None or not task.done() or task.cancelled():
                continue
            error = task.exception()
            if error is not None:
                raise EventBusError(
                    f"normalized event consumer {runtime.name!r} failed: {error}"
                ) from error

    async def _put_or_failure(
        self,
        runtime: _ConsumerRuntime,
        item: NormalizedMarketEvent | object,
    ) -> None:
        task = runtime.task
        if task is None:
            raise EventBusError("event bus is not running")
        if not runtime.queue.full():
            runtime.queue.put_nowait(item)
            return
        runtime.backpressure_events += 1
        putter = asyncio.create_task(runtime.queue.put(item))
        done, _ = await asyncio.wait({putter, task}, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            if not putter.done():
                putter.cancel()
                with suppress(asyncio.CancelledError):
                    await putter
            self._raise_consumer_failure()
            if item is _STOP and not putter.cancelled() and putter.exception() is None:
                return
            raise EventBusError(f"normalized event consumer {runtime.name!r} stopped")
        await putter

    async def publish(self, event: NormalizedMarketEvent) -> None:
        """Publish one event to every consumer, applying backpressure rather than dropping."""

        if not self._started:
            raise EventBusError("start the event bus before publishing")
        if self._closed:
            raise EventBusError("cannot publish after event bus close")
        self._raise_consumer_failure()
        for runtime in self._consumers.values():
            await self._put_or_failure(runtime, event)
            runtime.published_events += 1
            runtime.maximum_queue_depth = max(
                runtime.maximum_queue_depth,
                runtime.queue.qsize(),
            )

    async def close(self) -> None:
        """Drain all queues and surface any consumer failure."""

        if self._closed:
            return
        self._closed = True
        if not self._started:
            return
        failure: EventBusError | None = None
        try:
            self._raise_consumer_failure()
        except EventBusError as exc:
            failure = exc
        for runtime in self._consumers.values():
            task = runtime.task
            if task is None or task.done():
                continue
            await self._put_or_failure(runtime, _STOP)
        results = await asyncio.gather(
            *(runtime.task for runtime in self._consumers.values() if runtime.task is not None),
            return_exceptions=True,
        )
        if failure is not None:
            raise failure
        for runtime, result in zip(self._consumers.values(), results, strict=True):
            if isinstance(result, BaseException):
                raise EventBusError(
                    f"normalized event consumer {runtime.name!r} failed: {result}"
                ) from result

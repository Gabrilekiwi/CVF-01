"""Ordered, bounded fan-out for normalized market events."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Final

from cvf.normalization.common import NormalizedMarketEvent
from cvf.utils.async_lifecycle import await_task_completion

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
                try:
                    if item is _STOP:
                        return
                    assert not isinstance(item, object) or hasattr(item, "event_type")
                    started = loop.time()
                    await self.handler(item)  # type: ignore[arg-type]
                    latency_ms = (loop.time() - started) * 1000.0
                    self.last_processing_latency_ms = latency_ms
                    current_maximum = self.maximum_processing_latency_ms
                    self.maximum_processing_latency_ms = (
                        latency_ms
                        if current_maximum is None
                        else max(current_maximum, latency_ms)
                    )
                    self.processed_events += 1
                finally:
                    self.queue.task_done()
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
        self._close_task: asyncio.Task[None] | None = None

    def register(
        self,
        name: str,
        handler: EventConsumer,
        *,
        queue_capacity: int | None = None,
    ) -> None:
        """Register a consumer before the bus starts."""

        if self._started or self._closed or self._close_task is not None:
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
        if self._close_task is not None:
            raise EventBusError("cannot start a closing event bus")
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
        try:
            done, _ = await asyncio.wait(
                {putter, task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.CancelledError:
            if not putter.done():
                putter.cancel()
                with suppress(asyncio.CancelledError):
                    await putter
            raise
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
        if self._closed or self._close_task is not None:
            raise EventBusError("cannot publish after event bus close")
        self._raise_consumer_failure()
        for runtime in self._consumers.values():
            await self._put_or_failure(runtime, event)
            runtime.published_events += 1
            runtime.maximum_queue_depth = max(
                runtime.maximum_queue_depth,
                runtime.queue.qsize(),
            )

    async def drain(self) -> None:
        """Wait until every published event is processed or surface a consumer failure."""

        if not self._started:
            raise EventBusError("event bus is not running")
        if self._closed or self._close_task is not None:
            raise EventBusError("cannot drain a closed event bus")
        self._raise_consumer_failure()
        joins = {
            asyncio.create_task(runtime.queue.join())
            for runtime in self._consumers.values()
        }
        consumers = {
            runtime.task
            for runtime in self._consumers.values()
            if runtime.task is not None
        }
        pending_joins = set(joins)
        try:
            while pending_joins:
                done, _ = await asyncio.wait(
                    pending_joins | consumers,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                stopped_consumers = done & consumers
                if stopped_consumers:
                    self._raise_consumer_failure()
                    raise EventBusError("normalized event consumer stopped during drain")
                pending_joins.difference_update(done)
        finally:
            for join in pending_joins:
                join.cancel()
            if pending_joins:
                await asyncio.gather(*pending_joins, return_exceptions=True)
        self._raise_consumer_failure()

    async def close(self) -> None:
        """Drain all queues and surface any consumer failure."""

        close_task = self._close_task
        if close_task is None:
            close_task = asyncio.create_task(
                self._close_lifecycle(),
                name="normalized-event-bus-close",
            )
            self._close_task = close_task
        await await_task_completion(close_task)

    async def _close_lifecycle(self) -> None:
        if not self._started:
            self._closed = True
            return
        failures: list[BaseException] = []
        try:
            self._raise_consumer_failure()
        except EventBusError as exc:
            failures.append(exc)
        try:
            for runtime in self._consumers.values():
                task = runtime.task
                if task is None or task.done():
                    continue
                try:
                    await self._put_or_failure(runtime, _STOP)
                except BaseException as exc:
                    failures.append(exc)
            runtimes = [
                runtime
                for runtime in self._consumers.values()
                if runtime.task is not None
            ]
            results = await asyncio.gather(
                *(runtime.task for runtime in runtimes if runtime.task is not None),
                return_exceptions=True,
            )
            for runtime, result in zip(runtimes, results, strict=True):
                if isinstance(result, BaseException) and all(
                    result is not failure and failure.__cause__ is not result
                    for failure in failures
                ):
                    failures.append(
                        EventBusError(
                            "normalized event consumer "
                            f"{runtime.name!r} failed: {result}"
                        )
                    )
        finally:
            self._closed = True
        if len(failures) == 1:
            raise failures[0]
        if failures:
            raise BaseExceptionGroup(
                "normalized event bus close failures",
                failures,
            )

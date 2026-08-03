"""Cancellation-safe helpers for shared asynchronous lifecycle tasks."""

from __future__ import annotations

import asyncio


async def await_task_completion[T](task: asyncio.Future[T]) -> T:
    """Finish ``task`` before propagating any cancellation of the waiter.

    Repeated ``Task.cancel()`` calls are absorbed while the shared inner task is
    still running. An inner failure remains more important than cancellation
    because callers must not lose a lifecycle error.
    """

    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError as exc:
            if task.done() and task.cancelled():
                raise
            if cancellation is None:
                cancellation = exc
        else:
            if cancellation is not None:
                raise cancellation
            return result
    if task.cancelled():
        return await task
    error = task.exception()
    if error is not None:
        raise error
    if cancellation is not None:
        raise cancellation
    return task.result()

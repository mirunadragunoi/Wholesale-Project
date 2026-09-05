"""Shared consumer-loop and shutdown helpers.

The engine and egress processes both do the same thing structurally: pull
batches from a queue, do something, acknowledge. ``consume_loop`` captures that
so each process only supplies a per-batch handler.

The handler receives a batch of ``ReceivedMessage`` and returns the handles it
wants acknowledged (deleted). Handles it omits are left for redelivery after the
visibility timeout.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable, Sequence

from relay.common.logging import get_logger
from relay.queues.base import Consumer, ReceivedMessage

_log = get_logger("worker")

BatchHandler = Callable[[list[ReceivedMessage]], Awaitable[Sequence[str]]]


async def consume_loop(
    consumer: Consumer,
    handler: BatchHandler,
    concurrency: int,
    stop: asyncio.Event,
) -> None:
    async def worker(worker_id: int) -> None:
        while not stop.is_set():
            batch = await consumer.receive(max_messages=10)
            if not batch:
                continue
            try:
                handles = await handler(batch)
            except Exception:
                _log.exception("batch_handler_failed", worker_id=worker_id, size=len(batch))
                continue  # leave messages for redelivery
            if handles:
                await consumer.delete(list(handles))

    await asyncio.gather(*(worker(i) for i in range(concurrency)))


def install_shutdown(stop: asyncio.Event) -> None:
    """Set ``stop`` on SIGINT/SIGTERM.

    Uses the asyncio signal handlers where available (POSIX); falls back to
    ``signal.signal`` on Windows, where ``add_signal_handler`` is not supported.
    """
    loop = asyncio.get_running_loop()

    def _handler() -> None:
        _log.info("shutdown_signal_received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handler)
        except NotImplementedError:
            # Windows: no add_signal_handler; use the classic handler.
            signal.signal(sig, lambda *_: stop.set())

"""In-memory queue backend, for tests and for running the pipeline without
Docker/AWS.

Backed by ``asyncio.Queue`` with a bounded size so it exercises the same
backpressure behaviour as the real path: when a queue is full, ``publish``
blocks (callers turn that into a 429 upstream) rather than buffering without
limit.

Delivery is at-most-once here (a received message is already removed from the
queue, so ``delete`` is a no-op). That is sufficient for the pass-through POC
and its tests; the SQS backend provides the real visibility-timeout semantics.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence

from relay.common.message import Message
from relay.queues.base import Consumer, Producer, QueueBackend, ReceivedMessage

_DEFAULT_MAXSIZE = 10_000


class _Channel:
    __slots__ = ("queue",)

    def __init__(self, maxsize: int) -> None:
        self.queue: asyncio.Queue[tuple[Message, float]] = asyncio.Queue(maxsize=maxsize)


class MemoryProducer(Producer):
    def __init__(self, channel: _Channel) -> None:
        self._channel = channel

    async def publish(self, messages: Sequence[Message]) -> None:
        now = time.time()
        for msg in messages:
            await self._channel.queue.put((msg, now))

    async def close(self) -> None:
        return None


class MemoryConsumer(Consumer):
    def __init__(self, channel: _Channel, poll_timeout: float = 1.0) -> None:
        self._channel = channel
        self._poll_timeout = poll_timeout

    async def receive(self, max_messages: int) -> list[ReceivedMessage]:
        out: list[ReceivedMessage] = []
        try:
            msg, sent_at = await asyncio.wait_for(
                self._channel.queue.get(), timeout=self._poll_timeout
            )
        except TimeoutError:
            return out
        out.append(ReceivedMessage(msg, handle="", sent_at=sent_at))
        while len(out) < max_messages:
            try:
                msg, sent_at = self._channel.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            out.append(ReceivedMessage(msg, handle="", sent_at=sent_at))
        return out

    async def delete(self, handles: Sequence[str]) -> None:
        return None

    async def close(self) -> None:
        return None


class MemoryBackend(QueueBackend):
    def __init__(self, maxsize: int = _DEFAULT_MAXSIZE) -> None:
        self._maxsize = maxsize
        self._channels: dict[str, _Channel] = {}

    def _channel(self, queue: str) -> _Channel:
        channel = self._channels.get(queue)
        if channel is None:
            channel = _Channel(self._maxsize)
            self._channels[queue] = channel
        return channel

    async def producer(self, queue: str) -> Producer:
        return MemoryProducer(self._channel(queue))

    async def consumer(self, queue: str) -> Consumer:
        return MemoryConsumer(self._channel(queue))

    async def close(self) -> None:
        self._channels.clear()

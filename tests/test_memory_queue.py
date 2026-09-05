from __future__ import annotations

import asyncio
import time

import pytest

from relay.common.message import Message
from relay.queues.memory import MemoryBackend


def _msg(i: int) -> Message:
    return Message(
        id=f"id{i:06d}",
        to="+40712345678",
        text=f"msg {i}",
        sender=None,
        source="http",
        received_at=time.time(),
        attributes={},
    )


async def test_publish_receive_roundtrip() -> None:
    backend = MemoryBackend()
    producer = await backend.producer("q")
    consumer = await backend.consumer("q")

    await producer.publish([_msg(i) for i in range(25)])

    received: list[Message] = []
    while len(received) < 25:
        batch = await consumer.receive(max_messages=10)
        received.extend(r.message for r in batch)
    assert [m.id for m in received] == [f"id{i:06d}" for i in range(25)]
    await backend.close()


async def test_receive_times_out_when_empty() -> None:
    backend = MemoryBackend()
    consumer = await backend.consumer("q")
    consumer._poll_timeout = 0.05  # type: ignore[attr-defined]
    start = time.monotonic()
    batch = await consumer.receive(max_messages=10)
    assert batch == []
    assert time.monotonic() - start < 1.0
    await backend.close()


async def test_bounded_queue_applies_backpressure() -> None:
    backend = MemoryBackend(maxsize=5)
    producer = await backend.producer("q")
    # Filling beyond maxsize must block, not buffer without limit.
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(producer.publish([_msg(i) for i in range(10)]), timeout=0.2)
    await backend.close()

from __future__ import annotations

import time

from relay.common.message import Message
from relay.engine.pipeline import Pipeline, passthrough


def _msg() -> Message:
    return Message(
        id="x",
        to="+40712345678",
        text="hi",
        sender=None,
        source="http",
        received_at=time.time(),
        attributes={},
    )


async def test_passthrough_forwards_unchanged() -> None:
    pipeline = Pipeline([passthrough])
    msg = _msg()
    assert await pipeline.process(msg) is msg


async def test_dropping_stage_stops_pipeline() -> None:
    async def drop(_: Message) -> Message | None:
        return None

    called = False

    async def after(m: Message) -> Message | None:
        nonlocal called
        called = True
        return m

    pipeline = Pipeline([passthrough, drop, after])
    assert await pipeline.process(_msg()) is None
    assert not called, "stages after a drop must not run"

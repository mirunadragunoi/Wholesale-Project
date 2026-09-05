"""The processing pipeline.

A pipeline is an ordered list of stages. Each stage is
``async def stage(msg) -> Message | None``; returning ``None`` drops the message
(it will not reach egress). For the POC there is exactly one stage that does
nothing — the point is that adding real logic later is a single line in the
stage list, not a restructuring.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

from relay.common.message import Message

Stage = Callable[[Message], Awaitable[Message | None]]


async def passthrough(msg: Message) -> Message | None:
    """No-op stage: forwards every message unchanged."""
    return msg


class Pipeline:
    def __init__(self, stages: list[Stage]) -> None:
        self._stages = stages

    async def process(self, msg: Message) -> Message | None:
        current: Message | None = msg
        for stage in self._stages:
            assert current is not None
            current = await stage(current)
            if current is None:
                return None
        return current


def default_pipeline() -> Pipeline:
    # Add future stages here — e.g. [passthrough, rate_limit, normalize, route].
    return Pipeline([passthrough])

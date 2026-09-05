"""Token-bucket rate shaper.

Used to cap aggregate TPS per egress connector (per provider). A rate of 0 means
unlimited (the shaper becomes a no-op). Acquisitions are serialized so the
aggregate emission rate is respected across all concurrent workers.
"""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    def __init__(self, rate: float, capacity: float | None = None) -> None:
        self._rate = rate
        self._capacity = capacity if capacity is not None else max(rate, 1.0)
        self._tokens = self._capacity
        self._updated = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def unlimited(self) -> bool:
        return self._rate <= 0

    async def acquire(self, n: float = 1.0) -> None:
        if self.unlimited:
            return
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity, self._tokens + (now - self._updated) * self._rate
                )
                self._updated = now
                if self._tokens >= n:
                    self._tokens -= n
                    return
                await asyncio.sleep((n - self._tokens) / self._rate)

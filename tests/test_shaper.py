from __future__ import annotations

import time

from relay.egress.shaper import TokenBucket


async def test_unlimited_is_noop() -> None:
    bucket = TokenBucket(0)
    assert bucket.unlimited
    start = time.monotonic()
    for _ in range(1000):
        await bucket.acquire()
    assert time.monotonic() - start < 0.5


async def test_limited_rate_enforces_delay() -> None:
    # 50 tokens/s, capacity 5: after the initial burst, further tokens are paced.
    bucket = TokenBucket(50, capacity=5)
    start = time.monotonic()
    for _ in range(15):
        await bucket.acquire()
    elapsed = time.monotonic() - start
    # 15 - 5 burst = 10 tokens at 50/s ≈ 0.2s minimum.
    assert elapsed >= 0.15

"""Load generator.

Sends N messages through the HTTP ingress (batched), measures ingress-side
submit throughput and accept latency, then reads end-to-end latency percentiles
from the HTTP sink once all messages have arrived.

    python tools/loadgen.py --count 100000 --batch-size 500 --concurrency 32
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from collections import deque

import aiohttp

from relay.egress.shaper import TokenBucket


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def _build_batches(count: int, batch_size: int, text: str) -> deque[list[dict[str, str]]]:
    batches: deque[list[dict[str, str]]] = deque()
    remaining = count
    while remaining > 0:
        n = min(batch_size, remaining)
        batches.append([{"to": "+40712345678", "text": text, "sender": "RELAY"} for _ in range(n)])
        remaining -= n
    return batches


async def _send(
    session: aiohttp.ClientSession,
    url: str,
    token: str,
    batches: deque[list[dict[str, str]]],
    shaper: TokenBucket,
) -> tuple[list[float], int, int]:
    accept_latencies: list[float] = []
    accepted = 0
    rejected = 0
    headers = {"X-Auth-Token": token}
    while batches:
        batch = batches.popleft()
        await shaper.acquire(len(batch))  # pace submission to --rate (no-op if 0)
        t0 = time.perf_counter()
        try:
            async with session.post(
                f"{url}/v1/messages/batch", json={"messages": batch}, headers=headers
            ) as resp:
                await resp.read()
                if resp.status == 202:
                    accepted += len(batch)
                elif resp.status == 429:
                    rejected += len(batch)
                    batches.append(batch)  # retry later (backpressure)
                    await asyncio.sleep(0.05)
        except Exception:
            batches.append(batch)
            await asyncio.sleep(0.05)
            continue
        accept_latencies.append((time.perf_counter() - t0) * 1000.0)
    return accept_latencies, accepted, rejected


async def _wait_for_sink(
    session: aiohttp.ClientSession, sink_url: str, target: int
) -> dict[str, object]:
    last: dict[str, object] = {}
    prev_count = -1
    stalled = 0
    for _ in range(1200):  # hard cap ~10 min
        async with session.get(f"{sink_url}/stats") as resp:
            last = await resp.json()
        count = int(str(last.get("count", 0)))
        if count >= target:
            return last
        stalled = stalled + 1 if count == prev_count else 0
        if stalled >= 60:  # ~30s with no new arrivals: give up, report what we have
            return last
        prev_count = count
        await asyncio.sleep(0.5)
    return last


async def run(args: argparse.Namespace) -> dict[str, object]:
    text = "x" * args.text_size
    batches = _build_batches(args.count, args.batch_size, text)
    connector = aiohttp.TCPConnector(limit=args.concurrency + 8)
    timeout = aiohttp.ClientTimeout(total=120)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        if args.sink_url:
            async with session.post(f"{args.sink_url}/reset") as resp:
                await resp.read()

        shaper = TokenBucket(args.rate, capacity=max(args.rate, args.batch_size))
        start = time.perf_counter()
        senders = [
            _send(session, args.url, args.token, batches, shaper) for _ in range(args.concurrency)
        ]
        results = await asyncio.gather(*senders)
        submit_elapsed = time.perf_counter() - start

        accept_latencies: list[float] = []
        accepted = rejected = 0
        for lat, acc, rej in results:
            accept_latencies.extend(lat)
            accepted += acc
            rejected += rej

        summary: dict[str, object] = {
            "count": args.count,
            "accepted": accepted,
            "rejected_429": rejected,
            "submit_seconds": round(submit_elapsed, 3),
            "submit_throughput_per_s": round(accepted / submit_elapsed, 1)
            if submit_elapsed
            else None,
            "accept_p50_ms": round(_percentile(accept_latencies, 0.50), 3),
            "accept_p95_ms": round(_percentile(accept_latencies, 0.95), 3),
            "accept_p99_ms": round(_percentile(accept_latencies, 0.99), 3),
        }

        if args.sink_url:
            sink = await _wait_for_sink(session, args.sink_url, args.count)
            summary["end_to_end"] = sink
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="relay load generator")
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--sink-url", default="http://localhost:8090")
    parser.add_argument("--token", default="devtoken")
    parser.add_argument("--count", type=int, default=100_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--text-size", type=int, default=20)
    parser.add_argument(
        "--rate", type=float, default=0.0, help="target submit rate msg/s (0 = unlimited)"
    )
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    summary = asyncio.run(run(args))
    print(json.dumps(summary, indent=2))
    if args.out:
        from pathlib import Path

        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(summary, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

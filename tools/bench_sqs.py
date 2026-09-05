"""Isolated SQS benchmark — the M0 baseline.

Measures the queue layer with nothing else around it, so every later number can
be read relative to this ceiling:

* producer throughput  — messages/second written, batched 10 per API call
* consumer throughput  — messages/second received and deleted, long polling
* round-trip latency   — p50/p95/p99 from publish to receive

Run it against ElasticMQ (``--endpoint-url http://localhost:9324``) and, with
AWS credentials in the environment, against real SQS (omit ``--endpoint-url``).
It also reports JSON-vs-msgpack serialization cost, measured in-process.

Usage::

    python tools/bench_sqs.py --endpoint-url http://localhost:9324 --count 20000
    python tools/bench_sqs.py --region eu-central-1 --count 20000   # real SQS
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import statistics
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path

from relay.common.config import QueueConfig, SqsConfig
from relay.common.ids import new_ulid
from relay.common.message import Message, get_serializer
from relay.queues.sqs import SqsBackend


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


def _make_messages(count: int, text_size: int) -> list[Message]:
    text = "x" * text_size
    now = time.time()
    return [
        Message(
            id=new_ulid(),
            to="+40712345678",
            text=text,
            sender="RELAY",
            source="http",
            received_at=now,
            attributes={},
        )
        for _ in range(count)
    ]


@dataclass
class SerializationResult:
    serializer: str
    encode_us_per_msg: float
    decode_us_per_msg: float
    bytes_per_msg: float


def measure_serialization(sample: Message, iterations: int = 50_000) -> list[SerializationResult]:
    results: list[SerializationResult] = []
    for name in ("json", "msgpack"):
        ser = get_serializer(name)
        encoded = ser.encode(sample)
        t0 = time.perf_counter()
        for _ in range(iterations):
            ser.encode(sample)
        enc = (time.perf_counter() - t0) / iterations * 1e6
        t0 = time.perf_counter()
        for _ in range(iterations):
            ser.decode(encoded)
        dec = (time.perf_counter() - t0) / iterations * 1e6
        results.append(SerializationResult(name, round(enc, 3), round(dec, 3), float(len(encoded))))
    return results


async def bench_producer(backend: SqsBackend, messages: list[Message], concurrency: int) -> float:
    producer = await backend.producer("bench")
    batches: deque[list[Message]] = deque(messages[i : i + 10] for i in range(0, len(messages), 10))

    async def worker() -> None:
        while batches:
            batch = batches.popleft()
            await producer.publish(batch)

    start = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - start
    return len(messages) / elapsed


async def bench_consumer(backend: SqsBackend, total: int, concurrency: int) -> tuple[float, int]:
    """Return (throughput, actually_consumed). All ``total`` messages are already
    enqueued before this runs, so an empty-poll streak means the queue is drained."""
    consumer = await backend.consumer("bench")
    consumed = 0
    start = time.perf_counter()

    async def worker() -> None:
        nonlocal consumed
        empty_streak = 0
        while consumed < total:
            batch = await consumer.receive(max_messages=10)
            if not batch:
                empty_streak += 1
                if empty_streak >= 3:  # queue drained; stop rather than spin
                    return
                continue
            empty_streak = 0
            consumed += len(batch)
            await consumer.delete([r.handle for r in batch])

    await asyncio.gather(*(worker() for _ in range(concurrency)))
    elapsed = time.perf_counter() - start
    return consumed / elapsed, consumed


async def bench_roundtrip(
    backend: SqsBackend, count: int, concurrency: int, text_size: int
) -> dict[str, float]:
    producer = await backend.producer("bench")
    consumer = await backend.consumer("bench")
    latencies: list[float] = []
    text = "x" * text_size
    sent_done = asyncio.Event()

    async def send_all() -> None:
        try:
            batch: list[Message] = []
            for i in range(count):
                # received_at is stamped as late as possible: its intended use.
                batch.append(
                    Message(
                        id=new_ulid(),
                        to="+40712345678",
                        text=text,
                        sender="RELAY",
                        source="http",
                        received_at=time.time(),
                        attributes={},
                    )
                )
                if len(batch) == 10 or i == count - 1:
                    await producer.publish(batch)
                    batch = []
        finally:
            sent_done.set()

    async def receive_all() -> None:
        empty_streak = 0
        while len(latencies) < count:
            got = await consumer.receive(max_messages=10)
            recv = time.time()
            if not got:
                # Only give up once the sender is finished and the queue is dry.
                if sent_done.is_set():
                    empty_streak += 1
                    if empty_streak >= 3:
                        return
                continue
            empty_streak = 0
            latencies.extend(recv - r.message.received_at for r in got)
            await consumer.delete([r.handle for r in got])

    sender = asyncio.create_task(send_all())
    receivers = [asyncio.create_task(receive_all()) for _ in range(concurrency)]
    await sender
    await asyncio.gather(*receivers)

    return {
        "p50_ms": round(_percentile(latencies, 0.50) * 1000, 3),
        "p95_ms": round(_percentile(latencies, 0.95) * 1000, 3),
        "p99_ms": round(_percentile(latencies, 0.99) * 1000, 3),
        "mean_ms": round(statistics.fmean(latencies) * 1000, 3) if latencies else float("nan"),
        "samples": float(len(latencies)),
    }


def _build_backend(args: argparse.Namespace) -> SqsBackend:
    sqs = SqsConfig(
        region=args.region,
        endpoint_url=args.endpoint_url,
        wait_time_seconds=args.wait_time,
        visibility_timeout=60,
        # Pool must exceed concurrency, else long-polling consumers starve producers.
        max_pool_connections=args.concurrency + 8,
    )
    config = QueueConfig(
        backend="sqs", serializer=args.serializer, queues={"bench": args.queue}, sqs=sqs
    )
    return SqsBackend(config)


async def _prepare_queue(backend: SqsBackend) -> None:
    await backend.ensure_queue("bench")
    # purge is best-effort: real SQS rate-limits it to once per 60s.
    with contextlib.suppress(Exception):
        await backend.purge("bench")


async def run(args: argparse.Namespace) -> dict[str, object]:
    target = "elasticmq" if args.endpoint_url else "aws-sqs"
    messages = _make_messages(args.count, args.text_size)
    ser_results = measure_serialization(messages[0])

    def progress(stage: str) -> None:
        print(f"[bench] {stage}", file=sys.stderr, flush=True)

    backend = _build_backend(args)
    await backend.start()
    try:
        await _prepare_queue(backend)
        progress("producer...")
        producer_tps = await bench_producer(backend, messages, args.concurrency)
        progress(f"producer done: {producer_tps:.0f}/s; consumer...")
        consumer_tps, consumed = await bench_consumer(backend, args.count, args.concurrency)
        progress(f"consumer done: {consumer_tps:.0f}/s ({consumed}/{args.count}); roundtrip...")
        await _prepare_queue(backend)
        roundtrip = await bench_roundtrip(backend, args.rt_count, args.concurrency, args.text_size)
        progress("roundtrip done")
    finally:
        await backend.close()

    result: dict[str, object] = {
        "target": target,
        "endpoint_url": args.endpoint_url,
        "region": args.region,
        "serializer": args.serializer,
        "count": args.count,
        "consumed": consumed,
        "concurrency": args.concurrency,
        "text_size": args.text_size,
        "producer_tps": round(producer_tps, 1),
        "consumer_tps": round(consumer_tps, 1),
        "roundtrip": roundtrip,
        "serialization": [asdict(r) for r in ser_results],
    }
    return result


def _print_report(result: dict[str, object]) -> None:
    print(json.dumps(result, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Isolated SQS baseline benchmark")
    parser.add_argument("--endpoint-url", default=None, help="ElasticMQ URL; omit for real SQS")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--queue", default="bench")
    parser.add_argument("--count", type=int, default=20_000, help="messages for throughput tests")
    parser.add_argument("--rt-count", type=int, default=5_000, help="messages for latency test")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--text-size", type=int, default=20)
    parser.add_argument("--wait-time", type=int, default=1, help="long-poll seconds")
    parser.add_argument("--serializer", default="json", choices=["json", "msgpack"])
    parser.add_argument("--uvloop", action="store_true", help="use uvloop event loop (Linux/macOS)")
    parser.add_argument("--out", default=None, help="write raw JSON result to this path")
    args = parser.parse_args()

    loop_name = "asyncio"
    if args.uvloop:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop_name = "uvloop"

    result = asyncio.run(run(args))
    result["event_loop"] = loop_name
    _print_report(result)
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(json.dumps(result, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

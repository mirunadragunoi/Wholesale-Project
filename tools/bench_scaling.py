"""Horizontal scaling benchmark (M1.5 Sarcina 2).

Runs the full flow with 1, 2 and 4 instances of the engine AND egress processes
consuming from the same queues, under the same offered load, and reports
aggregate end-to-end throughput and the scaling factor vs a single instance.

Interpretation (see docs/BENCHMARKS.md):
- near-linear scaling  -> the bottleneck is not our code
- plateau              -> the bottleneck is real (the shared broker, or contention)

Run it from the Linux venv, with ElasticMQ (or real SQS) reachable:

    python tools/bench_scaling.py --instances 1 2 4 --count 60000
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import subprocess
import sys
import time
from argparse import Namespace
from pathlib import Path

import aiohttp

import loadgen  # sibling tool (tools/ is on sys.path[0] when run as a script)
from relay.common.config import QueueConfig, SqsConfig
from relay.queues.sqs import SqsBackend

REPO = Path(__file__).resolve().parent.parent
PY = sys.executable


async def _purge(endpoint: str) -> None:
    cfg = QueueConfig(
        backend="sqs",
        queues={"ingress": "ingress", "egress": "egress"},
        sqs=SqsConfig(endpoint_url=endpoint, max_pool_connections=8),
    )
    backend = SqsBackend(cfg)
    await backend.start()
    try:
        for q in ("ingress", "egress"):
            await backend.ensure_queue(q)
            with contextlib.suppress(Exception):  # purge best-effort
                await backend.purge(q)
    finally:
        await backend.close()


def _spawn(module: str, extra_env: dict[str, str], log: Path) -> subprocess.Popen[bytes]:
    env = {**os.environ, **extra_env}
    fh = log.open("wb")
    return subprocess.Popen(
        [PY, "-m", module, "--config", f"config/{module.split('.')[1]}.yaml"],
        cwd=REPO,
        env=env,
        stdout=fh,
        stderr=subprocess.STDOUT,
    )


def _spawn_sink(port: int, log: Path) -> subprocess.Popen[bytes]:
    fh = log.open("wb")
    return subprocess.Popen(
        [PY, "tools/http_sink.py", "--port", str(port)],
        cwd=REPO,
        env={**os.environ},
        stdout=fh,
        stderr=subprocess.STDOUT,
    )


async def _wait_ready(url: str, timeout_s: float = 30.0) -> bool:
    async with aiohttp.ClientSession() as session:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                async with session.get(url) as resp:
                    if resp.status < 500:
                        return True
            except Exception:
                pass
            await asyncio.sleep(0.3)
    return False


def _stop(procs: list[subprocess.Popen[bytes]]) -> None:
    for p in procs:
        p.terminate()  # SIGTERM -> graceful shutdown on Linux
    for p in procs:
        try:
            p.wait(timeout=15)
        except subprocess.TimeoutExpired:
            p.kill()


async def run_config(n: int, args: argparse.Namespace, logdir: Path) -> dict[str, object]:
    await _purge(args.endpoint)
    procs: list[subprocess.Popen[bytes]] = []
    procs.append(_spawn_sink(8090, logdir / f"sink_n{n}.log"))
    ser_env = {"SERIALIZER": args.serializer}
    procs.append(_spawn("relay.ingress.main", ser_env, logdir / f"ingress_n{n}.log"))
    for i in range(n):
        procs.append(
            _spawn(
                "relay.engine.main",
                {**ser_env, "ENGINE_METRICS_PORT": "0"},
                logdir / f"engine{i}_n{n}.log",
            )
        )
        procs.append(
            _spawn(
                "relay.egress.main",
                {**ser_env, "EGRESS_METRICS_PORT": "0"},
                logdir / f"egress{i}_n{n}.log",
            )
        )
    try:
        if not await _wait_ready("http://localhost:8080/health"):
            raise RuntimeError("ingress did not become ready")
        if not await _wait_ready("http://localhost:8090/stats"):
            raise RuntimeError("sink did not become ready")
        await asyncio.sleep(2.0)  # let consumers bind and start polling

        ns = Namespace(
            url="http://localhost:8080",
            sink_url="http://localhost:8090",
            token="devtoken",
            count=args.count,
            batch_size=args.batch_size,
            concurrency=args.concurrency,
            text_size=20,
            rate=0.0,  # saturation
            out=None,
        )
        result = await loadgen.run(ns)
    finally:
        _stop(procs)
        await asyncio.sleep(1.0)

    e2e = result.get("end_to_end", {})
    tput = e2e.get("throughput_per_s") if isinstance(e2e, dict) else None
    return {
        "instances": n,
        "count": args.count,
        "e2e_throughput_per_s": tput,
        "accepted": result.get("accepted"),
        "rejected_429": result.get("rejected_429"),
        "submit_throughput_per_s": result.get("submit_throughput_per_s"),
        "e2e": e2e,
    }


async def main_async(args: argparse.Namespace) -> list[dict[str, object]]:
    logdir = REPO / "bench_out" / "scaling"
    logdir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, object]] = []
    for n in args.instances:
        print(f"[scaling] running {n} engine+egress instance(s)...", file=sys.stderr, flush=True)
        res = await run_config(n, args, logdir)
        results.append(res)
        print(json.dumps(res, indent=2), file=sys.stderr, flush=True)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="horizontal scaling benchmark")
    parser.add_argument("--instances", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--count", type=int, default=60_000)
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--endpoint", default="http://localhost:9324")
    parser.add_argument("--serializer", default="json", choices=["json", "msgpack"])
    parser.add_argument("--target", default="elasticmq")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()
    results = asyncio.run(main_async(args))
    base = results[0]["e2e_throughput_per_s"] if results else None
    print(json.dumps({"target": args.target, "results": results, "baseline_tput": base}, indent=2))
    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(
            json.dumps({"target": args.target, "results": results}, indent=2), encoding="utf-8"
        )


if __name__ == "__main__":
    main()

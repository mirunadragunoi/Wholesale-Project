"""Simulated HTTP provider (sink).

Accepts anything the egress connector POSTs, optionally adds artificial latency
and/or returns errors, and measures end-to-end latency using the ``received_at``
timestamp carried in each message. ``GET /stats`` returns count, throughput and
latency percentiles; ``POST /reset`` clears them.

    python tools/http_sink.py --port 8090 --latency-ms 0 --error-rate 0.0
"""

from __future__ import annotations

import argparse
import asyncio
import random
import time
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, Response


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    k = (len(ordered) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


class Stats:
    def __init__(self) -> None:
        self.latencies_ms: list[float] = []
        self.first_ts: float | None = None
        self.last_ts: float | None = None
        self.errors = 0


def create_app(latency_ms: float, error_rate: float) -> FastAPI:
    app = FastAPI(title="relay http sink")
    stats = Stats()
    app.state.stats = stats

    @app.post("/sink")
    async def sink(request: Request) -> Response:
        now = time.time()
        body: dict[str, Any] = await request.json()
        received_at = body.get("received_at")
        if isinstance(received_at, int | float):
            stats.latencies_ms.append((now - received_at) * 1000.0)
        if stats.first_ts is None:
            stats.first_ts = now
        stats.last_ts = now
        if latency_ms > 0:
            await asyncio.sleep(latency_ms / 1000.0)
        if error_rate > 0 and random.random() < error_rate:
            stats.errors += 1
            return Response(status_code=500)
        return Response(status_code=200)

    @app.get("/stats")
    async def get_stats() -> dict[str, Any]:
        lat = stats.latencies_ms
        elapsed = (
            (stats.last_ts - stats.first_ts)
            if stats.first_ts is not None and stats.last_ts is not None
            else 0.0
        )
        return {
            "count": len(lat),
            "errors": stats.errors,
            "wall_seconds": round(elapsed, 3),
            "throughput_per_s": round(len(lat) / elapsed, 1) if elapsed > 0 else None,
            "e2e_p50_ms": round(_percentile(lat, 0.50), 3),
            "e2e_p95_ms": round(_percentile(lat, 0.95), 3),
            "e2e_p99_ms": round(_percentile(lat, 0.99), 3),
            "e2e_max_ms": round(max(lat), 3) if lat else None,
        }

    @app.post("/reset")
    async def reset() -> dict[str, str]:
        stats.latencies_ms.clear()
        stats.first_ts = None
        stats.last_ts = None
        stats.errors = 0
        return {"status": "reset"}

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="simulated HTTP provider")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--latency-ms", type=float, default=0.0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    args = parser.parse_args()
    app = create_app(args.latency_ms, args.error_rate)
    uvicorn.run(app, host=args.host, port=args.port, log_config=None)


if __name__ == "__main__":
    main()

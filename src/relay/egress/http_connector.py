"""HTTP egress connector.

Consumes the ``egress`` queue and POSTs each message to a configurable endpoint,
reusing a single aiohttp connection pool. Records per-submit duration, result
counts, and the end-to-end latency (now - received_at) — the latter is the whole
point of carrying ``received_at`` untouched through the pipeline.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import asdict

import aiohttp

from relay.common.config import HttpEgressConfig
from relay.common.logging import get_logger
from relay.common.metrics import (
    egress_submit_duration_seconds,
    egress_submitted_total,
    end_to_end_duration_seconds,
)
from relay.egress.shaper import TokenBucket
from relay.queues.base import ReceivedMessage

_log = get_logger("egress.http")


class HttpEgressConnector:
    def __init__(self, config: HttpEgressConfig, session: aiohttp.ClientSession) -> None:
        self._config = config
        self._session = session
        self._name = config.connector_name
        self._shaper = TokenBucket(config.tps_limit)

    async def handle(self, batch: list[ReceivedMessage]) -> list[str]:
        # Submit the batch concurrently; ack every message (POC: no requeue).
        await asyncio.gather(*(self._submit(item) for item in batch))
        return [item.handle for item in batch]

    async def _submit(self, item: ReceivedMessage) -> None:
        await self._shaper.acquire()
        msg = item.message
        payload = asdict(msg)
        result = "success"
        try:
            with egress_submit_duration_seconds.labels(connector=self._name).time():
                async with self._session.post(self._config.endpoint_url, json=payload) as resp:
                    if resp.status >= 400:
                        result = "error"
                    await resp.read()
        except Exception:
            result = "error"
            _log.exception("egress_submit_failed", message_id=msg.id)
        egress_submitted_total.labels(connector=self._name, result=result).inc()
        end_to_end_duration_seconds.observe(max(0.0, time.time() - msg.received_at))

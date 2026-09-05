"""CSV ingress connector.

Streams a CSV file row by row — never the whole file in memory — and injects
each row as a ``Message`` at a configurable rate. A 5M-line file processes in
bounded memory: only one row plus a small publish batch (<= batch_size) is held
at any time. Invalid rows are skipped and logged; a few bad lines never stop the
run. Progress is reported periodically.

Expected columns: ``to`` (E.164), ``text`` (required), ``sender`` (optional).
"""

from __future__ import annotations

import asyncio
import csv
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TextIO

from relay.common.config import CsvIngressConfig
from relay.common.ids import new_ulid
from relay.common.logging import get_logger
from relay.common.message import Message
from relay.common.metrics import ingress_received_total
from relay.egress.shaper import TokenBucket
from relay.queues.base import Producer
from relay.queues.factory import create_backend

_log = get_logger("ingress.csv")


@dataclass(slots=True)
class CsvStats:
    total: int = 0
    sent: int = 0
    skipped: int = 0


def _read_chunk(reader: csv.DictReader[str], size: int) -> list[dict[str, str | None]]:
    """Read up to ``size`` rows. Blocking — runs in a thread executor."""
    rows: list[dict[str, str | None]] = []
    for _ in range(size):
        try:
            rows.append(next(reader))
        except StopIteration:
            break
    return rows


def _row_to_message(row: Mapping[str, str | None]) -> Message | None:
    to = (row.get("to") or "").strip()
    text = row.get("text")
    if not to or text is None:
        return None
    sender = (row.get("sender") or "").strip() or None
    return Message(
        id=new_ulid(),
        to=to,
        text=text,
        sender=sender,
        source="csv",
        received_at=time.time(),
        attributes={},
    )


class CsvIngress:
    def __init__(self, config: CsvIngressConfig) -> None:
        self._config = config
        self._backend = create_backend(config.queue)
        self._shaper = TokenBucket(config.rate)

    async def run(self) -> CsvStats:
        await self._backend.start()
        producer = await self._backend.producer(self._config.ingress_queue)
        stats = CsvStats()
        batch: list[Message] = []
        # File I/O is blocking, so all reads run in a thread (never on the loop).
        # Only one row-chunk (read_chunk rows) plus one publish batch are ever in
        # memory, so a 5M-line file processes in bounded memory.
        read_chunk = max(self._config.batch_size * 32, 256)
        fh: TextIO = await asyncio.to_thread(
            open, self._config.path, "r", -1, "utf-8", None, ""
        )
        try:
            reader = csv.DictReader(fh)
            while True:
                rows = await asyncio.to_thread(_read_chunk, reader, read_chunk)
                if not rows:
                    break
                for row in rows:
                    stats.total += 1
                    msg = _row_to_message(row)
                    if msg is None:
                        stats.skipped += 1
                        if stats.skipped <= 20 or stats.skipped % 1000 == 0:
                            _log.warning("csv_row_skipped", line=stats.total, skipped=stats.skipped)
                        continue
                    batch.append(msg)
                    if len(batch) >= self._config.batch_size:
                        await self._flush(producer, batch, stats)
                        batch = []
            if batch:
                await self._flush(producer, batch, stats)
        finally:
            await asyncio.to_thread(fh.close)
            await self._backend.close()
        _log.info("csv_ingest_done", total=stats.total, sent=stats.sent, skipped=stats.skipped)
        return stats

    async def _flush(self, producer: Producer, batch: list[Message], stats: CsvStats) -> None:
        await self._shaper.acquire(len(batch))
        await producer.publish(batch)
        stats.sent += len(batch)
        ingress_received_total.labels(source="csv").inc(len(batch))
        if stats.sent % self._config.progress_every < self._config.batch_size:
            _log.info("csv_progress", sent=stats.sent, skipped=stats.skipped)

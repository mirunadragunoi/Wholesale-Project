"""SMPP ingress connector.

Runs an SMPP server (``smpp/server.py``) and, for each accepted ``submit_sm``,
generates a ULID, builds the canonical ``Message`` and puts it on a bounded
internal queue that publisher workers drain to the ingress SQS queue. The submit
handler is fast — accept, enqueue, return — so ``submit_sm_resp`` goes out well
under 100 ms and never blocks the client's window. A full internal queue returns
``ESME_RMSGQFUL`` (the SMPP equivalent of HTTP 429), never unbounded buffering.

Multi-part transparency: the raw ``short_message`` bytes, ``esm_class`` and
``data_coding`` are preserved in ``Message.attributes`` so an SMPP egress can
pass a concatenated message through byte-for-byte (UDH intact) instead of
re-encoding it. HTTP egress uses the decoded ``text``.
"""

from __future__ import annotations

import asyncio
import time

from relay.common.config import SmppIngressConfig
from relay.common.ids import new_ulid
from relay.common.logging import get_logger
from relay.common.message import Message
from relay.common.metrics import ingress_received_total, start_metrics_server
from relay.queues.factory import create_backend
from relay.smpp.constants import CommandStatus, Ton
from relay.smpp.encoding import decode_segment
from relay.smpp.pdu import SubmitSm
from relay.smpp.server import ServerConfig, SmppServer

_log = get_logger("ingress.smpp")


class SmppIngress:
    def __init__(self, config: SmppIngressConfig) -> None:
        self._config = config
        self._backend = create_backend(config.queue)
        self._queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=config.internal_queue_maxsize)
        self._stop = asyncio.Event()
        self._workers: list[asyncio.Task[None]] = []
        self._server = SmppServer(
            config.host,
            config.port,
            ServerConfig(
                credentials=config.credentials,
                ip_allowlist=config.ip_allowlist,
                max_binds_per_system=config.max_binds_per_system,
                tps_per_system=config.tps_per_system,
                system_id=config.smsc_system_id,
            ),
            on_submit=self._on_submit,
        )

    async def start(self) -> None:
        await self._backend.start()
        self._workers = [
            asyncio.create_task(self._publisher()) for _ in range(self._config.publisher_workers)
        ]
        start_metrics_server(self._config.metrics_port)
        await self._server.start()
        _log.info(
            "smpp_ingress_started",
            port=self._config.port,
            metrics_port=self._config.metrics_port,
            workers=self._config.publisher_workers,
        )

    async def stop(self) -> None:
        _log.info("smpp_ingress_stopping", pending=self._queue.qsize())
        await self._server.stop()
        self._stop.set()
        await asyncio.gather(*self._workers, return_exceptions=True)
        await self._backend.close()

    async def wait_closed(self) -> None:
        await self._stop.wait()

    async def _publisher(self) -> None:
        producer = await self._backend.producer(self._config.ingress_queue)
        while not self._stop.is_set() or not self._queue.empty():
            try:
                first = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            batch = [first]
            while len(batch) < 10:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await producer.publish(batch)
            for _ in batch:
                self._queue.task_done()

    async def _on_submit(self, pdu: SubmitSm, system_id: str) -> tuple[int, str]:
        text = decode_segment(pdu.short_message, pdu.data_coding, pdu.esm_class)
        dest = pdu.destination_addr
        to = (
            f"+{dest}"
            if pdu.dest_addr_ton == Ton.INTERNATIONAL and not dest.startswith("+")
            else dest
        )
        msg = Message(
            id=new_ulid(),
            to=to,
            text=text,
            sender=pdu.source_addr or None,
            source="smpp",
            received_at=time.time(),
            attributes={
                # Pass-through: preserve the wire form so egress need not re-encode.
                "smpp.esm_class": f"0x{pdu.esm_class:02x}",
                "smpp.data_coding": str(pdu.data_coding),
                "smpp.raw": pdu.short_message.hex(),
                "smpp.system_id": system_id,
                "smpp.source_ton": str(pdu.source_addr_ton),
                "smpp.source_npi": str(pdu.source_addr_npi),
            },
        )
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            return CommandStatus.ESME_RMSGQFUL, ""
        ingress_received_total.labels(source="smpp").inc()
        return CommandStatus.ESME_ROK, msg.id

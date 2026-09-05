"""Simulated SMPP provider (SMSC).

A provider that always works tests nothing, so this one has configurable defects:

  * response latency (fixed + jitter)
  * error rate returning a specific command_status
  * a TPS cap that returns ESME_RTHROTTLED past the limit
  * message_id format hex or decimal, INDEPENDENTLY for submit_sm_resp and DLR
    — the classic integration trap is a provider that answers hex in the resp
    and decimal in the DLR (or vice versa); this reproduces it
  * delivery receipts (deliver_sm) after a delay, with a configurable status
  * abrupt disconnect after N submits, to test client reconnection

    python tools/smpp_sink.py --port 2775 --submit-id-format hex --dlr --dlr-id-format dec
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import time
from collections import deque
from dataclasses import dataclass

from relay.common.logging import configure_logging, get_logger
from relay.smpp import pdu as pdumod
from relay.smpp.constants import CommandId, CommandStatus, EsmClass, Tlv
from relay.smpp.pdu import (
    PDU,
    BindReceiverResp,
    BindTransceiverResp,
    BindTransmitterResp,
    DeliverSm,
    EnquireLinkResp,
    GenericNack,
    SubmitSm,
    SubmitSmResp,
    UnbindResp,
)

_log = get_logger("smpp.sink")


@dataclass(frozen=True, slots=True)
class SinkConfig:
    latency_ms: float = 0.0
    latency_jitter_ms: float = 0.0
    throttle_tps: int = 0  # 0 = unlimited
    error_rate: float = 0.0
    error_status: int = CommandStatus.ESME_RSUBMITFAIL
    submit_id_format: str = "hex"  # "hex" | "dec"
    dlr_enabled: bool = False
    dlr_id_format: str = "hex"
    dlr_delay_ms: float = 200.0
    dlr_stat: str = "DELIVRD"
    drop_after: int = 0  # 0 = never; else drop the connection after N submits


def _format_id(n: int, fmt: str) -> str:
    return f"{n:x}" if fmt == "hex" else str(n)


class Sink:
    def __init__(self, config: SinkConfig) -> None:
        self._config = config
        self._msg_counter = 0
        self._submit_times: deque[float] = deque()
        self._dlr_tasks: set[asyncio.Task[None]] = set()
        self.submits = 0  # total submit_sm received (incl. duplicates)
        self.throttled = 0
        # Duplicate detection by our ULID TLV. Survives reconnections because the
        # Sink instance is shared across all connections of one process.
        self._seen_ids: set[str] = set()
        self.with_id = 0  # submits carrying a ULID
        self.duplicates = 0  # submits whose ULID was already seen

    def _throttled(self) -> bool:
        if self._config.throttle_tps <= 0:
            return False
        now = time.monotonic()
        window = self._submit_times
        while window and now - window[0] > 1.0:
            window.popleft()
        if len(window) >= self._config.throttle_tps:
            return True
        window.append(now)
        return False

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        _log.info("sink_connection_open", peer=str(peer))
        submits_here = 0
        try:
            while True:
                header = await reader.readexactly(4)
                length = int.from_bytes(header, "big")
                if length < 16 or length > pdumod.MAX_PDU_SIZE:
                    writer.write(GenericNack(command_status=CommandStatus.ESME_RINVCMDLEN).encode())
                    await writer.drain()
                    break
                rest = await reader.readexactly(length - 4)
                try:
                    pdu = pdumod.decode(header + rest)
                except pdumod.PduError as exc:
                    _log.warning("sink_bad_pdu", error=str(exc))
                    writer.write(GenericNack(command_status=CommandStatus.ESME_RINVCMDLEN).encode())
                    await writer.drain()
                    continue

                if pdu.command_id == CommandId.SUBMIT_SM:
                    self._record_submit(pdu)
                    submits_here += 1
                    # Drop the socket BEFORE responding: the provider received the
                    # message but the resp is lost -> the client cannot know and
                    # will redeliver -> the classic at-least-once duplicate.
                    if self._config.drop_after and submits_here >= self._config.drop_after:
                        _log.warning("sink_abrupt_disconnect_before_resp", after=submits_here)
                        break
                stop = await self._respond(pdu, writer)
                if stop:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            with contextlib.suppress(Exception):
                writer.close()
            _log.info("sink_connection_close", peer=str(peer))

    async def _respond(self, pdu: PDU, writer: asyncio.StreamWriter) -> bool:
        """Answer one PDU. Returns True if the connection should close."""
        cid = pdu.command_id
        seq = pdu.sequence_number
        if cid in (
            CommandId.BIND_TRANSCEIVER,
            CommandId.BIND_TRANSMITTER,
            CommandId.BIND_RECEIVER,
        ):
            resp_cls = {
                CommandId.BIND_TRANSCEIVER: BindTransceiverResp,
                CommandId.BIND_TRANSMITTER: BindTransmitterResp,
                CommandId.BIND_RECEIVER: BindReceiverResp,
            }[cid]
            writer.write(resp_cls(sequence_number=seq, system_id="smpp-sink").encode())
            await writer.drain()
            return False
        if cid == CommandId.ENQUIRE_LINK:
            writer.write(EnquireLinkResp(sequence_number=seq).encode())
            await writer.drain()
            return False
        if cid == CommandId.UNBIND:
            writer.write(UnbindResp(sequence_number=seq).encode())
            await writer.drain()
            return True
        if cid == CommandId.SUBMIT_SM:
            await self._handle_submit(pdu, writer)
            return False
        writer.write(
            GenericNack(command_status=CommandStatus.ESME_RINVCMDID, sequence_number=seq).encode()
        )
        await writer.drain()
        return False

    def _record_submit(self, pdu: PDU) -> None:
        assert isinstance(pdu, SubmitSm)
        self.submits += 1
        ulid: str | None = None
        for tag, value in pdu.tlvs:
            if tag == int(Tlv.RELAY_MESSAGE_ID):
                ulid = value.decode("latin-1")
                break
        if ulid is None:
            return
        self.with_id += 1
        if ulid in self._seen_ids:
            self.duplicates += 1
        else:
            self._seen_ids.add(ulid)

    async def _handle_submit(self, pdu: PDU, writer: asyncio.StreamWriter) -> None:
        assert isinstance(pdu, SubmitSm)
        cfg = self._config
        if cfg.latency_ms > 0 or cfg.latency_jitter_ms > 0:
            delay = cfg.latency_ms + random.uniform(0, cfg.latency_jitter_ms)
            await asyncio.sleep(delay / 1000.0)

        if self._throttled():
            self.throttled += 1
            writer.write(
                SubmitSmResp(
                    command_status=CommandStatus.ESME_RTHROTTLED,
                    sequence_number=pdu.sequence_number,
                ).encode()
            )
            await writer.drain()
            return

        if cfg.error_rate > 0 and random.random() < cfg.error_rate:
            writer.write(
                SubmitSmResp(
                    command_status=cfg.error_status, sequence_number=pdu.sequence_number
                ).encode()
            )
            await writer.drain()
            return

        self._msg_counter += 1
        msg_int = self._msg_counter
        writer.write(
            SubmitSmResp(
                sequence_number=pdu.sequence_number,
                message_id=_format_id(msg_int, cfg.submit_id_format),
            ).encode()
        )
        await writer.drain()

        if cfg.dlr_enabled:
            task = asyncio.create_task(self._send_dlr(writer, msg_int, pdu))
            self._dlr_tasks.add(task)
            task.add_done_callback(self._dlr_tasks.discard)

    async def _send_dlr(self, writer: asyncio.StreamWriter, msg_int: int, submit: SubmitSm) -> None:
        await asyncio.sleep(self._config.dlr_delay_ms / 1000.0)
        dlr_id = _format_id(msg_int, self._config.dlr_id_format)
        stamp = time.strftime("%y%m%d%H%M")
        receipt = (
            f"id:{dlr_id} sub:001 dlvrd:001 submit date:{stamp} done date:{stamp} "
            f"stat:{self._config.dlr_stat} err:000 text:"
        ).encode("latin-1")
        dlr = DeliverSm(
            source_addr=submit.destination_addr,
            destination_addr=submit.source_addr,
            esm_class=EsmClass.MT_DELIVERY_RECEIPT,
            short_message=receipt,
            tlvs=((0x001E, dlr_id.encode("latin-1") + b"\x00"),),  # receipted_message_id
        )
        with contextlib.suppress(Exception):
            writer.write(dlr.encode())
            await writer.drain()


def _log_stats(sink: Sink, event: str) -> None:
    _log.info(
        event,
        received=sink.submits,
        with_id=sink.with_id,
        unique=len(sink._seen_ids),
        duplicates=sink.duplicates,
        throttled=sink.throttled,
    )


async def _stats_loop(sink: Sink, interval: float = 2.0) -> None:
    while True:
        await asyncio.sleep(interval)
        _log_stats(sink, "sink_stats")


async def main_async(config: SinkConfig, host: str, port: int) -> None:
    sink = Sink(config)
    server = await asyncio.start_server(sink.handle, host, port)
    addr = server.sockets[0].getsockname()
    _log.info("sink_listening", host=addr[0], port=addr[1], config=str(config))
    stats_task = asyncio.create_task(_stats_loop(sink))
    try:
        async with server:
            await server.serve_forever()
    finally:
        stats_task.cancel()
        _log_stats(sink, "sink_final_stats")


def main() -> None:
    parser = argparse.ArgumentParser(description="simulated SMPP provider")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=2775)
    parser.add_argument("--latency-ms", type=float, default=0.0)
    parser.add_argument("--latency-jitter-ms", type=float, default=0.0)
    parser.add_argument("--throttle-tps", type=int, default=0)
    parser.add_argument("--error-rate", type=float, default=0.0)
    parser.add_argument(
        "--error-status",
        type=lambda s: int(s, 0),
        default=int(CommandStatus.ESME_RSUBMITFAIL),
    )
    parser.add_argument("--submit-id-format", choices=["hex", "dec"], default="hex")
    parser.add_argument("--dlr", action="store_true")
    parser.add_argument("--dlr-id-format", choices=["hex", "dec"], default="hex")
    parser.add_argument("--dlr-delay-ms", type=float, default=200.0)
    parser.add_argument("--dlr-stat", default="DELIVRD")
    parser.add_argument("--drop-after", type=int, default=0)
    args = parser.parse_args()

    configure_logging("INFO")
    config = SinkConfig(
        latency_ms=args.latency_ms,
        latency_jitter_ms=args.latency_jitter_ms,
        throttle_tps=args.throttle_tps,
        error_rate=args.error_rate,
        error_status=args.error_status,
        submit_id_format=args.submit_id_format,
        dlr_enabled=args.dlr,
        dlr_id_format=args.dlr_id_format,
        dlr_delay_ms=args.dlr_delay_ms,
        dlr_stat=args.dlr_stat,
        drop_after=args.drop_after,
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main_async(config, args.host, args.port))


if __name__ == "__main__":
    main()

"""SMPP server (ESME-facing SMSC role) for the ingress side.

Accepts binds from clients, validates a static system_id/password plus an IP
allowlist, enforces a max number of binds and a TPS cap per credential, and hands
each ``submit_sm`` to a callback that must be fast (accept + enqueue + return).
The callback returns ``(command_status, message_id)`` and we answer
``submit_sm_resp`` immediately — never synchronous processing, so the client's
window is never blocked.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from relay.common.logging import BoundLogger, get_logger
from relay.smpp import pdu as pdumod
from relay.smpp.constants import CommandId, CommandStatus
from relay.smpp.pdu import (
    PDU,
    Bind,
    BindReceiverResp,
    BindTransceiverResp,
    BindTransmitterResp,
    EnquireLinkResp,
    GenericNack,
    SubmitSm,
    SubmitSmResp,
    UnbindResp,
)

_log = get_logger("smpp.server")

# (command_status, message_id) — status 0 means accepted.
SubmitHandler = Callable[[SubmitSm, str], Awaitable[tuple[int, str]]]

_BIND_RESP = {
    CommandId.BIND_TRANSMITTER: BindTransmitterResp,
    CommandId.BIND_RECEIVER: BindReceiverResp,
    CommandId.BIND_TRANSCEIVER: BindTransceiverResp,
}


@dataclass(frozen=True, slots=True)
class ServerConfig:
    credentials: dict[str, str] = field(default_factory=dict)  # system_id -> password
    ip_allowlist: tuple[str, ...] = ()  # empty = allow all
    max_binds_per_system: int = 4
    tps_per_system: float = 0.0  # 0 = unlimited
    system_id: str = "relay-smsc"  # our identity in bind_resp


@dataclass(slots=True)
class _CredState:
    binds: int = 0
    submit_times: deque[float] = field(default_factory=deque)


class SmppServer:
    def __init__(
        self,
        host: str,
        port: int,
        config: ServerConfig,
        on_submit: SubmitHandler,
        log: BoundLogger | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._config = config
        self._on_submit = on_submit
        self._log = log or _log
        self._creds: dict[str, _CredState] = {}
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self._host, self._port)
        sock = self._server.sockets[0].getsockname()
        self._log.info("smpp_server_listening", host=sock[0], port=sock[1])

    @property
    def port(self) -> int:
        assert self._server is not None
        return int(self._server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    def _cred(self, system_id: str) -> _CredState:
        state = self._creds.get(system_id)
        if state is None:
            state = _CredState()
            self._creds[system_id] = state
        return state

    def _tps_ok(self, system_id: str) -> bool:
        limit = self._config.tps_per_system
        if limit <= 0:
            return True
        now = time.monotonic()
        times = self._cred(system_id).submit_times
        while times and now - times[0] > 1.0:
            times.popleft()
        if len(times) >= limit:
            return False
        times.append(now)
        return True

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip = peer[0] if peer else ""
        bound_system: str | None = None
        try:
            while True:
                header = await reader.readexactly(4)
                length = int.from_bytes(header, "big")
                if length < 16 or length > pdumod.MAX_PDU_SIZE:
                    await self._write(
                        writer, GenericNack(command_status=CommandStatus.ESME_RINVCMDLEN)
                    )
                    break
                rest = await reader.readexactly(length - 4)
                try:
                    pdu = pdumod.decode(header + rest)
                except pdumod.PduError as exc:
                    self._log.warning("smpp_server_bad_pdu", error=str(exc))
                    await self._write(
                        writer, GenericNack(command_status=CommandStatus.ESME_RINVCMDLEN)
                    )
                    continue
                stop, bound_system = await self._dispatch(pdu, writer, peer_ip, bound_system)
                if stop:
                    break
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            pass
        finally:
            if bound_system is not None:
                self._cred(bound_system).binds -= 1
            with contextlib.suppress(Exception):
                writer.close()

    async def _dispatch(
        self, pdu: PDU, writer: asyncio.StreamWriter, peer_ip: str, bound_system: str | None
    ) -> tuple[bool, str | None]:
        cid = pdu.command_id
        seq = pdu.sequence_number
        if cid in _BIND_RESP:
            assert isinstance(pdu, Bind)
            status = self._authenticate(pdu, peer_ip)
            resp_cls = _BIND_RESP[cid]
            await self._write(
                writer,
                resp_cls(
                    command_status=status, sequence_number=seq, system_id=self._config.system_id
                ),
            )
            if status == CommandStatus.ESME_ROK:
                self._cred(pdu.system_id).binds += 1
                self._log.info("smpp_server_bound", system_id=pdu.system_id, bind=cid.name)
                return False, pdu.system_id
            self._log.warning("smpp_server_bind_rejected", system_id=pdu.system_id, status=status)
            return True, None  # close on rejected bind
        if cid == CommandId.SUBMIT_SM:
            assert isinstance(pdu, SubmitSm)
            await self._handle_submit(pdu, writer, bound_system)
            return False, bound_system
        if cid == CommandId.ENQUIRE_LINK:
            await self._write(writer, EnquireLinkResp(sequence_number=seq))
            return False, bound_system
        if cid == CommandId.UNBIND:
            await self._write(writer, UnbindResp(sequence_number=seq))
            return True, bound_system
        await self._write(
            writer, GenericNack(command_status=CommandStatus.ESME_RINVCMDID, sequence_number=seq)
        )
        return False, bound_system

    def _authenticate(self, pdu: Bind, peer_ip: str) -> int:
        cfg = self._config
        if cfg.ip_allowlist and peer_ip not in cfg.ip_allowlist:
            return CommandStatus.ESME_RBINDFAIL
        expected = cfg.credentials.get(pdu.system_id)
        if expected is None:
            return CommandStatus.ESME_RINVSYSID
        if expected != pdu.password:
            return CommandStatus.ESME_RINVPASWD
        if self._cred(pdu.system_id).binds >= cfg.max_binds_per_system:
            return CommandStatus.ESME_RBINDFAIL
        return CommandStatus.ESME_ROK

    async def _handle_submit(
        self, pdu: SubmitSm, writer: asyncio.StreamWriter, bound_system: str | None
    ) -> None:
        seq = pdu.sequence_number
        if bound_system is None:
            await self._write(
                writer,
                SubmitSmResp(command_status=CommandStatus.ESME_RINVBNDSTS, sequence_number=seq),
            )
            return
        if not self._tps_ok(bound_system):
            await self._write(
                writer,
                SubmitSmResp(command_status=CommandStatus.ESME_RTHROTTLED, sequence_number=seq),
            )
            return
        status, message_id = await self._on_submit(pdu, bound_system)
        await self._write(
            writer,
            SubmitSmResp(command_status=status, sequence_number=seq, message_id=message_id),
        )

    async def _write(self, writer: asyncio.StreamWriter, pdu: PDU) -> None:
        writer.write(pdu.encode())
        await writer.drain()

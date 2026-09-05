"""SMPP session: state machine, unacked window, keepalive, timeouts.

One ``SmppSession`` drives one TCP connection. Reconnection/backoff is a helper
here (``backoff_delays``) but is driven by the owner (the egress connector),
which supervises connect → bind → run → reconnect.

Concurrency model:
  * a reader task frames and decodes inbound PDUs;
  * responses resolve the future of the matching ``sequence_number`` in the
    window; peer-originated requests (enquire_link, deliver_sm, unbind) are
    answered inline;
  * an enquire_link task probes liveness; two consecutive misses fail the
    session, which the owner turns into a reconnect.

Nothing blocks the event loop: the codec runs on bytes synchronously and fast,
all I/O is async.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, replace
from enum import IntEnum

from relay.common.logging import BoundLogger, get_logger
from relay.smpp import pdu as pdumod
from relay.smpp.constants import CommandId, CommandStatus
from relay.smpp.pdu import (
    PDU,
    DeliverSmResp,
    EnquireLink,
    EnquireLinkResp,
    GenericNack,
    Unbind,
    UnbindResp,
)

_log = get_logger("smpp.session")

SEQ_MAX = 0x7FFFFFFF
HEADER_SIZE = 16


class SessionState(IntEnum):
    OPEN = 0
    BOUND_TX = 1
    BOUND_RX = 2
    BOUND_TRX = 3
    UNBOUND = 4
    CLOSED = 5


@dataclass(frozen=True, slots=True)
class SessionConfig:
    window_size: int = 10
    enquire_link_interval: float = 30.0
    enquire_link_timeout: float = 10.0
    response_timeout: float = 30.0
    max_enquire_misses: int = 2


class SubmitTimeout(Exception):
    """No response arrived within the timeout. The peer may still have the PDU —
    a distinct outcome, not a plain failure."""


class BindError(Exception):
    def __init__(self, status: int) -> None:
        self.status = status
        name = CommandStatus(status).name if status in set(CommandStatus) else f"0x{status:08x}"
        super().__init__(f"bind rejected with {name}")


class SessionClosed(Exception):
    pass


class SequenceGenerator:
    """32-bit SMPP sequence numbers: 1 .. 0x7FFFFFFF, wrapping back to 1."""

    __slots__ = ("_n",)

    def __init__(self) -> None:
        self._n = 0

    def next(self) -> int:
        self._n = 1 if self._n >= SEQ_MAX else self._n + 1
        return self._n


def backoff_delays(base: float = 1.0, cap: float = 60.0, factor: float = 2.0) -> Iterator[float]:
    """Exponential backoff with equal jitter, from ~1s up to 60s."""
    attempt = 0
    while True:
        window = min(cap, base * (factor**attempt))
        yield window / 2 + random.uniform(0, window / 2)
        attempt += 1


DeliverHandler = Callable[[PDU], Awaitable[None]]


@dataclass(slots=True)
class _Pending:
    future: asyncio.Future[PDU]
    command_id: CommandId
    sent_at: float


class SmppSession:
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        config: SessionConfig,
        *,
        on_deliver_sm: DeliverHandler | None = None,
        log: BoundLogger | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._config = config
        self._on_deliver_sm = on_deliver_sm
        self._log = log or _log
        self._seq = SequenceGenerator()
        self._window: dict[int, _Pending] = {}
        self._sem = asyncio.Semaphore(config.window_size)
        self._closed = asyncio.Event()
        self._enquire_misses = 0
        self.state = SessionState.OPEN
        self._reader_task: asyncio.Task[None] | None = None
        self._enquire_task: asyncio.Task[None] | None = None

    @property
    def window_used(self) -> int:
        return len(self._window)

    async def wait_closed(self) -> None:
        await self._closed.wait()

    # -- lifecycle ---------------------------------------------------------- #
    def start(self) -> None:
        self._reader_task = asyncio.create_task(self._read_loop())

    async def bind(self, request: PDU, bound_state: SessionState) -> None:
        resp = await self.request(request, self._config.response_timeout)
        if resp.command_status != CommandStatus.ESME_ROK:
            await self._fail("bind_failed")
            raise BindError(resp.command_status)
        self.state = bound_state
        self._enquire_task = asyncio.create_task(self._enquire_loop())
        self._log.info("smpp_bound", state=self.state.name)

    async def request(self, request: PDU, timeout_s: float) -> PDU:
        """Send a request and await its response. Raises SubmitTimeout / SessionClosed."""
        if self._closed.is_set():
            raise SessionClosed("session is closed")
        await self._sem.acquire()
        seq = self._seq.next()
        framed = replace(request, sequence_number=seq)
        loop = asyncio.get_running_loop()
        future: asyncio.Future[PDU] = loop.create_future()
        self._window[seq] = _Pending(future, framed.command_id, time.monotonic())
        try:
            await self._write(framed)
            return await asyncio.wait_for(future, timeout_s)
        except TimeoutError:
            raise SubmitTimeout(f"no response for seq {seq}") from None
        finally:
            self._window.pop(seq, None)
            self._sem.release()

    async def unbind(self) -> None:
        if self.state in (SessionState.BOUND_TX, SessionState.BOUND_RX, SessionState.BOUND_TRX):
            with contextlib.suppress(SubmitTimeout, SessionClosed):
                await self.request(Unbind(), self._config.response_timeout)
            self.state = SessionState.UNBOUND
        await self.close()

    async def close(self) -> None:
        if self._closed.is_set():
            return
        self._closed.set()
        for task in (self._enquire_task, self._reader_task):
            if task is not None:
                task.cancel()
        for pending in self._window.values():
            if not pending.future.done():
                pending.future.set_exception(SessionClosed("session closed"))
        self._window.clear()
        with contextlib.suppress(Exception):  # best-effort close
            self._writer.close()
        self.state = SessionState.CLOSED

    async def _fail(self, reason: str) -> None:
        self._log.warning("smpp_session_failed", reason=reason)
        await self.close()

    # -- internals ---------------------------------------------------------- #
    async def _write(self, pdu: PDU) -> None:
        self._writer.write(pdu.encode())
        await self._writer.drain()

    async def _read_loop(self) -> None:
        try:
            while True:
                header = await self._reader.readexactly(4)
                length = int.from_bytes(header, "big")
                if length < HEADER_SIZE or length > pdumod.MAX_PDU_SIZE:
                    await self._fail(f"invalid command_length {length}")
                    return
                rest = await self._reader.readexactly(length - 4)
                try:
                    pdu = pdumod.decode(header + rest)
                except pdumod.PduError as exc:
                    # Framing stayed consistent (we read `length` bytes), so the
                    # stream is still aligned: nack and carry on.
                    self._log.warning("smpp_bad_pdu", error=str(exc))
                    await self._write(GenericNack(command_status=CommandStatus.ESME_RINVCMDLEN))
                    continue
                await self._dispatch(pdu)
        except (asyncio.IncompleteReadError, ConnectionError, OSError):
            await self._fail("connection_lost")
        except asyncio.CancelledError:
            raise

    async def _dispatch(self, pdu: PDU) -> None:
        if pdu.command_id.is_response:
            pending = self._window.get(pdu.sequence_number)
            if pending is not None and not pending.future.done():
                pending.future.set_result(pdu)
            else:
                self._log.debug("smpp_unexpected_response", seq=pdu.sequence_number)
            return
        # Peer-originated request.
        cid = pdu.command_id
        if cid == CommandId.ENQUIRE_LINK:
            await self._write(EnquireLinkResp(sequence_number=pdu.sequence_number))
        elif cid == CommandId.DELIVER_SM:
            if self._on_deliver_sm is not None:
                await self._on_deliver_sm(pdu)
            await self._write(DeliverSmResp(sequence_number=pdu.sequence_number))
        elif cid == CommandId.UNBIND:
            await self._write(UnbindResp(sequence_number=pdu.sequence_number))
            await self._fail("peer_unbind")
        else:
            self._log.debug("smpp_unhandled_request", command=cid.name)
            await self._write(
                GenericNack(
                    command_status=CommandStatus.ESME_RINVCMDID,
                    sequence_number=pdu.sequence_number,
                )
            )

    async def _enquire_loop(self) -> None:
        interval = self._config.enquire_link_interval
        try:
            while not self._closed.is_set():
                try:
                    await asyncio.wait_for(self._closed.wait(), timeout=interval)
                    return  # closed
                except TimeoutError:
                    pass
                try:
                    await self.request(EnquireLink(), self._config.enquire_link_timeout)
                    self._enquire_misses = 0
                except SubmitTimeout:
                    self._enquire_misses += 1
                    self._log.warning("smpp_enquire_miss", misses=self._enquire_misses)
                    if self._enquire_misses >= self._config.max_enquire_misses:
                        await self._fail("enquire_link_timeout")
                        return
                except SessionClosed:
                    return
        except asyncio.CancelledError:
            raise


async def open_connection(
    host: str, port: int
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    return await asyncio.open_connection(host, port)

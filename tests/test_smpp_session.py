from __future__ import annotations

import asyncio

import pytest

from relay.smpp import pdu as pdumod
from relay.smpp.constants import CommandId
from relay.smpp.pdu import (
    BindTransceiver,
    BindTransceiverResp,
    EnquireLinkResp,
    SubmitSm,
    SubmitSmResp,
    UnbindResp,
)
from relay.smpp.session import (
    SEQ_MAX,
    SequenceGenerator,
    SessionConfig,
    SessionState,
    SmppSession,
    SubmitTimeout,
    backoff_delays,
    open_connection,
)


def test_sequence_generator_wraps() -> None:
    gen = SequenceGenerator()
    assert gen.next() == 1
    assert gen.next() == 2
    gen._n = SEQ_MAX - 1
    assert gen.next() == SEQ_MAX
    assert gen.next() == 1  # wraps back to 1, not 0


def test_backoff_delays_bounds() -> None:
    gen = backoff_delays(base=1.0, cap=60.0)
    delays = [next(gen) for _ in range(12)]
    assert all(0 <= d <= 60.0 for d in delays)
    # equal jitter: first delay in [0.5, 1.0]; later ones grow but stay <= 60
    assert 0.5 <= delays[0] <= 1.0
    assert delays[-1] <= 60.0


async def _mini_smsc(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """A minimal SMSC that answers bind/submit/enquire/unbind."""
    try:
        while True:
            header = await reader.readexactly(4)
            length = int.from_bytes(header, "big")
            rest = await reader.readexactly(length - 4)
            pdu = pdumod.decode(header + rest)
            cid = pdu.command_id
            if cid == CommandId.BIND_TRANSCEIVER:
                writer.write(
                    BindTransceiverResp(sequence_number=pdu.sequence_number, system_id="smsc").encode()
                )
            elif cid == CommandId.SUBMIT_SM:
                writer.write(
                    SubmitSmResp(sequence_number=pdu.sequence_number, message_id="abc123").encode()
                )
            elif cid == CommandId.ENQUIRE_LINK:
                writer.write(EnquireLinkResp(sequence_number=pdu.sequence_number).encode())
            elif cid == CommandId.UNBIND:
                writer.write(UnbindResp(sequence_number=pdu.sequence_number).encode())
                await writer.drain()
                break
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


async def test_session_bind_submit_unbind() -> None:
    server = await asyncio.start_server(_mini_smsc, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await open_connection("127.0.0.1", port)
    session = SmppSession(reader, writer, SessionConfig(response_timeout=2.0))
    session.start()

    await session.bind(BindTransceiver(system_id="esme", password="pw"), SessionState.BOUND_TRX)
    bound_state = session.state
    assert bound_state == SessionState.BOUND_TRX

    resp = await session.request(SubmitSm(destination_addr="123", short_message=b"hi"), 2.0)
    assert isinstance(resp, SubmitSmResp)
    assert resp.message_id == "abc123"
    assert session.window_used == 0  # window freed after response

    await session.unbind()
    closed_state = session.state
    assert closed_state == SessionState.CLOSED
    server.close()
    await server.wait_closed()


async def _silent_smsc(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Answers bind, then never answers submit -> triggers SubmitTimeout."""
    try:
        while True:
            header = await reader.readexactly(4)
            length = int.from_bytes(header, "big")
            rest = await reader.readexactly(length - 4)
            pdu = pdumod.decode(header + rest)
            if pdu.command_id == CommandId.BIND_TRANSCEIVER:
                writer.write(
                    BindTransceiverResp(sequence_number=pdu.sequence_number, system_id="s").encode()
                )
                await writer.drain()
            # submit_sm: deliberately no response
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


async def test_submit_timeout_is_distinct() -> None:
    server = await asyncio.start_server(_silent_smsc, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    reader, writer = await open_connection("127.0.0.1", port)
    session = SmppSession(reader, writer, SessionConfig(response_timeout=0.3))
    session.start()
    await session.bind(BindTransceiver(system_id="e", password="p"), SessionState.BOUND_TRX)

    with pytest.raises(SubmitTimeout):
        await session.request(SubmitSm(destination_addr="1", short_message=b"x"), 0.3)
    assert session.window_used == 0  # timed-out request removed from window

    await session.close()
    server.close()
    await server.wait_closed()

from __future__ import annotations

import asyncio

import pytest

from relay.common.config import QueueConfig, SmppIngressConfig
from relay.ingress.smpp_connector import SmppIngress
from relay.smpp.constants import CommandStatus, Ton
from relay.smpp.pdu import BindTransceiver, SubmitSm, SubmitSmResp
from relay.smpp.server import ServerConfig, SmppServer, SubmitHandler
from relay.smpp.session import (
    BindError,
    SessionConfig,
    SessionState,
    SmppSession,
    open_connection,
)


async def _bound_session(port: int, password: str = "pw") -> SmppSession:
    reader, writer = await open_connection("127.0.0.1", port)
    session = SmppSession(reader, writer, SessionConfig(response_timeout=2.0))
    session.start()
    await session.bind(BindTransceiver(system_id="esme", password=password), SessionState.BOUND_TRX)
    return session


async def _server(handler: SubmitHandler, **cfg: object) -> SmppServer:
    server = SmppServer(
        "127.0.0.1",
        0,
        ServerConfig(credentials={"esme": "pw"}, **cfg),  # type: ignore[arg-type]
        handler,
    )
    await server.start()
    return server


async def _ok(pdu: SubmitSm, system_id: str) -> tuple[int, str]:
    return int(CommandStatus.ESME_ROK), "MID-1"


async def test_server_bind_and_submit_under_100ms() -> None:
    received: list[tuple[SubmitSm, str]] = []

    async def handler(pdu: SubmitSm, system_id: str) -> tuple[int, str]:
        received.append((pdu, system_id))
        return int(CommandStatus.ESME_ROK), "MID-1"

    server = await _server(handler)
    session = await _bound_session(server.port)
    start = asyncio.get_running_loop().time()
    resp = await session.request(
        SubmitSm(
            destination_addr="40712345678",
            dest_addr_ton=int(Ton.INTERNATIONAL),
            short_message=b"hi",
        ),
        2.0,
    )
    elapsed = asyncio.get_running_loop().time() - start
    assert isinstance(resp, SubmitSmResp)
    assert resp.command_status == CommandStatus.ESME_ROK
    assert resp.message_id == "MID-1"
    assert received and received[0][1] == "esme"
    assert elapsed < 0.1  # responds under 100 ms
    await session.unbind()
    await server.stop()


async def test_server_rejects_bad_password() -> None:
    server = await _server(_ok)
    reader, writer = await open_connection("127.0.0.1", server.port)
    session = SmppSession(reader, writer, SessionConfig(response_timeout=2.0))
    session.start()
    with pytest.raises(BindError) as exc:
        await session.bind(
            BindTransceiver(system_id="esme", password="WRONG"), SessionState.BOUND_TRX
        )
    assert exc.value.status == CommandStatus.ESME_RINVPASWD
    await server.stop()


async def test_server_unknown_system_id() -> None:
    server = await _server(_ok)
    reader, writer = await open_connection("127.0.0.1", server.port)
    session = SmppSession(reader, writer, SessionConfig(response_timeout=2.0))
    session.start()
    with pytest.raises(BindError) as exc:
        await session.bind(
            BindTransceiver(system_id="ghost", password="pw"), SessionState.BOUND_TRX
        )
    assert exc.value.status == CommandStatus.ESME_RINVSYSID
    await server.stop()


async def test_submit_before_bind_is_rejected() -> None:
    server = await _server(_ok)
    reader, writer = await open_connection("127.0.0.1", server.port)
    session = SmppSession(reader, writer, SessionConfig(response_timeout=2.0))
    session.start()
    resp = await session.request(SubmitSm(destination_addr="1", short_message=b"x"), 2.0)
    assert resp.command_status == CommandStatus.ESME_RINVBNDSTS
    await session.close()
    await server.stop()


async def test_max_binds_per_system_enforced() -> None:
    server = await _server(_ok, max_binds_per_system=1)
    first = await _bound_session(server.port)  # uses the one allowed bind
    reader, writer = await open_connection("127.0.0.1", server.port)
    second = SmppSession(reader, writer, SessionConfig(response_timeout=2.0))
    second.start()
    with pytest.raises(BindError) as exc:
        await second.bind(BindTransceiver(system_id="esme", password="pw"), SessionState.BOUND_TRX)
    assert exc.value.status == CommandStatus.ESME_RBINDFAIL
    await first.unbind()
    await server.stop()


async def test_ingress_rmsgqful_when_buffer_full() -> None:
    # Tiny buffer, no publishers running -> the queue fills and _on_submit returns
    # ESME_RMSGQFUL instead of buffering without limit.
    config = SmppIngressConfig(internal_queue_maxsize=1, queue=QueueConfig(backend="memory"))
    ingress = SmppIngress(config)
    submit = SubmitSm(
        destination_addr="40712345678", dest_addr_ton=int(Ton.INTERNATIONAL), short_message=b"x"
    )
    status1, mid1 = await ingress._on_submit(submit, "esme")
    status2, _ = await ingress._on_submit(submit, "esme")
    assert status1 == CommandStatus.ESME_ROK and mid1
    assert status2 == CommandStatus.ESME_RMSGQFUL


async def test_ingress_preserves_smpp_attributes() -> None:
    config = SmppIngressConfig(internal_queue_maxsize=10, queue=QueueConfig(backend="memory"))
    ingress = SmppIngress(config)
    submit = SubmitSm(
        destination_addr="40712345678",
        dest_addr_ton=int(Ton.INTERNATIONAL),
        esm_class=0x40,
        data_coding=8,
        short_message=b"\x05\x00\x03\x01\x02\x01raw",
    )
    await ingress._on_submit(submit, "esme")
    msg = ingress._queue.get_nowait()
    assert msg.source == "smpp"
    assert msg.to == "+40712345678"
    assert msg.attributes["smpp.esm_class"] == "0x40"
    assert msg.attributes["smpp.data_coding"] == "8"
    assert msg.attributes["smpp.raw"] == b"\x05\x00\x03\x01\x02\x01raw".hex()

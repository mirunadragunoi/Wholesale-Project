from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

from relay.common.config import SmppEgressConfig
from relay.common.message import Message
from relay.egress.smpp_connector import SmppEgressConnector
from relay.queues.base import ReceivedMessage
from relay.smpp.constants import CommandStatus

# tools/ is not a package; add it to the path to reuse the simulated provider.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tools"))
import smpp_sink


def _msg(text: str = "hello") -> ReceivedMessage:
    m = Message(
        id="01ULIDTEST",
        to="+40712345678",
        text=text,
        sender=None,
        source="http",
        received_at=time.time(),
        attributes={},
    )
    return ReceivedMessage(m, handle="h1", sent_at=None)


async def _start_sink(config: smpp_sink.SinkConfig) -> tuple[asyncio.AbstractServer, int]:
    sink = smpp_sink.Sink(config)
    server = await asyncio.start_server(sink.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


async def _connector(port: int, registered_delivery: int = 0) -> SmppEgressConnector:
    cfg = SmppEgressConfig(
        host="127.0.0.1",
        port=port,
        bind_count=1,
        system_id="e",
        password="p",
        registered_delivery=registered_delivery,
    )
    conn = SmppEgressConnector(cfg)
    conn.start()
    for _ in range(100):  # wait for the bind to become ready
        if any(b is not None for b in conn._binds):
            return conn
        await asyncio.sleep(0.02)
    raise AssertionError("bind never became ready")


async def test_submit_ack_deletes() -> None:
    server, port = await _start_sink(smpp_sink.SinkConfig())
    conn = await _connector(port)
    deleted = await conn.handle([_msg()])
    assert deleted == ["h1"]
    await conn.stop()
    server.close()
    await server.wait_closed()


async def test_permanent_error_is_dropped() -> None:
    # RINVDSTADR is permanent -> dropped (deleted), never retried.
    server, port = await _start_sink(
        smpp_sink.SinkConfig(error_rate=1.0, error_status=CommandStatus.ESME_RINVDSTADR)
    )
    conn = await _connector(port)
    deleted = await conn.handle([_msg()])
    assert deleted == ["h1"]  # dropped from queue
    await conn.stop()
    server.close()
    await server.wait_closed()


async def test_temporary_error_is_retried() -> None:
    # RSUBMITFAIL is temporary -> NOT deleted, left for redelivery.
    server, port = await _start_sink(
        smpp_sink.SinkConfig(error_rate=1.0, error_status=CommandStatus.ESME_RSUBMITFAIL)
    )
    conn = await _connector(port)
    deleted = await conn.handle([_msg()])
    assert deleted == []  # retried via redelivery
    await conn.stop()
    server.close()
    await server.wait_closed()


async def test_dlr_correlates_when_formats_match() -> None:
    server, port = await _start_sink(
        smpp_sink.SinkConfig(
            dlr_enabled=True, submit_id_format="hex", dlr_id_format="hex", dlr_delay_ms=30
        )
    )
    conn = await _connector(port, registered_delivery=1)
    await conn.handle([_msg()])
    for _ in range(50):
        if conn.dlr_count:
            break
        await asyncio.sleep(0.02)
    assert conn.dlr_count == 1
    assert conn.dlr_correlated == 1
    assert conn.dlr_missed == 0
    await conn.stop()
    server.close()
    await server.wait_closed()


async def test_dlr_correlation_fails_on_hex_dec_mismatch() -> None:
    # The classic integration trap: hex in submit_resp, decimal in the DLR.
    server, port = await _start_sink(
        smpp_sink.SinkConfig(
            dlr_enabled=True, submit_id_format="hex", dlr_id_format="dec", dlr_delay_ms=30
        )
    )
    conn = await _connector(port, registered_delivery=1)
    # ids 1-9 render identically in hex and decimal; id >= 10 differs
    # (10 -> hex "a" vs dec "10"), so submitting 12 guarantees mismatches.
    for _ in range(12):
        await conn.handle([_msg()])
    for _ in range(100):
        if conn.dlr_count >= 12:
            break
        await asyncio.sleep(0.02)
    assert conn.dlr_missed >= 1  # the hex/dec trap causes correlation misses
    await conn.stop()
    server.close()
    await server.wait_closed()


async def test_sink_counts_duplicate_ulids() -> None:
    # The connector carries our ULID in a TLV; the sink dedups by it.
    sink = smpp_sink.Sink(smpp_sink.SinkConfig())
    server = await asyncio.start_server(sink.handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    conn = await _connector(port)
    await conn.handle([_msg()])  # ULID 01ULIDTEST
    await conn.handle([_msg()])  # same ULID again -> duplicate
    assert sink.submits == 2
    assert sink.with_id == 2
    assert len(sink._seen_ids) == 1
    assert sink.duplicates == 1
    await conn.stop()
    server.close()
    await server.wait_closed()


@pytest.mark.parametrize("text", ["salut", "mesaj în română ăâîșț", "a" * 200])
async def test_various_encodings_ack(text: str) -> None:
    server, port = await _start_sink(smpp_sink.SinkConfig())
    conn = await _connector(port)
    deleted = await conn.handle([_msg(text)])
    assert deleted == ["h1"]
    await conn.stop()
    server.close()
    await server.wait_closed()

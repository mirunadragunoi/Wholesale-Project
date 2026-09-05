from __future__ import annotations

import time

import pytest

from relay.common.ids import new_ulid
from relay.common.message import Message, get_serializer


def _sample() -> Message:
    return Message(
        id=new_ulid(),
        to="+40712345678",
        text="hello",
        sender="RELAY",
        source="http",
        received_at=time.time(),
        attributes={"k": "v"},
    )


@pytest.mark.parametrize("name", ["json", "msgpack"])
def test_serializer_roundtrip(name: str) -> None:
    ser = get_serializer(name)
    msg = _sample()
    assert ser.decode(ser.encode(msg)) == msg


@pytest.mark.parametrize("name", ["json", "msgpack"])
def test_serializer_roundtrip_none_sender_empty_attrs(name: str) -> None:
    ser = get_serializer(name)
    msg = Message(
        id=new_ulid(), to="+40700000000", text="", sender=None,
        source="csv", received_at=1.5, attributes={},
    )
    assert ser.decode(ser.encode(msg)) == msg


def test_unknown_serializer() -> None:
    with pytest.raises(ValueError):
        get_serializer("protobuf")


def test_received_at_preserved_exactly() -> None:
    ser = get_serializer("json")
    msg = _sample()
    assert ser.decode(ser.encode(msg)).received_at == msg.received_at

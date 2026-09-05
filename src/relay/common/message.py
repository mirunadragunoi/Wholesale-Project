"""The canonical message envelope and its wire serialization.

Every ingress connector produces exactly a ``Message``. Nothing else travels
through the queues. ``received_at`` is stamped once at ingress and propagated
unchanged so end-to-end latency can be measured at egress.

Two serializers are provided — JSON (compact) and msgpack — because at high
volume the serialization cost is non-trivial; ``docs/BENCHMARKS.md`` records the
measured difference.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

import msgpack


@dataclass(frozen=True, slots=True)
class Message:
    id: str  # ULID, generated at ingress
    to: str  # E.164, with leading '+'
    text: str
    sender: str | None  # sender ID, optional
    source: str  # "http" | "smpp" | "csv"
    received_at: float  # time.time() at ingress, for end-to-end latency
    attributes: dict[str, str] = field(default_factory=dict)


class Serializer(Protocol):
    """Encodes/decodes a Message to/from bytes for the queue."""

    name: str

    def encode(self, message: Message) -> bytes: ...

    def decode(self, data: bytes) -> Message: ...


def _message_from_mapping(obj: dict[str, Any]) -> Message:
    return Message(
        id=obj["id"],
        to=obj["to"],
        text=obj["text"],
        sender=obj["sender"],
        source=obj["source"],
        received_at=obj["received_at"],
        attributes=dict(obj.get("attributes", {})),
    )


class JsonSerializer:
    name = "json"

    def encode(self, message: Message) -> bytes:
        return json.dumps(asdict(message), separators=(",", ":")).encode("utf-8")

    def decode(self, data: bytes) -> Message:
        return _message_from_mapping(json.loads(data))


class MsgpackSerializer:
    name = "msgpack"

    def encode(self, message: Message) -> bytes:
        packed: bytes = msgpack.packb(asdict(message), use_bin_type=True)
        return packed

    def decode(self, data: bytes) -> Message:
        obj: dict[str, Any] = msgpack.unpackb(data, raw=False)
        return _message_from_mapping(obj)


_SERIALIZERS: dict[str, Serializer] = {
    JsonSerializer.name: JsonSerializer(),
    MsgpackSerializer.name: MsgpackSerializer(),
}


def get_serializer(name: str) -> Serializer:
    try:
        return _SERIALIZERS[name]
    except KeyError:
        raise ValueError(
            f"unknown serializer {name!r}; known: {sorted(_SERIALIZERS)}"
        ) from None

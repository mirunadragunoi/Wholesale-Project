"""SMPP v3.4 PDU codec — header, fields, TLVs, and the PDUs the POC needs.

Wire format is big-endian. The 16-byte header is:

    command_length (u32) | command_id (u32) | command_status (u32) | sequence_number (u32)

Field types on the wire:
  * integer          — fixed width, big-endian
  * C-Octet String   — NUL-terminated bytes
  * Octet String     — fixed length (length carried by a preceding field or TLV)
  * TLV              — tag (u16) | length (u16) | value (length bytes)

Robustness is a first-class requirement: a truncated or inconsistent PDU must
raise ``PduError``, never crash the connector. The caller decides whether to
answer with generic_nack.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import ClassVar

from relay.smpp.constants import CommandId

HEADER_SIZE = 16
MAX_PDU_SIZE = 1 << 20  # 1 MiB sanity cap; real PDUs are far smaller

Tlvs = tuple[tuple[int, bytes], ...]


class PduError(ValueError):
    """Raised on any malformed / truncated / inconsistent PDU."""


# --------------------------------------------------------------------------- #
# Low-level readers / writers
# --------------------------------------------------------------------------- #
class Reader:
    __slots__ = ("_data", "_pos")

    def __init__(self, data: bytes) -> None:
        self._data = data
        self._pos = 0

    @property
    def remaining(self) -> int:
        return len(self._data) - self._pos

    def uint(self, width: int) -> int:
        if self.remaining < width:
            raise PduError(f"need {width} bytes for integer, {self.remaining} left")
        value = int.from_bytes(self._data[self._pos : self._pos + width], "big")
        self._pos += width
        return value

    def c_octet_string(self, max_len: int | None = None) -> str:
        end = self._data.find(b"\x00", self._pos)
        if end == -1:
            raise PduError("unterminated C-Octet String")
        raw = self._data[self._pos : end]
        if max_len is not None and len(raw) + 1 > max_len:
            raise PduError(f"C-Octet String too long: {len(raw) + 1} > {max_len}")
        self._pos = end + 1
        return raw.decode("latin-1")

    def octet_string(self, length: int) -> bytes:
        if length < 0 or self.remaining < length:
            raise PduError(f"need {length} bytes for octet string, {self.remaining} left")
        raw = self._data[self._pos : self._pos + length]
        self._pos += length
        return bytes(raw)

    def rest(self) -> bytes:
        raw = self._data[self._pos :]
        self._pos = len(self._data)
        return bytes(raw)


class Writer:
    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def uint(self, value: int, width: int) -> None:
        if value < 0 or value >= (1 << (8 * width)):
            raise PduError(f"integer {value} out of range for {width} bytes")
        self._buf += value.to_bytes(width, "big")

    def c_octet_string(self, value: str, max_len: int) -> None:
        raw = value.encode("latin-1")
        if len(raw) + 1 > max_len:
            raise PduError(f"C-Octet String too long: {len(raw) + 1} > {max_len}")
        self._buf += raw
        self._buf += b"\x00"

    def octet_string(self, value: bytes) -> None:
        self._buf += value

    def bytes(self) -> bytes:
        return bytes(self._buf)


def encode_tlvs(tlvs: Tlvs) -> bytes:
    out = bytearray()
    for tag, value in tlvs:
        out += struct.pack(">HH", tag, len(value))
        out += value
    return bytes(out)


def decode_tlvs(reader: Reader) -> Tlvs:
    tlvs: list[tuple[int, bytes]] = []
    while reader.remaining >= 4:
        tag = reader.uint(2)
        length = reader.uint(2)
        value = reader.octet_string(length)
        tlvs.append((tag, value))
    if reader.remaining != 0:
        raise PduError(f"trailing {reader.remaining} bytes not a valid TLV")
    return tuple(tlvs)


# --------------------------------------------------------------------------- #
# PDU definitions
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class PDU:
    command_status: int = 0
    sequence_number: int = 0

    command_id: ClassVar[CommandId]

    def encode_body(self) -> bytes:  # pragma: no cover - overridden
        return b""

    def encode(self) -> bytes:
        body = self.encode_body()
        length = HEADER_SIZE + len(body)
        header = struct.pack(
            ">IIII", length, int(self.command_id), self.command_status, self.sequence_number
        )
        return header + body


@dataclass(frozen=True, slots=True)
class Bind(PDU):
    system_id: str = ""
    password: str = ""
    system_type: str = ""
    interface_version: int = 0x34
    addr_ton: int = 0
    addr_npi: int = 0
    address_range: str = ""

    # command_id is set per subclass (TX/RX/TRX share this body).
    def encode_body(self) -> bytes:
        w = Writer()
        w.c_octet_string(self.system_id, 16)
        w.c_octet_string(self.password, 9)
        w.c_octet_string(self.system_type, 13)
        w.uint(self.interface_version, 1)
        w.uint(self.addr_ton, 1)
        w.uint(self.addr_npi, 1)
        w.c_octet_string(self.address_range, 41)
        return w.bytes()

    @classmethod
    def _read(cls, r: Reader, status: int, seq: int) -> Bind:
        return cls(
            command_status=status,
            sequence_number=seq,
            system_id=r.c_octet_string(16),
            password=r.c_octet_string(9),
            system_type=r.c_octet_string(13),
            interface_version=r.uint(1),
            addr_ton=r.uint(1),
            addr_npi=r.uint(1),
            address_range=r.c_octet_string(41),
        )


@dataclass(frozen=True, slots=True)
class BindTransmitter(Bind):
    command_id: ClassVar[CommandId] = CommandId.BIND_TRANSMITTER


@dataclass(frozen=True, slots=True)
class BindReceiver(Bind):
    command_id: ClassVar[CommandId] = CommandId.BIND_RECEIVER


@dataclass(frozen=True, slots=True)
class BindTransceiver(Bind):
    command_id: ClassVar[CommandId] = CommandId.BIND_TRANSCEIVER


@dataclass(frozen=True, slots=True)
class BindResp(PDU):
    system_id: str = ""
    tlvs: Tlvs = ()

    def encode_body(self) -> bytes:
        w = Writer()
        w.c_octet_string(self.system_id, 16)
        return w.bytes() + encode_tlvs(self.tlvs)

    @classmethod
    def _read(cls, r: Reader, status: int, seq: int) -> BindResp:
        # On a failed bind the body may be empty (only the error status matters).
        system_id = r.c_octet_string(16) if r.remaining else ""
        return cls(
            command_status=status,
            sequence_number=seq,
            system_id=system_id,
            tlvs=decode_tlvs(r),
        )


@dataclass(frozen=True, slots=True)
class BindTransmitterResp(BindResp):
    command_id: ClassVar[CommandId] = CommandId.BIND_TRANSMITTER_RESP


@dataclass(frozen=True, slots=True)
class BindReceiverResp(BindResp):
    command_id: ClassVar[CommandId] = CommandId.BIND_RECEIVER_RESP


@dataclass(frozen=True, slots=True)
class BindTransceiverResp(BindResp):
    command_id: ClassVar[CommandId] = CommandId.BIND_TRANSCEIVER_RESP


@dataclass(frozen=True, slots=True)
class SubmitSm(PDU):
    command_id: ClassVar[CommandId] = CommandId.SUBMIT_SM

    service_type: str = ""
    source_addr_ton: int = 0
    source_addr_npi: int = 0
    source_addr: str = ""
    dest_addr_ton: int = 0
    dest_addr_npi: int = 0
    destination_addr: str = ""
    esm_class: int = 0
    protocol_id: int = 0
    priority_flag: int = 0
    schedule_delivery_time: str = ""
    validity_period: str = ""
    registered_delivery: int = 0
    replace_if_present_flag: int = 0
    data_coding: int = 0
    sm_default_msg_id: int = 0
    short_message: bytes = b""
    tlvs: Tlvs = ()

    def encode_body(self) -> bytes:
        if len(self.short_message) > 254:
            raise PduError("short_message > 254 bytes; use message_payload TLV")
        w = Writer()
        w.c_octet_string(self.service_type, 6)
        w.uint(self.source_addr_ton, 1)
        w.uint(self.source_addr_npi, 1)
        w.c_octet_string(self.source_addr, 21)
        w.uint(self.dest_addr_ton, 1)
        w.uint(self.dest_addr_npi, 1)
        w.c_octet_string(self.destination_addr, 21)
        w.uint(self.esm_class, 1)
        w.uint(self.protocol_id, 1)
        w.uint(self.priority_flag, 1)
        w.c_octet_string(self.schedule_delivery_time, 17)
        w.c_octet_string(self.validity_period, 17)
        w.uint(self.registered_delivery, 1)
        w.uint(self.replace_if_present_flag, 1)
        w.uint(self.data_coding, 1)
        w.uint(self.sm_default_msg_id, 1)
        w.uint(len(self.short_message), 1)
        w.octet_string(self.short_message)
        return w.bytes() + encode_tlvs(self.tlvs)

    @classmethod
    def _read(cls, r: Reader, status: int, seq: int) -> SubmitSm:
        service_type = r.c_octet_string(6)
        source_addr_ton = r.uint(1)
        source_addr_npi = r.uint(1)
        source_addr = r.c_octet_string(21)
        dest_addr_ton = r.uint(1)
        dest_addr_npi = r.uint(1)
        destination_addr = r.c_octet_string(21)
        esm_class = r.uint(1)
        protocol_id = r.uint(1)
        priority_flag = r.uint(1)
        schedule_delivery_time = r.c_octet_string(17)
        validity_period = r.c_octet_string(17)
        registered_delivery = r.uint(1)
        replace_if_present_flag = r.uint(1)
        data_coding = r.uint(1)
        sm_default_msg_id = r.uint(1)
        sm_length = r.uint(1)
        short_message = r.octet_string(sm_length)
        return cls(
            command_status=status,
            sequence_number=seq,
            service_type=service_type,
            source_addr_ton=source_addr_ton,
            source_addr_npi=source_addr_npi,
            source_addr=source_addr,
            dest_addr_ton=dest_addr_ton,
            dest_addr_npi=dest_addr_npi,
            destination_addr=destination_addr,
            esm_class=esm_class,
            protocol_id=protocol_id,
            priority_flag=priority_flag,
            schedule_delivery_time=schedule_delivery_time,
            validity_period=validity_period,
            registered_delivery=registered_delivery,
            replace_if_present_flag=replace_if_present_flag,
            data_coding=data_coding,
            sm_default_msg_id=sm_default_msg_id,
            short_message=short_message,
            tlvs=decode_tlvs(r),
        )


@dataclass(frozen=True, slots=True)
class DeliverSm(SubmitSm):
    command_id: ClassVar[CommandId] = CommandId.DELIVER_SM


@dataclass(frozen=True, slots=True)
class SubmitSmResp(PDU):
    command_id: ClassVar[CommandId] = CommandId.SUBMIT_SM_RESP
    message_id: str = ""

    def encode_body(self) -> bytes:
        w = Writer()
        w.c_octet_string(self.message_id, 65)
        return w.bytes()

    @classmethod
    def _read(cls, r: Reader, status: int, seq: int) -> SubmitSmResp:
        # A failed submit carries no body (only the error status).
        message_id = r.c_octet_string(65) if r.remaining else ""
        return cls(command_status=status, sequence_number=seq, message_id=message_id)


@dataclass(frozen=True, slots=True)
class DeliverSmResp(PDU):
    command_id: ClassVar[CommandId] = CommandId.DELIVER_SM_RESP
    message_id: str = ""

    def encode_body(self) -> bytes:
        w = Writer()
        w.c_octet_string(self.message_id, 65)
        return w.bytes()

    @classmethod
    def _read(cls, r: Reader, status: int, seq: int) -> DeliverSmResp:
        message_id = r.c_octet_string(65) if r.remaining else ""
        return cls(command_status=status, sequence_number=seq, message_id=message_id)


@dataclass(frozen=True, slots=True)
class _Empty(PDU):
    """PDUs with no body: enquire_link(_resp), unbind(_resp), generic_nack."""

    @classmethod
    def _read(cls, r: Reader, status: int, seq: int) -> _Empty:
        return cls(command_status=status, sequence_number=seq)


@dataclass(frozen=True, slots=True)
class EnquireLink(_Empty):
    command_id: ClassVar[CommandId] = CommandId.ENQUIRE_LINK


@dataclass(frozen=True, slots=True)
class EnquireLinkResp(_Empty):
    command_id: ClassVar[CommandId] = CommandId.ENQUIRE_LINK_RESP


@dataclass(frozen=True, slots=True)
class Unbind(_Empty):
    command_id: ClassVar[CommandId] = CommandId.UNBIND


@dataclass(frozen=True, slots=True)
class UnbindResp(_Empty):
    command_id: ClassVar[CommandId] = CommandId.UNBIND_RESP


@dataclass(frozen=True, slots=True)
class GenericNack(_Empty):
    command_id: ClassVar[CommandId] = CommandId.GENERIC_NACK


# --------------------------------------------------------------------------- #
# Dispatch
# --------------------------------------------------------------------------- #
_DECODERS = {
    CommandId.BIND_TRANSMITTER: BindTransmitter._read,
    CommandId.BIND_RECEIVER: BindReceiver._read,
    CommandId.BIND_TRANSCEIVER: BindTransceiver._read,
    CommandId.BIND_TRANSMITTER_RESP: BindTransmitterResp._read,
    CommandId.BIND_RECEIVER_RESP: BindReceiverResp._read,
    CommandId.BIND_TRANSCEIVER_RESP: BindTransceiverResp._read,
    CommandId.SUBMIT_SM: SubmitSm._read,
    CommandId.SUBMIT_SM_RESP: SubmitSmResp._read,
    CommandId.DELIVER_SM: DeliverSm._read,
    CommandId.DELIVER_SM_RESP: DeliverSmResp._read,
    CommandId.ENQUIRE_LINK: EnquireLink._read,
    CommandId.ENQUIRE_LINK_RESP: EnquireLinkResp._read,
    CommandId.UNBIND: Unbind._read,
    CommandId.UNBIND_RESP: UnbindResp._read,
    CommandId.GENERIC_NACK: GenericNack._read,
}


def peek_length(data: bytes) -> int:
    """Return command_length from the first 4 bytes (for framing on a stream)."""
    if len(data) < 4:
        raise PduError("need 4 bytes to read command_length")
    return int.from_bytes(data[:4], "big")


def decode(data: bytes) -> PDU:
    """Decode a single complete PDU. Raises PduError on anything malformed."""
    if len(data) < HEADER_SIZE:
        raise PduError(f"PDU shorter than header: {len(data)} < {HEADER_SIZE}")
    command_length, command_id_raw, command_status, sequence_number = struct.unpack(
        ">IIII", data[:HEADER_SIZE]
    )
    if command_length != len(data):
        raise PduError(f"command_length {command_length} != actual {len(data)}")
    if command_length > MAX_PDU_SIZE:
        raise PduError(f"command_length {command_length} exceeds cap {MAX_PDU_SIZE}")
    try:
        command_id = CommandId(command_id_raw)
    except ValueError:
        raise PduError(f"unknown command_id 0x{command_id_raw:08x}") from None
    decoder = _DECODERS.get(command_id)
    if decoder is None:
        raise PduError(f"unsupported command_id {command_id.name}")
    reader = Reader(data[HEADER_SIZE:])
    return decoder(reader, command_status, sequence_number)

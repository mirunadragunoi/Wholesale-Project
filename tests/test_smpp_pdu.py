from __future__ import annotations

import pytest

from relay.smpp import pdu
from relay.smpp.constants import CommandId
from relay.smpp.pdu import (
    BindTransceiver,
    BindTransceiverResp,
    EnquireLink,
    GenericNack,
    PduError,
    SubmitSm,
    SubmitSmResp,
    decode,
)


def test_header_framing_enquire_link() -> None:
    # command_length=0x10, command_id=0x15, status=0, seq=1
    expected = bytes.fromhex("00000010 00000015 00000000 00000001".replace(" ", ""))
    assert EnquireLink(sequence_number=1).encode() == expected


def test_submit_sm_byte_vector() -> None:
    """Hand-written expected bytes, not a round-trip."""
    p = SubmitSm(
        sequence_number=7,
        dest_addr_ton=1,
        dest_addr_npi=1,
        destination_addr="12345",
        short_message=b"hello",
    )
    expected = bytes.fromhex(
        "0000002b"  # command_length = 43
        "00000004"  # command_id = submit_sm
        "00000000"  # status
        "00000007"  # sequence_number
        "00"  # service_type ""
        "00"  # source_addr_ton
        "00"  # source_addr_npi
        "00"  # source_addr ""
        "01"  # dest_addr_ton
        "01"  # dest_addr_npi
        "3132333435" + "00"  # destination_addr "12345\0"
        "00"  # esm_class
        "00"  # protocol_id
        "00"  # priority_flag
        "00"  # schedule_delivery_time ""
        "00"  # validity_period ""
        "00"  # registered_delivery
        "00"  # replace_if_present_flag
        "00"  # data_coding
        "00"  # sm_default_msg_id
        "05"  # sm_length
        "68656c6c6f"  # "hello"
    )
    assert p.encode() == expected


@pytest.mark.parametrize(
    "obj",
    [
        EnquireLink(sequence_number=9),
        GenericNack(sequence_number=3),
        BindTransceiver(
            sequence_number=1, system_id="esme", password="pw", system_type="", addr_ton=1
        ),
        BindTransceiverResp(sequence_number=1, system_id="smsc01"),
        SubmitSm(sequence_number=2, destination_addr="40712345678", short_message=b"hi"),
        SubmitSmResp(sequence_number=2, message_id="deadbeef"),
    ],
)
def test_roundtrip(obj: pdu.PDU) -> None:
    decoded = decode(obj.encode())
    assert decoded == obj
    assert decoded.command_id == obj.command_id


def test_submit_sm_with_tlv_roundtrip() -> None:
    p = SubmitSm(
        sequence_number=5,
        destination_addr="123",
        short_message=b"x",
        tlvs=((0x0424, b"payload"),),
    )
    decoded = decode(p.encode())
    assert isinstance(decoded, SubmitSm)
    assert decoded.tlvs == ((0x0424, b"payload"),)


def test_response_bit_helpers() -> None:
    assert CommandId.SUBMIT_SM.response_id() == CommandId.SUBMIT_SM_RESP
    assert CommandId.SUBMIT_SM_RESP.is_response
    assert not CommandId.SUBMIT_SM.is_response


# --- Robustness: malformed input must raise PduError, never crash ---


def test_truncated_header() -> None:
    with pytest.raises(PduError):
        decode(b"\x00\x00\x00\x10\x00\x00")


def test_command_length_mismatch() -> None:
    good = EnquireLink(sequence_number=1).encode()
    with pytest.raises(PduError):
        decode(good + b"\x00")  # extra byte, length no longer matches


def test_unknown_command_id() -> None:
    body = (16).to_bytes(4, "big") + (0x00001234).to_bytes(4, "big") + b"\x00" * 8
    with pytest.raises(PduError):
        decode(body)


def test_unterminated_c_octet_string() -> None:
    # A bind body whose system_id never terminates.
    body = b"esme-no-null-ever"
    length = pdu.HEADER_SIZE + len(body)
    raw = (
        length.to_bytes(4, "big")
        + int(CommandId.BIND_TRANSCEIVER).to_bytes(4, "big")
        + b"\x00" * 8
        + body
    )
    with pytest.raises(PduError):
        decode(raw)


def test_short_message_too_long_raises() -> None:
    with pytest.raises(PduError):
        SubmitSm(destination_addr="1", short_message=b"x" * 255).encode()

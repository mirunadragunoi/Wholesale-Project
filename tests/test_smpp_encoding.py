from __future__ import annotations

from relay.smpp.constants import DataCoding, EsmClass
from relay.smpp.encoding import (
    can_encode_gsm7,
    decode_segment,
    detect_encoding,
    encode_message,
    gsm7_positions,
    pack_gsm7,
    ucs2_positions,
    unpack_gsm7,
)


def test_pack_gsm7_hello_known_vector() -> None:
    # "hello" (5 septets = 35 bits) packs into 5 octets; the last octet holds
    # o's top 3 bits (0,1,1 = 0x06) plus 5 zero pad bits. Derived by hand.
    septets = [0x68, 0x65, 0x6C, 0x6C, 0x6F]
    assert pack_gsm7(septets) == bytes.fromhex("e8329bfd06")


def test_pack_unpack_roundtrip() -> None:
    septets = [0x00, 0x7F, 0x41, 0x1B, 0x65, 0x20]
    packed = pack_gsm7(septets)
    assert unpack_gsm7(packed, len(septets)) == septets


def test_detect_ascii_is_gsm7() -> None:
    assert detect_encoding("Hello world 123") == "gsm7"


def test_detect_romanian_is_ucs2() -> None:
    # ă â î ș ț are not in GSM 03.38.
    for ch in "ăâîșț":
        assert not can_encode_gsm7(ch)
    assert detect_encoding("mesaj în română") == "ucs2"


def test_extension_chars_cost_two_positions() -> None:
    # € and [ are extension-table characters: 2 septets each.
    assert can_encode_gsm7("€[")
    assert gsm7_positions("€[") == 4
    assert gsm7_positions("a€b") == 4  # 1 + 2 + 1


def test_gsm7_160_is_one_segment_161_is_two() -> None:
    m160 = encode_message("a" * 160)
    assert m160.encoding == "gsm7"
    assert m160.total_segments == 1

    m161 = encode_message("a" * 161)
    assert m161.total_segments == 2


def test_gsm7_extension_boundary_counts() -> None:
    # 80 '€' = 160 positions -> still one segment; 81 -> two.
    assert encode_message("€" * 80).total_segments == 1
    assert encode_message("€" * 81).total_segments == 2


def test_romanian_100_chars_switches_to_ucs2_two_segments() -> None:
    text = ("ăâîșț " * 20)[:100]  # exactly 100 real Romanian characters
    assert len(text) == 100
    msg = encode_message(text)
    assert msg.encoding == "ucs2"
    assert ucs2_positions(text) == 100
    assert msg.total_segments == 2  # 100 > 70 -> concatenated (67 + 33)
    assert msg.segments[0].data_coding == DataCoding.UCS2


def test_emoji_is_surrogate_pair_two_positions() -> None:
    grin = "\U0001f600"  # 😀, supplementary plane
    assert detect_encoding(grin) == "ucs2"
    assert ucs2_positions(grin) == 2
    assert encode_message(grin).segments[0].data == grin.encode("utf-16-be")


def test_ucs2_single_segment_bytes() -> None:
    # 日本語 is clearly outside GSM 03.38 -> UCS-2.
    msg = encode_message("日本語")
    assert msg.encoding == "ucs2"
    assert msg.total_segments == 1
    assert msg.segments[0].data == "日本語".encode("utf-16-be")
    assert msg.segments[0].esm_class == EsmClass.DEFAULT


def test_concatenated_gsm7_udh_bytes() -> None:
    ref = 0x42
    msg = encode_message("a" * 200, ref=ref)  # 200 septets -> 153 + 47
    assert msg.total_segments == 2
    for seq, seg in enumerate(msg.segments, start=1):
        assert seg.esm_class == EsmClass.UDHI
        # UDH: 05 00 03 <ref> <total> <seq>
        assert seg.data[:6] == bytes([0x05, 0x00, 0x03, ref, 2, seq])


def test_concatenated_ucs2_udh_bytes() -> None:
    ref = 0x07
    text = "😀" * 40  # 80 UCS-2 positions -> 67 + 13
    msg = encode_message(text, ref=ref)
    assert msg.encoding == "ucs2"
    assert msg.total_segments == 2
    for seq, seg in enumerate(msg.segments, start=1):
        assert seg.data[:6] == bytes([0x05, 0x00, 0x03, ref, 2, seq])
        # payload after UDH is UTF-16BE (even number of bytes)
        assert (len(seg.data) - 6) % 2 == 0


def test_decode_segment_gsm7_roundtrip() -> None:
    text = "Hello, world! 123 €[]"
    msg = encode_message(text)
    assert msg.encoding == "gsm7"
    seg = msg.segments[0]
    assert decode_segment(seg.data, seg.data_coding, seg.esm_class) == text


def test_decode_segment_ucs2_roundtrip() -> None:
    text = "Salut, în română: ăâîșț 🙂"
    msg = encode_message(text)
    assert msg.encoding == "ucs2"
    seg = msg.segments[0]
    assert decode_segment(seg.data, seg.data_coding, seg.esm_class) == text


def test_gsm7_segment_never_splits_extension_pair() -> None:
    # 152 'a' (152 septets) then 5 '€' (10 septets) = 162 positions -> 2 parts.
    # Adding the first '€' to part 1 would need 154 > 153, so it moves to part 2
    # whole; an extension escape+code pair is never cut across a segment.
    text = "a" * 152 + "€" * 5
    msg = encode_message(text)
    assert msg.total_segments == 2
    # Part 1 payload = UDH(6) + packed(152 septets). Part 2 = UDH(6) + packed(10).
    assert unpack_gsm7(msg.segments[0].data[6:], 152, fill_bits=1) == [0x61] * 152  # all 'a'

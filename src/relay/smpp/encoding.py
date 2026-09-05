"""GSM 03.38 / UCS-2 encoding and UDH segmentation.

Auto-detects the encoding: if every character fits GSM 03.38 (basic alphabet +
extension table) it is used; otherwise UCS-2 (UTF-16BE). Romanian diacritics
(ă â î ș ț) are NOT in GSM 03.38, so any Romanian text switches to UCS-2 — this
is exercised explicitly in the tests.

Segment limits (in the encoding's own "positions"):
  * GSM7 : 160 single, 153 per part concatenated
  * UCS-2:  70 single,  67 per part concatenated

Extension-table characters (| ^ € { } [ ] ~ \\) cost **two** GSM7 positions
(escape 0x1B + code). Supplementary-plane characters (emoji) are UTF-16 surrogate
pairs and cost **two** UCS-2 positions.

Concatenation uses a 6-octet UDH ``05 00 03 <ref> <total> <seq>`` and sets the
UDHI bit (``esm_class |= 0x40``). For GSM7 the packed 7-bit data is shifted by
one fill bit so the first septet starts on a septet boundary after the UDH.
"""

from __future__ import annotations

from dataclasses import dataclass

from relay.smpp.constants import DataCoding, EsmClass

# GSM 03.38 basic alphabet: index == septet value. Position 0x1B is the ESC
# escape and is never a literal character (sentinel below).
_GSM_BASIC = (
    "@£$¥èéùìòÇ\nØø\rÅå"
    "Δ_ΦΓΛΩΠΨΣΘΞ\x1bÆæßÉ"
    " !\"#¤%&'()*+,-./"
    "0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNO"
    "PQRSTUVWXYZÄÖÑÜ§"
    "¿abcdefghijklmno"
    "pqrstuvwxyzäöñüà"
)
_ESC = 0x1B

# Extension table: char -> second septet (emitted after 0x1B).
_GSM_EXT: dict[str, int] = {
    "\f": 0x0A,
    "^": 0x14,
    "{": 0x28,
    "}": 0x29,
    "\\": 0x2F,
    "[": 0x3C,
    "~": 0x3D,
    "]": 0x3E,
    "|": 0x40,
    "€": 0x65,
}

_BASIC_TO_SEPTET: dict[str, int] = {ch: i for i, ch in enumerate(_GSM_BASIC) if i != _ESC}
_SEPTET_TO_BASIC: dict[int, str] = {i: ch for ch, i in _BASIC_TO_SEPTET.items()}
_SEPTET_TO_EXT: dict[int, str] = {v: k for k, v in _GSM_EXT.items()}

GSM7_SINGLE = 160
GSM7_MULTI = 153
UCS2_SINGLE = 70
UCS2_MULTI = 67


@dataclass(frozen=True, slots=True)
class Segment:
    data: bytes  # short_message bytes (UDH + payload when concatenated)
    data_coding: int
    esm_class: int


@dataclass(frozen=True, slots=True)
class EncodedMessage:
    encoding: str  # "gsm7" | "ucs2"
    segments: list[Segment]

    @property
    def total_segments(self) -> int:
        return len(self.segments)


def can_encode_gsm7(text: str) -> bool:
    return all(ch in _BASIC_TO_SEPTET or ch in _GSM_EXT for ch in text)


def detect_encoding(text: str) -> str:
    return "gsm7" if can_encode_gsm7(text) else "ucs2"


def _char_septets(ch: str) -> list[int]:
    if ch in _BASIC_TO_SEPTET:
        return [_BASIC_TO_SEPTET[ch]]
    return [_ESC, _GSM_EXT[ch]]


def gsm7_positions(text: str) -> int:
    """Septet count: basic chars cost 1, extension chars cost 2."""
    return sum(2 if ch in _GSM_EXT else 1 for ch in text)


def ucs2_positions(text: str) -> int:
    """UTF-16 code-unit count: BMP chars cost 1, supplementary chars cost 2."""
    return len(text.encode("utf-16-be")) // 2


# --------------------------------------------------------------------------- #
# 7-bit packing
# --------------------------------------------------------------------------- #
def pack_gsm7(septets: list[int], fill_bits: int = 0) -> bytes:
    out = bytearray()
    acc = 0
    nbits = fill_bits  # leading zero fill bits (for UDH septet alignment)
    for s in septets:
        acc |= (s & 0x7F) << nbits
        nbits += 7
        while nbits >= 8:
            out.append(acc & 0xFF)
            acc >>= 8
            nbits -= 8
    if nbits > 0:
        out.append(acc & 0xFF)
    return bytes(out)


def unpack_gsm7(data: bytes, num_septets: int, fill_bits: int = 0) -> list[int]:
    septets: list[int] = []
    acc = 0
    nbits = 0
    total_fill = fill_bits
    for byte in data:
        acc |= byte << nbits
        nbits += 8
        while nbits >= 7:
            if total_fill > 0:
                # Discard the leading fill bits before the first septet.
                drop = min(total_fill, nbits)
                acc >>= drop
                nbits -= drop
                total_fill -= drop
                continue
            if len(septets) >= num_septets:
                return septets
            septets.append(acc & 0x7F)
            acc >>= 7
            nbits -= 7
    return septets


def _septets_to_text(septets: list[int]) -> str:
    out: list[str] = []
    i = 0
    while i < len(septets):
        s = septets[i]
        if s == _ESC:
            i += 1
            nxt = septets[i] if i < len(septets) else -1
            out.append(_SEPTET_TO_EXT.get(nxt, " "))  # unknown escape -> space
        else:
            out.append(_SEPTET_TO_BASIC.get(s, "?"))
        i += 1
    return "".join(out)


def decode_segment(data: bytes, data_coding: int, esm_class: int) -> str:
    """Decode one short_message back to text (used by the SMPP server ingress).

    Strips the UDH when the UDHI bit is set. For GSM7 the septet count is derived
    from the octet length; a message whose last octet has exactly 7 spare bits can
    yield a spurious trailing '@' — a known GSM7 ambiguity, acceptable for the POC.
    """
    fill = 0
    if esm_class & EsmClass.UDHI:
        if data:
            udhl = data[0]
            data = data[1 + udhl :]
        fill = 1
    if data_coding == DataCoding.UCS2:
        try:
            return data.decode("utf-16-be")
        except UnicodeDecodeError:
            return data.decode("utf-16-be", errors="replace")
    # GSM7 (SMSC default / IA5 treated the same here).
    num_septets = max(0, (len(data) * 8 - fill) // 7)
    return _septets_to_text(unpack_gsm7(data, num_septets, fill_bits=fill))


# --------------------------------------------------------------------------- #
# Segmentation
# --------------------------------------------------------------------------- #
def _greedy_split(costs: list[int], cap: int) -> list[tuple[int, int]]:
    """Return (start, end) char-index ranges, each with summed cost <= cap.

    Splits on character boundaries so extension pairs / surrogate pairs are never
    cut across segments.
    """
    ranges: list[tuple[int, int]] = []
    start = 0
    running = 0
    for i, cost in enumerate(costs):
        if running + cost > cap and i > start:
            ranges.append((start, i))
            start = i
            running = 0
        running += cost
    ranges.append((start, len(costs)))
    return ranges


def _udh(ref: int, total: int, seq: int) -> bytes:
    return bytes([0x05, 0x00, 0x03, ref & 0xFF, total & 0xFF, seq & 0xFF])


def _encode_gsm7(text: str, ref: int) -> EncodedMessage:
    if gsm7_positions(text) <= GSM7_SINGLE:
        septets = [s for ch in text for s in _char_septets(ch)]
        seg = Segment(pack_gsm7(septets), DataCoding.SMSC_DEFAULT, EsmClass.DEFAULT)
        return EncodedMessage("gsm7", [seg])

    costs = [2 if ch in _GSM_EXT else 1 for ch in text]
    ranges = _greedy_split(costs, GSM7_MULTI)
    total = len(ranges)
    segments: list[Segment] = []
    for seq, (start, end) in enumerate(ranges, start=1):
        septets = [s for ch in text[start:end] for s in _char_septets(ch)]
        data = _udh(ref, total, seq) + pack_gsm7(septets, fill_bits=1)
        segments.append(Segment(data, DataCoding.SMSC_DEFAULT, EsmClass.UDHI))
    return EncodedMessage("gsm7", segments)


def _encode_ucs2(text: str, ref: int) -> EncodedMessage:
    if ucs2_positions(text) <= UCS2_SINGLE:
        seg = Segment(text.encode("utf-16-be"), DataCoding.UCS2, EsmClass.DEFAULT)
        return EncodedMessage("ucs2", [seg])

    costs = [len(ch.encode("utf-16-be")) // 2 for ch in text]
    ranges = _greedy_split(costs, UCS2_MULTI)
    total = len(ranges)
    segments: list[Segment] = []
    for seq, (start, end) in enumerate(ranges, start=1):
        data = _udh(ref, total, seq) + text[start:end].encode("utf-16-be")
        segments.append(Segment(data, DataCoding.UCS2, EsmClass.UDHI))
    return EncodedMessage("ucs2", segments)


def encode_message(text: str, ref: int = 0) -> EncodedMessage:
    """Encode text into one or more SMPP short_message segments.

    ``ref`` is the concatenation reference (0-255), only used when the message
    spans multiple segments.
    """
    if can_encode_gsm7(text):
        return _encode_gsm7(text, ref)
    return _encode_ucs2(text, ref)

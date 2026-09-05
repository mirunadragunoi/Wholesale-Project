"""SMPP v3.4 protocol constants.

Own codec, no external library. Only the subset the POC needs is defined here,
but the error-code classification (temporary / permanent / fatal) is complete
enough to drive retry decisions, because that decision is the whole point of
having these codes typed rather than magic numbers.
"""

from __future__ import annotations

from enum import IntEnum

# The response bit: command_id of a response = request id | 0x80000000.
RESPONSE_BIT = 0x80000000


class CommandId(IntEnum):
    GENERIC_NACK = 0x80000000
    BIND_RECEIVER = 0x00000001
    BIND_RECEIVER_RESP = 0x80000001
    BIND_TRANSMITTER = 0x00000002
    BIND_TRANSMITTER_RESP = 0x80000002
    QUERY_SM = 0x00000003
    QUERY_SM_RESP = 0x80000003
    SUBMIT_SM = 0x00000004
    SUBMIT_SM_RESP = 0x80000004
    DELIVER_SM = 0x00000005
    DELIVER_SM_RESP = 0x80000005
    UNBIND = 0x00000006
    UNBIND_RESP = 0x80000006
    REPLACE_SM = 0x00000007
    REPLACE_SM_RESP = 0x80000007
    CANCEL_SM = 0x00000008
    CANCEL_SM_RESP = 0x80000008
    BIND_TRANSCEIVER = 0x00000009
    BIND_TRANSCEIVER_RESP = 0x80000009
    ENQUIRE_LINK = 0x00000015
    ENQUIRE_LINK_RESP = 0x80000015

    @property
    def is_response(self) -> bool:
        return bool(self.value & RESPONSE_BIT)

    def response_id(self) -> CommandId:
        """The response command_id for this request (id | response bit)."""
        return CommandId(self.value | RESPONSE_BIT)


class CommandStatus(IntEnum):
    ESME_ROK = 0x00000000  # No error
    ESME_RINVMSGLEN = 0x00000001
    ESME_RINVCMDLEN = 0x00000002
    ESME_RINVCMDID = 0x00000003
    ESME_RINVBNDSTS = 0x00000004
    ESME_RALYBND = 0x00000005
    ESME_RINVPRTFLG = 0x00000006
    ESME_RINVREGDLVFLG = 0x00000007
    ESME_RSYSERR = 0x00000008
    ESME_RINVSRCADR = 0x0000000A
    ESME_RINVDSTADR = 0x0000000B
    ESME_RINVMSGID = 0x0000000C
    ESME_RBINDFAIL = 0x0000000D
    ESME_RINVPASWD = 0x0000000E
    ESME_RINVSYSID = 0x0000000F
    ESME_RCANCELFAIL = 0x00000011
    ESME_RREPLACEFAIL = 0x00000013
    ESME_RMSGQFUL = 0x00000014
    ESME_RINVSERTYP = 0x00000015
    ESME_RINVESMCLASS = 0x00000043
    ESME_RSUBMITFAIL = 0x00000045
    ESME_RINVSRCTON = 0x00000048
    ESME_RINVSRCNPI = 0x00000049
    ESME_RINVDSTTON = 0x00000050
    ESME_RINVDSTNPI = 0x00000051
    ESME_RINVSYSTYP = 0x00000053
    ESME_RTHROTTLED = 0x00000058
    ESME_RX_T_APPN = 0x00000064  # transient application error
    ESME_RX_P_APPN = 0x00000065  # permanent application error
    ESME_RX_R_APPN = 0x00000066  # reject message
    ESME_RUNKNOWNERR = 0x000000FF


class ErrorCategory(IntEnum):
    """Drives retry/routing decisions. Never left implicit."""

    OK = 0
    TEMPORARY = 1  # retry (same or another route)
    PERMANENT = 2  # never retry this message; it will always fail
    FATAL = 3  # connection/credential-level; tear down the bind


# Explicit classification. Anything not listed is treated as PERMANENT: we never
# retry an error we do not understand, to avoid infinite retry loops.
_CATEGORY: dict[int, ErrorCategory] = {
    CommandStatus.ESME_ROK: ErrorCategory.OK,
    # Temporary — the message may succeed on retry.
    CommandStatus.ESME_RMSGQFUL: ErrorCategory.TEMPORARY,
    CommandStatus.ESME_RTHROTTLED: ErrorCategory.TEMPORARY,
    CommandStatus.ESME_RSUBMITFAIL: ErrorCategory.TEMPORARY,
    CommandStatus.ESME_RSYSERR: ErrorCategory.TEMPORARY,
    CommandStatus.ESME_RX_T_APPN: ErrorCategory.TEMPORARY,
    # Permanent — retrying will always fail (bad address, bad content).
    CommandStatus.ESME_RINVSRCADR: ErrorCategory.PERMANENT,
    CommandStatus.ESME_RINVDSTADR: ErrorCategory.PERMANENT,
    CommandStatus.ESME_RINVMSGLEN: ErrorCategory.PERMANENT,
    CommandStatus.ESME_RINVESMCLASS: ErrorCategory.PERMANENT,
    CommandStatus.ESME_RX_P_APPN: ErrorCategory.PERMANENT,
    CommandStatus.ESME_RX_R_APPN: ErrorCategory.PERMANENT,
    # Fatal — the bind itself is unusable; tear it down and reconnect/alert.
    CommandStatus.ESME_RBINDFAIL: ErrorCategory.FATAL,
    CommandStatus.ESME_RINVPASWD: ErrorCategory.FATAL,
    CommandStatus.ESME_RINVSYSID: ErrorCategory.FATAL,
    CommandStatus.ESME_RINVBNDSTS: ErrorCategory.FATAL,
}


def classify(status: int) -> ErrorCategory:
    return _CATEGORY.get(status, ErrorCategory.PERMANENT)


class Ton(IntEnum):
    UNKNOWN = 0x00
    INTERNATIONAL = 0x01
    NATIONAL = 0x02
    NETWORK_SPECIFIC = 0x03
    SUBSCRIBER_NUMBER = 0x04
    ALPHANUMERIC = 0x05
    ABBREVIATED = 0x06


class Npi(IntEnum):
    UNKNOWN = 0x00
    ISDN = 0x01  # E.163/E.164
    DATA = 0x03
    TELEX = 0x04
    LAND_MOBILE = 0x06
    NATIONAL = 0x08
    PRIVATE = 0x09
    ERMES = 0x0A
    INTERNET = 0x0E
    WAP = 0x12


class DataCoding(IntEnum):
    SMSC_DEFAULT = 0x00  # GSM 03.38 in practice
    IA5_ASCII = 0x01
    LATIN1 = 0x03
    UCS2 = 0x08


class EsmClass(IntEnum):
    DEFAULT = 0x00
    UDHI = 0x40  # user data header present (bit 6) — set for concatenation
    # deliver_sm message-type bits:
    MT_DELIVERY_RECEIPT = 0x04  # this deliver_sm is a DLR


class RegisteredDelivery(IntEnum):
    NONE = 0x00
    DLR_ON_SUCCESS_OR_FAILURE = 0x01


# Optional TLV parameter tags used by the POC.
class Tlv(IntEnum):
    MESSAGE_PAYLOAD = 0x0424
    RECEIPTED_MESSAGE_ID = 0x001E
    MESSAGE_STATE = 0x0427
    # Vendor-specific range (0x1400-0x3FFF): carry our ULID for end-to-end
    # correlation and duplicate detection at the provider.
    RELAY_MESSAGE_ID = 0x1400

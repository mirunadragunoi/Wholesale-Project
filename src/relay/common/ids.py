"""ULID generation.

ULID = 48-bit millisecond timestamp + 80-bit randomness, rendered as a 26-char
Crockford base32 string. Implemented in-tree rather than pulling a dependency:
the spec is small, well defined, and we want full control over monotonicity.

The timestamp prefix makes IDs roughly time-sortable, which is convenient for
tracing a message through the pipeline in logs.
"""

from __future__ import annotations

import os
import threading
import time

# Crockford base32: excludes I, L, O, U to avoid ambiguity.
_ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ENCODED_LEN = 26
_RANDOM_BITS = 80
_RANDOM_MAX = (1 << _RANDOM_BITS) - 1

_lock = threading.Lock()
_last_ms: int = -1
_last_random: int = 0


def _encode(value: int, length: int) -> str:
    chars = ["0"] * length
    for i in range(length - 1, -1, -1):
        chars[i] = _ALPHABET[value & 0x1F]
        value >>= 5
    return "".join(chars)


def new_ulid(now_ms: int | None = None) -> str:
    """Return a new ULID string.

    Monotonic within a single millisecond: if called repeatedly in the same
    millisecond, the random component is incremented rather than redrawn, so
    lexical order matches creation order.
    """
    global _last_ms, _last_random

    ms = now_ms if now_ms is not None else int(time.time() * 1000)
    with _lock:
        if ms == _last_ms:
            _last_random = (_last_random + 1) & _RANDOM_MAX
            if _last_random == 0:
                # Overflow within the millisecond; step time forward.
                ms += 1
                _last_ms = ms
                _last_random = int.from_bytes(os.urandom(10), "big")
        else:
            _last_ms = ms
            _last_random = int.from_bytes(os.urandom(10), "big")
        random_part = _last_random

    value = (ms << _RANDOM_BITS) | random_part
    return _encode(value, _ENCODED_LEN)

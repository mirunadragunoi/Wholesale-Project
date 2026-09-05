from __future__ import annotations

from relay.common.ids import _ALPHABET, new_ulid


def test_ulid_length_and_alphabet() -> None:
    ulid = new_ulid()
    assert len(ulid) == 26
    assert all(c in _ALPHABET for c in ulid)


def test_ulid_monotonic_within_millisecond() -> None:
    ms = 1_700_000_000_000
    ulids = [new_ulid(now_ms=ms) for _ in range(1000)]
    assert ulids == sorted(ulids), "IDs in the same millisecond must be lexically increasing"
    assert len(set(ulids)) == len(ulids), "IDs must be unique"


def test_ulid_time_ordering_across_millis() -> None:
    earlier = new_ulid(now_ms=1_700_000_000_000)
    later = new_ulid(now_ms=1_700_000_000_001)
    assert earlier < later

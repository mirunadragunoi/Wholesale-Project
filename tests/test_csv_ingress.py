from __future__ import annotations

from pathlib import Path

from relay.common.config import CsvIngressConfig, QueueConfig
from relay.ingress.csv_connector import CsvIngress, _row_to_message


def test_row_to_message_valid() -> None:
    m = _row_to_message({"to": "+40712345678", "text": "hi", "sender": "RELAY"})
    assert m is not None
    assert m.to == "+40712345678"
    assert m.text == "hi"
    assert m.sender == "RELAY"
    assert m.source == "csv"


def test_row_to_message_optional_sender() -> None:
    m = _row_to_message({"to": "+40712345678", "text": "hi", "sender": ""})
    assert m is not None
    assert m.sender is None


def test_row_to_message_invalid() -> None:
    assert _row_to_message({"to": "", "text": "hi"}) is None  # missing to
    assert _row_to_message({"to": "+40712345678"}) is None  # missing text column


async def test_csv_streams_and_skips_invalid(tmp_path: Path) -> None:
    path = tmp_path / "campaign.csv"
    path.write_text(
        "to,text,sender\n"
        "+40712345678,hello,RELAY\n"
        ",missing destination,X\n"  # invalid -> skipped
        "+40700000000,another,\n",
        encoding="utf-8",
    )
    config = CsvIngressConfig(path=str(path), batch_size=2, queue=QueueConfig(backend="memory"))
    stats = await CsvIngress(config).run()
    assert stats.total == 3
    assert stats.sent == 2
    assert stats.skipped == 1

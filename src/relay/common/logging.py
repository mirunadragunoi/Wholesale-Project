"""Structured JSON logging.

One JSON object per line. Every relevant line carries a ``message_id`` where a
message is in scope. No ``print`` anywhere in the codebase — this is the only
output channel.

Usage::

    configure_logging("INFO")
    log = get_logger("ingress.http")
    log.info("message_accepted", message_id=msg.id, source="http")
"""

from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any, TextIO

_RESERVED = frozenset(
    logging.makeLogRecord({}).__dict__.keys()
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": round(record.created, 6),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        # Merge structured fields attached via `extra=`.
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


class BoundLogger:
    """Thin wrapper turning ``log.info("event", key=value)`` into structured logs."""

    __slots__ = ("_bound", "_logger")

    def __init__(self, logger: logging.Logger, bound: dict[str, Any] | None = None) -> None:
        self._logger = logger
        self._bound = bound or {}

    def bind(self, **fields: Any) -> BoundLogger:
        merged = {**self._bound, **fields}
        return BoundLogger(self._logger, merged)

    def _log(self, level: int, event: str, fields: dict[str, Any]) -> None:
        if not self._logger.isEnabledFor(level):
            return
        self._logger.log(level, event, extra={**self._bound, **fields})

    def debug(self, event: str, **fields: Any) -> None:
        self._log(logging.DEBUG, event, fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log(logging.INFO, event, fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log(logging.WARNING, event, fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log(logging.ERROR, event, fields)

    def exception(self, event: str, **fields: Any) -> None:
        if not self._logger.isEnabledFor(logging.ERROR):
            return
        self._logger.error(event, exc_info=True, extra={**self._bound, **fields})


def configure_logging(level: str = "INFO", stream: TextIO | None = None) -> None:
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    logging.Formatter.converter = time.gmtime


def get_logger(name: str) -> BoundLogger:
    return BoundLogger(logging.getLogger(name))

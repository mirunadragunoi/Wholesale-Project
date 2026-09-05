"""Backend selection from configuration."""

from __future__ import annotations

from relay.common.config import QueueConfig
from relay.queues.base import QueueBackend
from relay.queues.memory import MemoryBackend
from relay.queues.sqs import SqsBackend


def create_backend(config: QueueConfig) -> QueueBackend:
    if config.backend == "sqs":
        return SqsBackend(config)
    if config.backend == "memory":
        return MemoryBackend()
    raise ValueError(f"unknown queue backend {config.backend!r}")

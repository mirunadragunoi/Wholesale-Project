"""Abstract queue interfaces.

The data path talks only to these interfaces. Two backends implement them: SQS
(``sqs.py``) for real use and ElasticMQ, and an in-memory backend (``memory.py``)
for tests and local runs without Docker.

Design notes:

* A ``Producer``/``Consumer`` is bound to a single logical queue at creation.
* ``publish`` accepts any number of messages and is responsible for batching
  them into backend-sized chunks — callers never batch by hand.
* ``receive`` returns already-deserialized ``Message`` objects wrapped with the
  handle needed to delete them and the enqueue time for lag measurement.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass

from relay.common.message import Message


@dataclass(frozen=True, slots=True)
class ReceivedMessage:
    message: Message
    handle: str  # opaque handle passed back to delete()
    sent_at: float | None  # enqueue time (epoch seconds), for lag; None if unknown


class Producer(ABC):
    @abstractmethod
    async def publish(self, messages: Sequence[Message]) -> None:
        """Publish messages, batching into backend-sized chunks internally."""

    @abstractmethod
    async def close(self) -> None: ...

    async def __aenter__(self) -> Producer:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class Consumer(ABC):
    @abstractmethod
    async def receive(self, max_messages: int) -> list[ReceivedMessage]:
        """Receive up to ``max_messages`` messages (long-polling where supported)."""

    @abstractmethod
    async def delete(self, handles: Sequence[str]) -> None:
        """Acknowledge (delete) previously received messages by handle."""

    @abstractmethod
    async def close(self) -> None: ...

    async def __aenter__(self) -> Consumer:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class QueueBackend(ABC):
    """Factory for producers/consumers over named logical queues."""

    async def start(self) -> None:
        """Open any shared resources (network clients). No-op by default."""
        return None

    @abstractmethod
    async def producer(self, queue: str) -> Producer: ...

    @abstractmethod
    async def consumer(self, queue: str) -> Consumer: ...

    @abstractmethod
    async def close(self) -> None: ...

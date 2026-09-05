"""SQS queue backend (works against real AWS SQS and against ElasticMQ).

Batching is mandatory: ``publish`` chunks into ``SendMessageBatch`` calls of up
to 10 (the SQS hard limit) and issues the chunks concurrently. ``receive`` uses
long polling. Everything runs on the event loop via aioboto3 — no blocking
boto3 calls in the hot path.

Message bodies must be UTF-8 text on the wire. JSON already is; msgpack (binary)
is base64-encoded and flagged with a ``b64`` message attribute so the consumer
can reverse it.
"""

from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import Iterator, Sequence
from typing import Any

import aioboto3

from relay.common.config import QueueConfig
from relay.common.logging import get_logger
from relay.common.message import Message, Serializer, get_serializer
from relay.common.metrics import queue_consume_lag_seconds, queue_publish_duration_seconds
from relay.queues.base import Consumer, Producer, QueueBackend, ReceivedMessage

_log = get_logger("queues.sqs")
_MAX_BATCH = 10  # SQS hard limit for SendMessageBatch / DeleteMessageBatch
_SEND_RETRIES = 3


def _chunks(items: Sequence[Message], size: int) -> Iterator[Sequence[Message]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _encode_body(raw: bytes) -> tuple[str, bool]:
    try:
        return raw.decode("utf-8"), False
    except UnicodeDecodeError:
        return base64.b64encode(raw).decode("ascii"), True


def _decode_body(body: str, is_b64: bool) -> bytes:
    return base64.b64decode(body) if is_b64 else body.encode("utf-8")


class SqsProducer(Producer):
    def __init__(self, client: Any, url: str, serializer: Serializer, label: str) -> None:
        self._client = client
        self._url = url
        self._serializer = serializer
        self._label = label

    async def publish(self, messages: Sequence[Message]) -> None:
        if not messages:
            return
        await asyncio.gather(
            *(self._send_batch(chunk) for chunk in _chunks(messages, _MAX_BATCH))
        )

    async def _send_batch(self, chunk: Sequence[Message]) -> None:
        entries: list[dict[str, Any]] = []
        for i, msg in enumerate(chunk):
            body, is_b64 = _encode_body(self._serializer.encode(msg))
            entry: dict[str, Any] = {"Id": str(i), "MessageBody": body}
            if is_b64:
                entry["MessageAttributes"] = {
                    "b64": {"DataType": "String", "StringValue": "1"}
                }
            entries.append(entry)

        pending = entries
        for attempt in range(_SEND_RETRIES):
            with queue_publish_duration_seconds.labels(self._label).time():
                resp = await self._client.send_message_batch(
                    QueueUrl=self._url, Entries=pending
                )
            failed = resp.get("Failed") or []
            if not failed:
                return
            failed_ids = {f["Id"] for f in failed}
            pending = [e for e in entries if e["Id"] in failed_ids]
            _log.warning(
                "sqs_batch_partial_failure",
                queue=self._label,
                failed=len(failed),
                attempt=attempt + 1,
            )
        raise RuntimeError(f"failed to publish {len(pending)} messages after {_SEND_RETRIES} tries")

    async def close(self) -> None:
        # The shared client is owned and closed by the backend.
        return None


class SqsConsumer(Consumer):
    def __init__(
        self,
        client: Any,
        url: str,
        serializer: Serializer,
        label: str,
        wait_time_seconds: int,
        visibility_timeout: int,
        max_per_call: int,
    ) -> None:
        self._client = client
        self._url = url
        self._serializer = serializer
        self._label = label
        self._wait = wait_time_seconds
        self._visibility = visibility_timeout
        self._max_per_call = max_per_call

    async def receive(self, max_messages: int) -> list[ReceivedMessage]:
        resp = await self._client.receive_message(
            QueueUrl=self._url,
            MaxNumberOfMessages=min(max_messages, self._max_per_call, _MAX_BATCH),
            WaitTimeSeconds=self._wait,
            VisibilityTimeout=self._visibility,
            AttributeNames=["SentTimestamp"],
            MessageAttributeNames=["b64"],
        )
        raw_messages = resp.get("Messages") or []
        out: list[ReceivedMessage] = []
        for m in raw_messages:
            attrs = m.get("MessageAttributes") or {}
            is_b64 = attrs.get("b64", {}).get("StringValue") == "1"
            message = self._serializer.decode(_decode_body(m["Body"], is_b64))
            sent_at: float | None = None
            sys_attrs = m.get("Attributes") or {}
            if "SentTimestamp" in sys_attrs:
                sent_at = int(sys_attrs["SentTimestamp"]) / 1000.0
                queue_consume_lag_seconds.labels(self._label).set(
                    max(0.0, time.time() - sent_at)
                )
            out.append(ReceivedMessage(message, m["ReceiptHandle"], sent_at))
        return out

    async def delete(self, handles: Sequence[str]) -> None:
        for i in range(0, len(handles), _MAX_BATCH):
            chunk = handles[i : i + _MAX_BATCH]
            entries = [{"Id": str(j), "ReceiptHandle": h} for j, h in enumerate(chunk)]
            await self._client.delete_message_batch(QueueUrl=self._url, Entries=entries)

    async def close(self) -> None:
        return None


class SqsBackend(QueueBackend):
    def __init__(self, config: QueueConfig) -> None:
        self._config = config
        self._serializer = get_serializer(config.serializer)
        self._session = aioboto3.Session()
        self._client_cm: Any = None
        self._client: Any = None
        self._url_cache: dict[str, str] = {}

    @staticmethod
    def from_config(config: QueueConfig) -> SqsBackend:
        return SqsBackend(config)

    async def start(self) -> None:
        sqs = self._config.sqs
        kwargs: dict[str, Any] = {"region_name": sqs.region}
        if sqs.endpoint_url:
            # ElasticMQ / local: dummy credentials, still required for signing.
            kwargs.update(
                endpoint_url=sqs.endpoint_url,
                aws_access_key_id="local",
                aws_secret_access_key="local",
            )
        self._client_cm = self._session.client("sqs", **kwargs)
        self._client = await self._client_cm.__aenter__()

    async def _resolve_url(self, queue: str) -> str:
        if queue in self._url_cache:
            return self._url_cache[queue]
        physical = self._config.queues.get(queue, queue)
        if physical.startswith("http://") or physical.startswith("https://"):
            url = physical
        else:
            resp = await self._client.get_queue_url(QueueName=physical)
            url = str(resp["QueueUrl"])
        self._url_cache[queue] = url
        return url

    async def ensure_queue(self, queue: str) -> str:
        """Create the queue if it does not exist; return its URL. Used by tooling."""
        physical = self._config.queues.get(queue, queue)
        resp = await self._client.create_queue(QueueName=physical)
        url = str(resp["QueueUrl"])
        self._url_cache[queue] = url
        return url

    async def purge(self, queue: str) -> None:
        url = await self._resolve_url(queue)
        await self._client.purge_queue(QueueUrl=url)

    async def producer(self, queue: str) -> Producer:
        url = await self._resolve_url(queue)
        return SqsProducer(self._client, url, self._serializer, queue)

    async def consumer(self, queue: str) -> Consumer:
        url = await self._resolve_url(queue)
        sqs = self._config.sqs
        return SqsConsumer(
            self._client,
            url,
            self._serializer,
            queue,
            sqs.wait_time_seconds,
            sqs.visibility_timeout,
            sqs.max_number_of_messages,
        )

    async def close(self) -> None:
        if self._client_cm is not None:
            await self._client_cm.__aexit__(None, None, None)
            self._client_cm = None
            self._client = None

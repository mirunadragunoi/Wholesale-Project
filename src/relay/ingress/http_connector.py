"""HTTP ingress connector.

``POST /v1/messages`` and ``POST /v1/messages/batch`` (up to 1000). Accepted
messages go onto a bounded in-memory queue; a pool of publisher workers drains
it to the ``ingress`` SQS queue in batches of 10. The bounded queue is the
backpressure mechanism: when it is full the endpoint returns 429 instead of
buffering without limit.

Auth is a single static token in the ``X-Auth-Token`` header — nothing more, by
design (this is a flow POC).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from pydantic import BaseModel, Field

from relay.common.config import HttpIngressConfig
from relay.common.ids import new_ulid
from relay.common.logging import get_logger
from relay.common.message import Message
from relay.common.metrics import ingress_received_total, start_metrics_server
from relay.queues.factory import create_backend

_log = get_logger("ingress.http")


class SubmitRequest(BaseModel):
    to: str = Field(min_length=1)
    text: str
    sender: str | None = None


class BatchRequest(BaseModel):
    messages: list[SubmitRequest]


class _State:
    def __init__(self, config: HttpIngressConfig) -> None:
        self.config = config
        self.backend = create_backend(config.queue)
        self.queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=config.internal_queue_maxsize)
        self.stop = asyncio.Event()
        self.workers: list[asyncio.Task[None]] = []

    async def publisher(self) -> None:
        producer = await self.backend.producer(self.config.ingress_queue)
        while not self.stop.is_set() or not self.queue.empty():
            try:
                first = await asyncio.wait_for(self.queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            batch = [first]
            while len(batch) < 10:
                try:
                    batch.append(self.queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await producer.publish(batch)
            for _ in batch:
                self.queue.task_done()


def _require_token(request: Request, x_auth_token: Annotated[str | None, Header()] = None) -> None:
    expected: str = request.app.state.relay.config.auth_token
    if x_auth_token != expected:
        raise HTTPException(status_code=401, detail="invalid or missing X-Auth-Token")


def _enqueue(state: _State, req: SubmitRequest, now: float) -> str:
    msg = Message(
        id=new_ulid(),
        to=req.to,
        text=req.text,
        sender=req.sender,
        source="http",
        received_at=now,
        attributes={},
    )
    state.queue.put_nowait(msg)  # caller guarantees capacity
    return msg.id


def create_app(config: HttpIngressConfig) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        state: _State = app.state.relay
        await state.backend.start()
        state.workers = [
            asyncio.create_task(state.publisher()) for _ in range(config.publisher_workers)
        ]
        start_metrics_server(config.metrics_port)
        _log.info(
            "ingress_started",
            http_port=config.http_port,
            metrics_port=config.metrics_port,
            workers=config.publisher_workers,
        )
        try:
            yield
        finally:
            _log.info("ingress_stopping", pending=state.queue.qsize())
            state.stop.set()
            await asyncio.gather(*state.workers, return_exceptions=True)
            await state.backend.close()

    app = FastAPI(title="relay ingress", lifespan=lifespan)
    app.state.relay = _State(config)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/v1/messages", status_code=202, dependencies=[Depends(_require_token)])
    async def submit(req: SubmitRequest, request: Request) -> dict[str, str]:
        state: _State = request.app.state.relay
        if state.queue.full():
            raise HTTPException(status_code=429, detail="ingress buffer full")
        msg_id = _enqueue(state, req, time.time())
        ingress_received_total.labels(source="http").inc()
        return {"id": msg_id}

    @app.post("/v1/messages/batch", status_code=202, dependencies=[Depends(_require_token)])
    async def submit_batch(req: BatchRequest, request: Request) -> dict[str, object]:
        state: _State = request.app.state.relay
        count = len(req.messages)
        if count == 0:
            raise HTTPException(status_code=400, detail="empty batch")
        if count > config.max_batch_size:
            raise HTTPException(
                status_code=400, detail=f"batch too large (max {config.max_batch_size})"
            )
        # All-or-nothing: reject the whole batch if it would overflow the buffer.
        if state.queue.qsize() + count > config.internal_queue_maxsize:
            raise HTTPException(status_code=429, detail="ingress buffer full")
        now = time.time()
        ids = [_enqueue(state, m, now) for m in req.messages]
        ingress_received_total.labels(source="http").inc(count)
        return {"accepted": count, "ids": ids}

    return app

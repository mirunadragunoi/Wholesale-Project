from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from relay.common.config import HttpIngressConfig, QueueConfig
from relay.ingress.http_connector import create_app


@pytest.fixture
def client() -> TestClient:
    config = HttpIngressConfig(
        auth_token="secret",
        publisher_workers=1,
        queue=QueueConfig(backend="memory"),
    )
    app = create_app(config)
    # Not used as a context manager on purpose: that skips the lifespan (metrics
    # server + publisher workers). We only exercise the request layer here;
    # end-to-end delivery is covered by the ElasticMQ run in the M1 report.
    return TestClient(app)


AUTH = {"X-Auth-Token": "secret"}


def test_health_needs_no_auth(client: TestClient) -> None:
    assert client.get("/health").status_code == 200


def test_submit_requires_token(client: TestClient) -> None:
    resp = client.post("/v1/messages", json={"to": "+40712345678", "text": "hi"})
    assert resp.status_code == 401


def test_submit_accepts_valid(client: TestClient) -> None:
    resp = client.post("/v1/messages", json={"to": "+40712345678", "text": "hi"}, headers=AUTH)
    assert resp.status_code == 202
    assert "id" in resp.json()


def test_batch_rejects_empty(client: TestClient) -> None:
    resp = client.post("/v1/messages/batch", json={"messages": []}, headers=AUTH)
    assert resp.status_code == 400


def test_batch_rejects_oversize(client: TestClient) -> None:
    msgs = [{"to": "+40712345678", "text": "x"}] * 1001
    resp = client.post("/v1/messages/batch", json={"messages": msgs}, headers=AUTH)
    assert resp.status_code == 400


def test_batch_accepts_valid(client: TestClient) -> None:
    msgs = [{"to": "+40712345678", "text": "x"}] * 10
    resp = client.post("/v1/messages/batch", json={"messages": msgs}, headers=AUTH)
    assert resp.status_code == 202
    assert resp.json()["accepted"] == 10

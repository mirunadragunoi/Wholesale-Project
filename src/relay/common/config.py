"""Configuration loading: YAML + environment variable interpolation.

No values are hardcoded in the application; everything comes from a YAML file,
with ``${VAR}`` / ``${VAR:default}`` placeholders resolved from the environment.
Credentials never live in YAML — AWS access is left to the standard credential
chain (env vars, shared config, instance role).
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::([^}]*))?\}")


def _interpolate(value: Any) -> Any:
    if isinstance(value, str):

        def repl(match: re.Match[str]) -> str:
            name, default = match.group(1), match.group(2)
            env = os.environ.get(name)
            if env is not None:
                return env
            if default is not None:
                return default
            raise KeyError(f"environment variable {name!r} is not set and has no default")

        return _ENV_PATTERN.sub(repl, value)
    if isinstance(value, dict):
        return {k: _interpolate(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v) for v in value]
    return value


def load_config(path: str | os.PathLike[str]) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"config root of {path} must be a mapping, got {type(raw).__name__}")
    result = _interpolate(raw)
    assert isinstance(result, dict)
    return result


@dataclass(frozen=True, slots=True)
class SqsConfig:
    region: str = "us-east-1"
    endpoint_url: str | None = None  # set for ElasticMQ; None for real AWS
    max_batch: int = 10  # SQS hard limit
    wait_time_seconds: int = 20  # long polling
    visibility_timeout: int = 30
    max_number_of_messages: int = 10  # per receive call (SQS hard limit)
    # HTTP connection pool for the SQS client. This is a hard concurrency ceiling:
    # long-polling consumers each hold a connection for up to wait_time_seconds, so
    # the pool must exceed the number of concurrent producers + consumers or they
    # starve each other. botocore's default of 10 is far too low for this workload.
    max_pool_connections: int = 50

    @staticmethod
    def from_dict(data: dict[str, Any]) -> SqsConfig:
        return SqsConfig(
            region=str(data.get("region", "us-east-1")),
            endpoint_url=data.get("endpoint_url"),
            max_batch=int(data.get("max_batch", 10)),
            wait_time_seconds=int(data.get("wait_time_seconds", 20)),
            visibility_timeout=int(data.get("visibility_timeout", 30)),
            max_number_of_messages=int(data.get("max_number_of_messages", 10)),
            max_pool_connections=int(data.get("max_pool_connections", 50)),
        )


@dataclass(frozen=True, slots=True)
class QueueConfig:
    backend: str = "sqs"  # "sqs" | "memory"
    serializer: str = "json"  # "json" | "msgpack"
    queues: dict[str, str] = field(default_factory=dict)  # logical name -> url/name
    sqs: SqsConfig = field(default_factory=SqsConfig)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> QueueConfig:
        return QueueConfig(
            backend=str(data.get("backend", "sqs")),
            serializer=str(data.get("serializer", "json")),
            queues=dict(data.get("queues", {})),
            sqs=SqsConfig.from_dict(data.get("sqs", {})),
        )


@dataclass(frozen=True, slots=True)
class HttpIngressConfig:
    http_host: str = "0.0.0.0"  # dev POC binds all interfaces by design
    http_port: int = 8080
    metrics_port: int = 9101
    log_level: str = "INFO"
    auth_token: str = "devtoken"
    ingress_queue: str = "ingress"
    internal_queue_maxsize: int = 20_000  # bounded → 429 on overflow (backpressure)
    publisher_workers: int = 16
    max_batch_size: int = 1000  # /v1/messages/batch limit
    queue: QueueConfig = field(default_factory=QueueConfig)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> HttpIngressConfig:
        svc = data.get("service", {})
        return HttpIngressConfig(
            http_host=str(svc.get("http_host", "0.0.0.0")),
            http_port=int(svc.get("http_port", 8080)),
            metrics_port=int(svc.get("metrics_port", 9101)),
            log_level=str(svc.get("log_level", "INFO")),
            auth_token=str(svc.get("auth_token", "devtoken")),
            ingress_queue=str(svc.get("ingress_queue", "ingress")),
            internal_queue_maxsize=int(svc.get("internal_queue_maxsize", 20_000)),
            publisher_workers=int(svc.get("publisher_workers", 16)),
            max_batch_size=int(svc.get("max_batch_size", 1000)),
            queue=QueueConfig.from_dict(data.get("queue", {})),
        )


@dataclass(frozen=True, slots=True)
class EngineConfig:
    metrics_port: int = 9102
    log_level: str = "INFO"
    in_queue: str = "ingress"
    out_queue: str = "egress"
    workers: int = 16
    queue: QueueConfig = field(default_factory=QueueConfig)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> EngineConfig:
        svc = data.get("service", {})
        return EngineConfig(
            metrics_port=int(svc.get("metrics_port", 9102)),
            log_level=str(svc.get("log_level", "INFO")),
            in_queue=str(svc.get("in_queue", "ingress")),
            out_queue=str(svc.get("out_queue", "egress")),
            workers=int(svc.get("workers", 16)),
            queue=QueueConfig.from_dict(data.get("queue", {})),
        )


@dataclass(frozen=True, slots=True)
class SmppEgressConfig:
    metrics_port: int = 9103
    log_level: str = "INFO"
    egress_queue: str = "egress"
    workers: int = 8
    connector_name: str = "smpp"
    # provider connection
    host: str = "127.0.0.1"
    port: int = 2775
    system_id: str = "esme"
    password: str = "password"
    system_type: str = ""
    bind_count: int = 2
    window_size: int = 10
    tps_limit: float = 0.0  # aggregate across all binds; 0 = unlimited
    submit_timeout_s: float = 30.0
    # addressing
    source_addr: str = "RELAY"
    source_ton: int = 5  # alphanumeric
    source_npi: int = 0
    dest_ton: int = 1  # international
    dest_npi: int = 1  # ISDN/E.164
    registered_delivery: int = 0  # 1 to request DLRs
    queue: QueueConfig = field(default_factory=QueueConfig)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> SmppEgressConfig:
        svc = data.get("service", {})
        return SmppEgressConfig(
            metrics_port=int(svc.get("metrics_port", 9103)),
            log_level=str(svc.get("log_level", "INFO")),
            egress_queue=str(svc.get("egress_queue", "egress")),
            workers=int(svc.get("workers", 8)),
            connector_name=str(svc.get("connector_name", "smpp")),
            host=str(svc.get("host", "127.0.0.1")),
            port=int(svc.get("port", 2775)),
            system_id=str(svc.get("system_id", "esme")),
            password=str(svc.get("password", "password")),
            system_type=str(svc.get("system_type", "")),
            bind_count=int(svc.get("bind_count", 2)),
            window_size=int(svc.get("window_size", 10)),
            tps_limit=float(svc.get("tps_limit", 0.0)),
            submit_timeout_s=float(svc.get("submit_timeout_s", 30.0)),
            source_addr=str(svc.get("source_addr", "RELAY")),
            source_ton=int(svc.get("source_ton", 5)),
            source_npi=int(svc.get("source_npi", 0)),
            dest_ton=int(svc.get("dest_ton", 1)),
            dest_npi=int(svc.get("dest_npi", 1)),
            registered_delivery=int(svc.get("registered_delivery", 0)),
            queue=QueueConfig.from_dict(data.get("queue", {})),
        )


@dataclass(frozen=True, slots=True)
class HttpEgressConfig:
    metrics_port: int = 9103
    log_level: str = "INFO"
    egress_queue: str = "egress"
    workers: int = 16
    endpoint_url: str = "http://localhost:8090/sink"
    connector_name: str = "http"
    http_pool_limit: int = 100
    request_timeout_s: float = 30.0
    tps_limit: float = 0.0  # 0 = unlimited
    queue: QueueConfig = field(default_factory=QueueConfig)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> HttpEgressConfig:
        svc = data.get("service", {})
        return HttpEgressConfig(
            metrics_port=int(svc.get("metrics_port", 9103)),
            log_level=str(svc.get("log_level", "INFO")),
            egress_queue=str(svc.get("egress_queue", "egress")),
            workers=int(svc.get("workers", 16)),
            endpoint_url=str(svc.get("endpoint_url", "http://localhost:8090/sink")),
            connector_name=str(svc.get("connector_name", "http")),
            http_pool_limit=int(svc.get("http_pool_limit", 100)),
            request_timeout_s=float(svc.get("request_timeout_s", 30.0)),
            tps_limit=float(svc.get("tps_limit", 0.0)),
            queue=QueueConfig.from_dict(data.get("queue", {})),
        )

"""Prometheus metrics.

All metric objects are defined here once and imported where needed, so the set
of series is discoverable in a single place. Histograms use buckets tuned for a
low-latency message pipeline (sub-millisecond to a few seconds).

``start_metrics_server(port)`` exposes ``/metrics`` over HTTP for scraping.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Latency buckets in seconds: fine-grained below ~100ms, coarser out to 10s.
_LATENCY_BUCKETS = (
    0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
    0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)

ingress_received_total = Counter(
    "relay_ingress_received_total",
    "Messages accepted by an ingress connector",
    ["source"],
)

queue_publish_duration_seconds = Histogram(
    "relay_queue_publish_duration_seconds",
    "Time to publish a batch to a queue",
    ["queue"],
    buckets=_LATENCY_BUCKETS,
)

queue_consume_lag_seconds = Gauge(
    "relay_queue_consume_lag_seconds",
    "Age of the most recently consumed message when it was received",
    ["queue"],
)

engine_processed_total = Counter(
    "relay_engine_processed_total",
    "Messages processed by the engine pipeline",
)

egress_submitted_total = Counter(
    "relay_egress_submitted_total",
    "Messages submitted by an egress connector",
    ["connector", "result"],
)

egress_submit_duration_seconds = Histogram(
    "relay_egress_submit_duration_seconds",
    "Time to submit one message via an egress connector",
    ["connector"],
    buckets=_LATENCY_BUCKETS,
)

end_to_end_duration_seconds = Histogram(
    "relay_end_to_end_duration_seconds",
    "Ingress-to-egress latency of a message",
    buckets=_LATENCY_BUCKETS,
)

smpp_bind_state = Gauge(
    "relay_smpp_bind_state",
    "SMPP bind state (0=closed 1=open 2=bound)",
    ["connector", "bind_id"],
)

smpp_window_usage = Gauge(
    "relay_smpp_window_usage",
    "In-flight unacknowledged PDUs on an SMPP bind",
    ["connector", "bind_id"],
)


def start_metrics_server(port: int) -> None:
    start_http_server(port)

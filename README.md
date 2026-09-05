# relay

A high-volume SMS wholesale (A2P) routing platform — **milestone 1: a flow
proof-of-concept**.

The POC does not implement business logic. It builds the *pipe* — three
independent processes communicating only through queues — and instruments it to
answer one question: **how much message volume can this architecture carry, and
where are the ceilings?**

```
CLIENTS                    PLATFORM                         PROVIDERS
        ┌──────────┐              ┌────────┐              ┌──────────┐
HTTP ──►│          │              │        │              │          │──► HTTP
SMPP ──►│ INGRESS  │──► SQS ─────►│ ENGINE │──► SQS ─────►│  EGRESS  │──► SMPP
CSV  ──►│          │  (ingress)   │(pass-  │  (egress)    │          │
        └──────────┘              │through)│              └──────────┘
                                  └────────┘
```

## Status

Under construction, milestone by milestone. See `docs/BENCHMARKS.md` for
measured results and `docs/ARCHITECTURE.md` for design decisions.

| Milestone | Scope | State |
|-----------|-------|-------|
| M0 | Foundation + isolated SQS baseline | done (ElasticMQ; real-AWS run pending credentials) |
| M1 | HTTP → SQS → engine → SQS → HTTP flow | done (ElasticMQ) |
| M2 | SMPP codec + client (egress) | pending |
| M3 | SMPP server (ingress) + CSV | pending |
| M4 | Full benchmarks + report | pending |

## Requirements

- Python 3.12+
- Docker (for ElasticMQ, the local SQS-compatible server)
- No `uv` on this machine → dependencies are managed with `pip` + `pyproject.toml`.

## Setup

```bash
python -m pip install -e ".[dev,http]"
docker compose up -d          # start ElasticMQ on :9324
```

## Running the M1 flow (HTTP → SQS → engine → SQS → HTTP)

```bash
docker compose up -d                                   # ElasticMQ
python tools/http_sink.py --port 8090                  # simulated provider
python -m relay.egress.main  --config config/egress.yaml
python -m relay.engine.main  --config config/engine.yaml
python -m relay.ingress.main --config config/ingress.yaml   # HTTP API on :8080

# drive traffic and measure end-to-end latency:
python tools/loadgen.py --count 20000 --rate 400       # paced (transit latency)
python tools/loadgen.py --count 100000                 # saturation (throughput ceiling)
```

Metrics are exposed per process: ingress `:9101`, engine `:9102`, egress `:9103`
(`/metrics`).

## Development

```bash
ruff check .            # lint
ruff format .           # format
mypy                    # type-check (strict)
pytest                  # tests
```

## Layout

```
src/relay/common/   message envelope, config, logging, metrics, ULIDs
src/relay/queues/   abstract queue interface + SQS and in-memory backends
src/relay/smpp/     own SMPP codec (M2+)
src/relay/ingress/  HTTP / SMPP / CSV inbound connectors
src/relay/engine/   pass-through pipeline
src/relay/egress/   HTTP / SMPP outbound connectors
tools/              load generator, protocol sinks, isolated SQS benchmark
docs/               architecture, SMPP notes, benchmark report
```

Code, comments and commit messages are in English; `docs/` may be in Romanian.

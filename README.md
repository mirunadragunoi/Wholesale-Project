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
| M1.5 | Measurement round: Linux/uvloop, horizontal scaling, 429 fix, JSON/msgpack | done (ElasticMQ; real-AWS still pending) |
| M2 | Own SMPP codec + client (egress) | done (ElasticMQ) |
| M3 | SMPP server (ingress) + streaming CSV | done (ElasticMQ) |
| M4 | Full benchmarks + report | done (ElasticMQ; real-AWS still the one open item) |

## Requirements

- Python 3.12+
- Docker (for ElasticMQ, the local SQS-compatible server)
- No `uv` on this machine → dependencies are managed with `pip` + `pyproject.toml`.

> **Benchmarks run on Linux, not Windows.** Windows is a development environment
> only; its asyncio ProactorEventLoop is ~2–3× slower at network I/O, so Windows
> numbers are not comparable to Linux/production. See `docs/BENCHMARKS.md`.

### Benchmark setup (Linux / WSL2)

```bash
python3 -m venv --without-pip ~/relay-venv    # if ensurepip is missing
curl -sSL https://bootstrap.pypa.io/get-pip.py | ~/relay-venv/bin/python
~/relay-venv/bin/pip install -e ".[dev,http,perf]"   # perf pulls uvloop
```

`uvloop` is wired into all processes automatically when installed (engine/egress
set the policy; uvicorn auto-selects it). Note: measurements show uvloop does not
help this SQS-bound workload — see `docs/BENCHMARKS.md`.

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
docs/               ARCHITECTURE, SMPP, BENCHMARKS (report + exec summary),
                    RUNBOOK (reproduce from scratch), NEXT-STEPS (phase 2)
```

Code, comments and commit messages are in English; `docs/` may be in Romanian.

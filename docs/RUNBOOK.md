# RUNBOOK — cum reproduci totul de la zero

O pagină. Pornire, rulare benchmark-uri, comutare pe SQS real. **Benchmark-urile
se rulează pe Linux** (pe Windows event loop-ul e ~2–3× mai lent — necomparabil).

## 1. Cerințe

- **Linux** (nativ sau WSL2 Ubuntu). Windows e doar pentru dezvoltare.
- **Python 3.12+**.
- **Docker** (pentru ElasticMQ, brokerul de cozi local, compatibil SQS).

## 2. Setup

```bash
# broker de cozi local (ElasticMQ pe :9324) — din rădăcina repo-ului
docker compose up -d

# venv + dependințe. Dacă `python3 -m venv` se plânge de ensurepip:
python3 -m venv --without-pip .venv
curl -sSL https://bootstrap.pypa.io/get-pip.py | .venv/bin/python
.venv/bin/pip install -e ".[dev,http,perf]"     # perf = uvloop (Linux)

# verificare
.venv/bin/python -m ruff check . && .venv/bin/python -m mypy && .venv/bin/python -m pytest
```

## 3. Pornirea celor trei procese (fluxul complet)

Fiecare proces își alege conectorul din fișierul de config (`service.connector`).
Pentru un flux HTTP→SMPP, de exemplu:

```bash
# furnizor simulat (unul dintre):
python tools/http_sink.py --port 8090                 # provider HTTP
python tools/smpp_sink.py --port 2775 --dlr           # provider SMPP (cu DLR-uri)

# cele trei procese ale platformei:
python -m relay.egress.main  --config config/egress.yaml        # sau egress_smpp.yaml
python -m relay.engine.main  --config config/engine.yaml
python -m relay.ingress.main --config config/ingress.yaml       # HTTP :8080
#   ingress SMPP:  --config config/ingress_smpp.yaml   (server SMPP :2775)
#   ingress CSV:   --config config/ingress_csv.yaml    (CSV_PATH=... , rulează și iese)
```

Metrici Prometheus: ingress `:9101` (HTTP) / `:9104` (SMPP), engine `:9102`,
egress `:9103`, CSV `:9105` — toate pe `/metrics`.

## 4. Rularea benchmark-urilor (comenzi exacte)

```bash
# baseline izolat pe cozi (producer/consumer/roundtrip); --uvloop pentru event loop
python tools/bench_sqs.py --endpoint-url http://localhost:9324 --count 50000 --concurrency 32

# scalare orizontală 1/2/4 instanțe (pornește singur tot fluxul)
python tools/bench_scaling.py --instances 1 2 4 --count 60000 --serializer json

# generator de trafic HTTP: paced (latență reală) sau saturație (debit max)
python tools/loadgen.py --count 20000 --rate 400       # paced sub plafon
python tools/loadgen.py --count 100000                 # saturație

# CSV de test (NU se comite) + injectare
python tools/gen_csv.py --count 5000000 --bad-every 100000 --out /tmp/campaign.csv
CSV_PATH=/tmp/campaign.csv python -m relay.ingress.main --config config/ingress_csv.yaml
```

Măsurarea end-to-end: `http_sink` calculează latența din `received_at`; citește
`GET http://localhost:8090/stats`.

## 5. Fișiere de configurare — ce contează

Toate în `config/*.yaml`, cu interpolare `${VAR:default}` din mediu. Ce se schimbă des:

| Cheie | Unde | Efect |
|---|---|---|
| `queue.serializer` | toate | `json` (implicit) sau `msgpack` |
| `service.workers` / `publisher_workers` | toate | paralelism de consum/publicare |
| `service.internal_queue_maxsize` | ingress | pragul de backpressure (429 / RMSGQFUL) |
| `service.bind_count`, `tps_limit`, `window_size` | egress SMPP | pool de binduri, shaper TPS agregat |
| `service.credentials`, `max_binds_per_system`, `tps_per_system` | ingress SMPP | auth + limite per client |
| `service.rate` | CSV | ritm de injectare (msg/s) |
| `queue.sqs.max_pool_connections` | toate | **trebuie > concurență** (altfel long-poll-urile înfometează scrierile) |

## 6. Comutarea pe AWS SQS real — schimbare de CONFIG, nu de cod

1. Credențiale AWS în mediu (lanțul standard): `~/.aws/credentials` + `~/.aws/config`,
   sau `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION`. **Nu în cod, nu în YAML.**
2. Creează cozile Standard `ingress` și `egress` în regiunea aleasă (o singură dată).
3. Scoate `endpoint_url` (sau `SQS_ENDPOINT=`) ca să nu mai indice ElasticMQ:
   ```bash
   export SQS_ENDPOINT=            # gol -> folosește AWS real
   export AWS_REGION=eu-central-1
   ```
   `bench_sqs.py` / `bench_scaling.py`: pur și simplu **omite** `--endpoint-url`.
4. Rulează exact aceleași comenzi. Cod zero modificat — doar mediul.

Cost estimat pentru o rundă completă: **sub 5 USD**, ~30 de minute (vezi Limitări
în BENCHMARKS.md).

# ARCHITECTURE

Decizii de design și alternativele reale luate în calcul. Când o alegere are
alternative legitime, e notată aici cu argumentul — nu ascunsă în cod.

## Principiu de bază

Trei procese (ingress, engine, egress) care comunică **exclusiv prin cozi**.
Niciun apel direct între ele. Fiecare pornește, se oprește și se scalează
independent. Cozile sunt punctul de decuplare și, deci, punctul unde măsurăm.

```
ingress ──► coada „ingress" ──► engine ──► coada „egress" ──► egress
```

Motivul e chiar întrebarea POC-ului: dacă totul trece prin cozi, atunci
throughput-ul și latența cozii sunt plafonul teoretic al platformei, iar orice
adăugăm peste se măsoară ca „cost peste coadă".

## Anvelopa de mesaj (`common/message.py`)

`Message` e `frozen=True, slots=True`: imutabil (nu se modifică accidental pe
calea de date) și compact în memorie. `received_at` se ștampilează o singură
dată, la ingress, și se propagă neschimbat — e singura sursă de adevăr pentru
latența end-to-end.

### Serializare: JSON vs msgpack

Ambele sunt implementate (`JsonSerializer`, `MsgpackSerializer`) în spatele unui
`Protocol`. Măsurătoarea (vezi BENCHMARKS.md) arată că serializarea e cu ~3
ordine de mărime sub costul unui apel SQS, deci **nu** e pe calea critică la
volumele astea. Implicit rămâne **JSON**: lizibil, ușor de depanat, corp text
valid pe sârmă. msgpack e păstrat pentru cazul în care transportul se schimbă.

## Stratul de cozi (`queues/`)

### Abstracție + două implementări

`base.py` definește `Producer` / `Consumer` / `QueueBackend`. Calea de date
vede doar aceste interfețe. Două implementări:

- **`sqs.py`** — real (AWS SQS și ElasticMQ, prin `endpoint_url`).
- **`memory.py`** — `asyncio.Queue` mărginit, pentru teste și rulări locale fără
  Docker/AWS.

Alternativă respinsă: să scriem cod direct pe boto3 în fiecare conector. Ar fi
legat conectorii de SQS și ar fi făcut testele imposibile fără AWS. Abstracția
costă puțin și plătește imediat în testabilitate.

### Batching obligatoriu

`publish()` acceptă N mesaje și le împarte intern în loturi de 10 (limita dură
SQS), trimise concurent. Callerii nu fac batching manual. Un mesaj per apel ar
face POC-ul irelevant (vezi asimetria producer/consumer din BENCHMARKS.md:
costul e numărul de apeluri, nu volumul).

### aioboto3, nu boto3

boto3 e sincron; l-am fi blocat pe event loop. Folosim `aioboto3`
(peste aiobotocore) — tot I/O-ul SQS e pe event loop, fără executor.

### Pool de conexiuni — plafon dur

`max_pool_connections` (implicit botocore: 10) e un **plafon de concurență**, nu
un detaliu. Consumatorii long-poll țin conexiuni ocupate; dacă pool-ul e mai mic
decât numărul de producători + consumatori, scrierile sunt înfometate până la
blocaj. E configurabil în `SqsConfig` și trebuie dimensionat peste concurență.
(Descoperit empiric — vezi BENCHMARKS.md, „Descoperiri neașteptate".)

### Transport binar pe SQS

Corpul unui mesaj SQS trebuie să fie text UTF-8 valid. JSON e deja. Payload-ul
binar (msgpack) e codat base64 și marcat cu atributul `b64`; consumatorul îl
inversează. Decizia se ia per-mesaj în producer, ca să nu penalizăm JSON cu
base64 inutil.

### Lag de consum

`ReceivedMessage.sent_at` vine din `SentTimestamp` (atribut de sistem SQS).
Gauge-ul `relay_queue_consume_lag_seconds` măsoară vechimea mesajului la
recepție. În backend-ul in-memory ștampilăm noi ora la `put`.

## Backpressure (`memory.py` și regula generală)

Cozile interne sunt **mărginite**. Când se umplu, `publish` blochează (în
in-memory) sau ingress-ul respinge cu 429 (în calea reală, de la M1). Nu
acumulăm în memorie până la OOM. Backend-ul in-memory reproduce intenționat
această semantică pentru ca testele de backpressure să fie reale.

## Identificatori (`common/ids.py`)

ULID implementat în casă, nu ca dependință: spec mic și bine definit, și vrem
control pe monotonie. Prefixul de timp (48 biți ms) face ID-urile
aproape-sortabile temporal — util pentru urmărirea unui mesaj în loguri.
Monotonie garantată în cadrul aceleiași milisecunde (incrementăm partea aleatoare
în loc s-o redesenăm).

## Logare și metrici

- **Logare** (`common/logging.py`): JSON structurat, o linie per eveniment, cu
  `message_id` unde există un mesaj în context. Fără `print` nicăieri. Implementare
  proprie subțire peste `logging` stdlib — control total pe câmpuri, fără probleme
  de stub-uri mypy.
- **Metrici** (`common/metrics.py`): Prometheus, toate seriile definite într-un
  singur loc. Buckets de histogramă calibrate sub-milisecundă → câteva secunde.

## Configurare (`common/config.py`)

YAML + interpolare `${VAR}` / `${VAR:default}` din mediu. Zero valori hardcodate.
Credențialele **nu** stau în YAML — accesul AWS folosește lanțul standard de
credențiale (env, config partajat, rol de instanță).

## M1 — conectori HTTP și structura proceselor

- **Ingress HTTP** (`ingress/http_connector.py`): FastAPI. Cererile acceptate
  intră într-o `asyncio.Queue` **mărginită**; un pool de workeri o drenează în
  loturi de 10 către coada SQS `ingress`. Buffer plin → **429** (backpressure),
  nu bufferare nelimitată. Auth = un singur token static în header `X-Auth-Token`.
- **Engine** (`engine/pipeline.py` + `main.py`): consumă `ingress`, rulează
  pipeline-ul (o listă de etape async; o etapă no-op deocamdată), publică pe
  `egress`. Adăugarea unei etape reale = o linie în listă.
- **Egress HTTP** (`egress/http_connector.py`): consumă `egress`, face POST la un
  endpoint configurabil pe un pool aiohttp reutilizat, aplică token bucket-ul
  pentru TPS, și observă latența end-to-end (`now - received_at`).
- **Measurement**: `received_at` ștampilat la accept-ul HTTP se propagă până la
  sink, care calculează latența reală end-to-end. Pentru latență corectă,
  load-gen-ul poate **pace-ui** submit-ul (`--rate`) sub plafon; altfel se
  formează coadă și latența devine timp de ședere, nu de tranzit (vezi
  BENCHMARKS.md, M1).

### Oprire curată (SIGTERM) — limitare pe Windows

`common/worker.py::install_shutdown` folosește `loop.add_signal_handler` pe POSIX
și cade pe `signal.signal` pe Windows (unde `add_signal_handler` nu e
implementat). Pe Windows, SIGTERM „real" (kill din alt proces) nu declanșează
handler-ul Python fiabil; doar Ctrl+C (SIGINT) în consolă îl declanșează. Pentru
benchmark-uri procesele sunt oprite forțat. Drenarea curată la oprire e
implementată (ingress golește buffer-ul, workerii termină lotul în lucru); pe
producție reală (Linux/containere) SIGTERM funcționează cum trebuie.

## Decizii confirmate sau infirmate de măsurători (M1.5)

Vezi BENCHMARKS.md pentru cifre. Pe scurt:

- **Confirmat — batching-ul obligatoriu contează.** Asimetria producer/consumer
  (1 apel/10 la scriere vs 2 apeluri/10 la consum) se vede în orice mediu și
  domină costul. Decizia de a face batching peste tot e validată.
- **Confirmat — pool-ul de conexiuni e plafon dur.** Rămâne necesar; fără el,
  înfometare. Nedeschis de M1.5.
- **Infirmat — uvloop NU ajută acest workload.** Pe Linux, uvloop e marginal mai
  lent decât asyncio implicit (BENCHMARKS 2.1, 3.2). Gâtuirea e CPU-ul Python din
  botocore, nu event loop-ul. L-am păstrat cablat (opțional, inofensiv), dar nu e
  o pârghie. Presupunerea că ar ridica plafonul era greșită.
- **Nuanțat — „gâtuirea e broker-ul, nu Python".** Adevărat **pentru ElasticMQ**
  (scalarea plafonează, aritmetica operațiilor se potrivește — BENCHMARKS 3.3).
  Dar valabil doar pentru un broker JVM unic; generalizarea la SQS real rămâne
  **ipoteză** (3.4). Concluzia M1 era corectă din motive nedovedite atunci.
- **Confirmat — backpressure-ul funcționează.** 429-urile masive din M1 erau un
  artefact de retry în loadgen, nu în platformă (3.5). Buffer-ul de 20.000 e
  rezonabil, nemodificat.

**Mediu de benchmark: Linux.** Windows rămâne doar mediu de dezvoltare;
măsurătorile pe Windows nu sunt comparabile (ProactorEventLoop taie ~2/3 din
debit). Toate cifrele de referință de aici încolo se iau pe Linux.

## Note de mediu (specifice acestei mașini)

- `uv` nu e disponibil → gestiune cu `pip` + `pyproject.toml` (permis de spec).
- Interpretorul inițial din `.venv` era un CPython msys2/mingw
  (`mingw_x86_64`), pentru care PyPI nu are wheel-uri binare (`ruff`, `msgpack`
  încercau build din surse și eșuau). Am recreat `.venv` din CPython python.org
  `win-amd64` 3.12.4, unde toate wheel-urile se instalează curat.

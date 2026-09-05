# BENCHMARKS

Rezultate măsurate. Fiecare cifră ulterioară din proiect se raportează la
baseline-ul SQS din acest document.

> **Important:** cifrele sunt un instrument de decizie, nu marketing. Unde o
> măsurătoare contrazice o presupunere, e notat explicit.

## Mediu de test

| | |
|---|---|
| CPU | 12th Gen Intel Core i7-1260P (16 loguri) |
| OS | Windows 11 Pro (10.0.26200) |
| Python | CPython 3.12.4, win-amd64 (python.org) |
| SQS local | ElasticMQ 1.6.7 native, în Docker Desktop, `localhost:9324` |
| SQS real | **încă nemăsurat** — vezi „Ce lipsește" |

Toate rulările M0 sunt pe **un singur proces** producer/consumer, pe aceeași
mașină cu ElasticMQ. Sunt deci un **plafon superior**: nu există latență de
rețea către AWS. Numerele pe SQS real vor fi mai mici la throughput și mai mari
la latență; de aceea ambele trebuie măsurate (secțiunea următoare).

## Cum se reproduce

```bash
docker compose up -d          # ElasticMQ pe :9324
python tools/bench_sqs.py --endpoint-url http://localhost:9324 \
    --serializer json --concurrency 32 --count 50000 --rt-count 10000
```

Fără `--endpoint-url` (și cu credențiale AWS în mediu) aceeași comandă
măsoară SQS real:

```bash
python tools/bench_sqs.py --region eu-central-1 \
    --serializer json --concurrency 32 --count 50000 --rt-count 10000
```

Benchmark-ul măsoară trei lucruri, izolat:
- **producer** — mesaje/s scrise, batch de 10 (`SendMessageBatch`);
- **consumer** — mesaje/s citite și șterse, cu long polling (`ReceiveMessage` +
  `DeleteMessageBatch`);
- **dus-întors** — latența publish→receive, p50/p95/p99, pe mesaje cu
  `received_at` ștampilat la trimitere.

Mesaj de test: `to=+40712345678`, `text` de 20 caractere (SMS scurt tipic).

## Baseline SQS — ElasticMQ local (M0)

50.000 mesaje pentru throughput, 10.000 pentru latență. `consumed` confirmă că
nu se pierde niciun mesaj.

| Serializare | Concurență | Producer (msg/s) | Consumer (msg/s) | RT p50 | RT p95 | RT p99 | Pierdute |
|---|---|---|---|---|---|---|---|
| json | 16 | 2 997 | 1 339 | 35.2 ms | 69.3 ms | 86.9 ms | 0 |
| json | 32 | 3 593 | 1 550 | 40.3 ms | 75.5 ms | 93.6 ms | 0 |
| json | 64 | 3 711 | 1 397 | 41.5 ms | 99.3 ms | 138.4 ms | 0 |
| msgpack | 32 | 2 676 | 1 490 | 42.6 ms | 82.9 ms | 103.2 ms | 0 |

### Ce se vede

1. **Producer plafonează în jur de ~3 000–3 700 msg/s** și aproape nu mai crește
   de la concurență 16 la 64. ElasticMQ e un singur proces JVM in-memory; peste
   ~c16–c32 nu mai câștigăm throughput, câștigăm doar coadă și latență.
2. **Consumer e ~2× mai lent decât producer** (~1 400–1 550 msg/s). Cauza e
   structurală, nu un bug: un batch de scriere = **1** apel API (`SendMessageBatch`
   de 10), dar consumul acelorași 10 mesaje = **2** apeluri (`ReceiveMessage` +
   `DeleteMessageBatch`). Costul dominant e numărul de apeluri API, nu volumul de
   date. Consecință de arhitectură: la scalare, consumatorii au nevoie de mai
   multe procese decât producătorii pentru același debit.
3. **Concurența mare strică latența fără să aducă throughput.** La c64 p99 sare
   la 138 ms (față de 87 ms la c16), pentru că mesajele stau la coadă în
   ElasticMQ care e deja saturat. Punctul dulce aici e c16–c32.
4. **La localhost, latența dus-întors e ~35–40 ms p50** — surprinzător de mare
   pentru „fără rețea". Nu e timp de transport, e overhead pe apel: semnare
   botocore + parsare XML în aiobotocore + procesare ElasticMQ. Acesta e un cost
   fix per apel care va conta pe SQS real, unde se adaugă și RTT-ul de rețea.

## Serializare: JSON vs msgpack

Măsurat în proces (50.000 iterații), pe mesajul de test:

| Serializare | Encode | Decode | Dimensiune |
|---|---|---|---|
| json | 6.46 µs/msg | 4.25 µs/msg | 166 B |
| msgpack | 3.90 µs/msg | 2.19 µs/msg | 131 B |

msgpack e ~1.7× mai rapid la encode, ~1.9× la decode, și cu ~21% mai mic pe
sârmă. **Dar** costul de serializare (câțiva µs) e cu trei ordine de mărime sub
costul unui apel SQS (zeci de ms). Se vede și în tabelul de mai sus: `msgpack_c32`
nu e mai rapid end-to-end decât `json_c32` — de fapt producer-ul e ușor mai mic,
în marja de zgomot.

**Concluzie:** la volumele astea, serializarea **nu** e pe calea critică; apelul
SQS e. Rămânem pe **JSON** ca implicit (lizibil, ușor de depanat, corp text
valid pe sârmă). msgpack devine relevant doar dacă mutăm vreodată transportul pe
ceva unde per-mesaj contează (ex. cozi cu payload binar și debite mult mai mari).

## Descoperiri neașteptate

### 1. Pool-ul de conexiuni al clientului SQS e un plafon dur de concurență

Prima versiune a benchmark-ului **se bloca** la testul dus-întros: producer-ul nu
trimitea niciun mesaj, la infinit. Cauza: botocore are implicit
`max_pool_connections = 10`. Consumatorii care fac long polling **țin** fiecare o
conexiune ocupată până la `wait_time_seconds`. Cu 16 consumatori concurenți,
toate cele 10 conexiuni erau monopolizate de long-poll-uri, iar producer-ul nu
mai putea obține o conexiune — înfometare (starvation) totală.

Fix: `max_pool_connections` configurabil (`SqsConfig`), dimensionat peste numărul
de producători + consumatori concurenți. **Regulă:** pool-ul trebuie să fie
strict mai mare decât concurența, altfel long-poll-urile înfometează scrierile.
Valoarea implicită botocore de 10 e nepotrivită pentru orice workload serios.

### 2. Sample-uri mici mint

O rulare de probă cu 500 de mesaje raporta producer ~5 900 msg/s. La 50.000 de
mesaje, sustained, cifra reală e ~3 000–3 700. Sample-urile mici măsoară warm-up
și cache, nu regim stabil. Toate cifrele de baseline sunt pe ≥50.000 mesaje din
acest motiv.

## Ce lipsește (de completat)

- [ ] **SQS real (AWS).** Nu există credențiale AWS pe mașina de dezvoltare, deci
      jumătatea „SQS real" a baseline-ului nu e încă rulată. Codul o suportă
      (rulează `bench_sqs.py` fără `--endpoint-url`, cu credențiale în mediu).
      Fără această cifră, nu știm cât din plafon e ElasticMQ vs. rețea AWS.
      **Aceasta e singura piesă din M0 rămasă deschisă.**

## Rezumat baseline (de referință pentru M1+)

> **ElasticMQ local, un proces, JSON, c32:**
> producer ≈ **3 600 msg/s**, consumer ≈ **1 550 msg/s**,
> dus-întors **p50 40 ms / p95 76 ms / p99 94 ms**, zero pierderi.
>
> Plafon dat de: numărul de apeluri API către ElasticMQ (single-JVM) + overhead
> de semnare/parsare per apel. Serializarea și rețeaua **nu** sunt încă factori.

---

# M1 — Flux complet HTTP → SQS → engine → SQS → HTTP

Trei procese separate (`ingress` HTTP, `engine` pass-through, `egress` HTTP),
comunicând doar prin cozile `ingress` și `egress` din ElasticMQ. Sink HTTP
simulat (`tools/http_sink.py`) măsoară latența end-to-end reală din `received_at`.

Toate rulările pe același mediu ca M0 (ElasticMQ local, câte un proces per etapă,
JSON, 16 workeri/proces). Zero mesaje pierdute în toate testele.

## Verificare corectitudine

`tools/loadgen.py` a trecut 122.000 de mesaje prin flux (2k + 100k + 20k). Metrici
Prometheus, perfect consistente la capătul lanțului:

```
relay_ingress_received_total{source="http"}          122000
relay_engine_processed_total                          122000
relay_egress_submitted_total{connector="http",result="success"} 122000
relay_end_to_end_duration_seconds_count               122000
```

## Latență de tranzit (sarcină sustenabilă — 400 msg/s, 20.000 mesaje)

Sub plafonul de debit, deci fără coadă acumulată: aceasta e latența **reală** de
traversare a conductei.

| Segment | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Accept la ingress (HTTP 202) | 5.8 ms | 9.5 ms | 50.9 ms | — |
| **End-to-end (ingress→sink)** | **176 ms** | **339 ms** | **422 ms** | 534 ms |

Cele ~176 ms p50 = 2 hop-uri SQS (fiecare cu costul fix de ~40–90 ms din M0) +
timpii de publish/consume/delete + POST-ul HTTP final. Coerent cu baseline-ul:
platforma adaugă două traversări de coadă peste dus-întorsul de 40 ms al unei
singure cozi.

## Debit la saturație (100.000 mesaje, submit nelimitat)

| Metrică | Valoare |
|---|---|
| Debit end-to-end (drenare conductă) | **~640 msg/s** |
| Debit accept ingress (cu backpressure) | ~1 265 msg/s |
| Respinse 429 (reîncercate) | 3,9 M |
| e2e p50 / p99 **la saturație** | **54 s / 76 s** |

Interpretare (important, ca să nu se citească greșit p50 = 54 s):

- Oferta de sarcină (~5 000 msg/s la accept) depășește **cu mult** plafonul de
  drenare al conductei (~640 msg/s). Se formează o coadă uriașă, iar latența
  end-to-end devine **timp de ședere în coadă**, nu timp de procesare. La 100k
  mesaje și 640/s, ultimul mesaj așteaptă ~77 s — exact ce se vede.
- Cele 3,9 M de 429 sunt un artefact al load-gen-ului (32 de workeri reîncearcă
  la fiecare 50 ms pe un buffer plin), **nu** un semn de pierdere: toate cele
  100k au fost în final acceptate și livrate. Dovedește însă că backpressure-ul
  funcționează — ingress-ul respinge, nu se umflă în memorie.

## Cost-ul platformei peste coadă

| | Debit | Raport față de baseline |
|---|---|---|
| Baseline M0 — consumer single-hop | ~1 550 msg/s | 1.00× |
| M1 — flux complet end-to-end | ~640 msg/s | **0.41×** |

Fluxul complet livrează ~41% din debitul unui singur consumator SQS. Motivul e
structural: fiecare mesaj traversează broker-ul de ~5 ori (1 publish la ingress +
consume+delete la engine + publish la engine + consume+delete la egress), iar
toate aceste operații lovesc **același** ElasticMQ single-JVM, care era deja
plafonul din M0. Engine-ul și egress-ul, fiind pass-through, adaugă CPU
neglijabil — plafonul rămâne broker-ul de cozi, nu Python-ul nostru.

## Concluzie M1

- Conducta completă funcționează pe calea HTTP, cu corectitudine dovedită
  (122k mesaje, zero pierderi, metrici consistente).
- **Latență de tranzit:** ~176 ms p50 / 422 ms p99 la sarcină sustenabilă.
- **Plafon de debit:** ~640 msg/s end-to-end, adică 0.41× din baseline-ul de
  coadă. Gâtuirea e ElasticMQ (single-JVM), amplificată de cele ~5 traversări de
  broker per mesaj. Pe SQS real, unde broker-ul scalează orizontal, ne așteptăm
  ca acest raport să crească (gâtuirea se mută pe rețea / conexiuni), dar asta
  rămâne de măsurat (vezi „Ce lipsește").

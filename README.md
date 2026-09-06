# relay — platformă de rutare SMS de mare volum (POC)

Document de predare. POC închis. Acesta e punctul unic de intrare în proiect,
pentru conducere (secțiunile 1–4, 8–9) și pentru un asistent care preia codul
(secțiunile 5, 6, 10). Cifrele detaliate stau în `docs/BENCHMARKS.md`; aici sunt
doar cele esențiale, cu trimiteri.

---

## 1. Ce este acest proiect

Businessul e **SMS wholesale (A2P)**: primim SMS-uri de la clienți (bănci,
magazine, aplicații care trimit coduri, notificări, campanii), le trimitem mai
departe către operatori sau agregatori, și câștigăm din diferența de preț. Ca să
fie profitabil, sistemul trebuie să ducă volume mari, rapid și fără să piardă
mesaje.

Am construit un **prototip al conductei**: trei programe independente legate prin
cozi de mesaje — unul care *primește* SMS-uri (prin HTTP, prin protocolul telecom
SMPP, sau din fișiere CSV de campanie), unul care le *procesează*, și unul care le
*trimite* mai departe (prin HTTP sau SMPP). Am scris de la zero suportul pentru
SMPP, inclusiv codarea corectă a textului (diacritice românești, emoji, mesaje
lungi rupte în segmente).

E un **POC, nu un produs**: scopul lui a fost să răspundă la o singură întrebare —
*poate arhitectura asta să ducă volume mari, și unde e limita?* — nu să fie gata
de producție. Deliberat nu conține logică de business, bază de date sau interfață
(vezi secțiunea 4). Rezultatul cel mai valoros nu e codul, ci ce am aflat despre
unde e limita — și am aflat-o în câteva săptămâni, înainte să construim tot.

---

## 2. Rezultatul, în trei cifre

- **~10.000 mesaje/secundă per proces** ⭐ — cât face codul platformei singur, fără
  brokerul de cozi în cale. **Acesta e răspunsul la întrebarea de la început.**
- **~2.000 mesaje/secundă** — cât face fluxul complet pe brokerul de test
  (ElasticMQ) în condiții normale. Aici brokerul e limita, nu codul.
- **1.000.000 mesaje livrate, zero pierdute** — pe fiecare cale de intrare,
  corectitudine dovedită la scară.

**Concluzia, în limbaj simplu: platforma nu e gâtuirea.** Limita de ~2.000/s
măsurată vine din brokerul de cozi folosit în teste (o unealtă de dezvoltare care
rulează într-un singur proces), nu din codul nostru. (Detalii și cele patru cifre
de debit în context: `docs/BENCHMARKS.md`.)

---

## 3. Ce funcționează acum

Toate cele cinci căi funcționează end-to-end. Toate cifrele sunt pe **Linux +
ElasticMQ**, nu pe SQS real (vezi secțiunea 9).

| Cale | Stare | Validat la |
|---|---|---|
| Intrare **HTTP** (`POST /v1/messages`, `/batch`) | funcțional | 1M mesaje |
| Intrare **SMPP** (server, acceptă binduri) | funcțional | 1M mesaje |
| Intrare **CSV** (streaming, ritm reglabil) | funcțional | 5M linii |
| Ieșire **HTTP** (pool de conexiuni) | funcțional | 1M mesaje |
| Ieșire **SMPP** (client, pool de binduri) | funcțional | 1M + reziliență + throttling |

Mai există: **codec SMPP propriu** testat la nivel de octet, **simulatoare de
furnizor** (HTTP și SMPP, cu defecte configurabile), **generator de sarcină**,
**unelte de benchmark** (baseline izolat pe cozi, scalare orizontală), generator
de CSV. ~75 de teste, `ruff` + `mypy --strict` curate.

---

## 4. Ce NU e implementat

La fel de important ca secțiunea precedentă. Cine sare peste ea va face presupuneri
false. `[scop]` = ieșit din scopul POC; `[amânat]` = amânat deliberat, de făcut în
faza următoare.

- **Bază de date, persistență, orice stare care supraviețuiește repornirii** —
  `[scop]`. Toată starea e în memorie, prin design.
- **Client, agregator, tarif, sold, facturare** — `[scop]`. Nicio noțiune de business.
- **Rutare, rating, antifraudă** — `[scop]`. Engine-ul e pass-through (nu face nimic).
- **Deduplicare (idempotență)** — `[amânat]`. Semantica e „cel puțin o dată":
  ~**0,29% duplicate** la pierderi de răspuns. Tradus: la **1 milion de mesaje =
  ~2.900 SMS trimise de două ori** → abonați deranjați și cost dublu. Obligatoriu
  înainte de producție.
- **Pipeline de DLR** — `[amânat]`. Confirmările de livrare doar se parsează și se
  loghează, nu se rutează înapoi.
- **Reasamblare multi-part la ingress** — `[amânat]`. Mesajele concatenate trec
  transparent (pass-through), nu se reasamblează.
- **UCP** — `[scop]`. Alt protocol, menționat ca „de văzut", neimplementat.
- **Autentificare reală, portal, interfață web** — `[scop]`. Doar un token static
  în header și credențiale statice SMPP.

---

## 5. Cum circulă un mesaj prin sistem

```
  client                INGRESS            SQS            ENGINE           SQS            EGRESS            furnizor
    │  HTTP/SMPP/CSV        │           coada „ingress"      │        coada „egress"        │      HTTP/SMPP     │
    └────────────────────► [1] ──────────► [2] ───────────► [3] ─────────► [2] ───────────► [4] ──────────────► │
```

1. **Ingress** (`src/relay/ingress/`) primește mesajul, îi dă un **ULID** unic
   (`common/ids.py`), construiește anvelopa canonică `Message` (`common/message.py`)
   și ștampilează `received_at`. Îl pune într-un buffer intern mărginit; dacă e
   plin, respinge (HTTP 429 / SMPP `ESME_RMSGQFUL`) — nu bufferează la nesfârșit.
2. Workeri de publicare scot din buffer și scriu în **coada SQS** în loturi de 10
   (`queues/sqs.py`). Coada e stratul de decuplare — cele trei procese nu se
   cunosc între ele.
3. **Engine** (`src/relay/engine/`) citește din coada `ingress`, trece mesajul
   printr-un **pipeline** de etape (`engine/pipeline.py` — deocamdată o etapă care
   nu face nimic), și îl scrie în coada `egress`.
4. **Egress** (`src/relay/egress/`) citește din coada `egress` și trimite mesajul
   la furnizor. Pe SMPP, îl codează (GSM 03.38 / UCS-2, segmentare), îl trimite pe
   una din bindurile din pool, clasifică răspunsul (succes / retry / drop). La
   livrare, calculează latența end-to-end din `received_at`, neschimbat de la pas 1.

Backpressure la fiecare capăt, retry doar pe erori temporare, `received_at` singura
sursă de latență reală. Detalii de protocol: `docs/SMPP.md`.

---

## 6. Structura codului

`►` = pe calea critică de date (hot path). `·` = unealtă auxiliară / infrastructură.

```
src/relay/
  common/
    message.py    ► anvelopa canonică Message + serializare JSON/msgpack
    ids.py        ► generare ULID (monoton în aceeași milisecundă)
    config.py     · încărcare YAML + interpolare ${VAR} din mediu
    logging.py    · logare JSON structurată
    metrics.py    · definiții metrici Prometheus
    worker.py     ► bucla de consum partajată + oprire curată (SIGTERM)
  queues/
    base.py       ► interfețele Producer/Consumer/QueueBackend
    sqs.py        ► backend SQS (aioboto3, batching, long polling)
    memory.py     ► backend in-memory (teste, și baseline fără broker)
    factory.py    · alege backend-ul din config
  smpp/                            (codec propriu, independent de rest)
    constants.py  ► command_id, ESME_*, clasificarea erorilor (retry sau nu)
    pdu.py        ► encoder/decoder PDU, robust la mesaje malformate
    encoding.py   ► GSM 03.38 / UCS-2, împachetare 7-bit, segmentare UDH
    session.py    ► mașina de stări SMPP: window, enquire_link, reconectare
    server.py     ► partea de server (ingress): acceptă binduri, validează
  ingress/
    http_connector.py  ► API HTTP (FastAPI), buffer mărginit → 429
    smpp_connector.py  ► server SMPP → coadă; pass-through UDH în attributes
    csv_connector.py   ► citire în streaming, memorie mărginită
    main.py            · alege conectorul din config (http/smpp/csv)
  engine/
    pipeline.py   ► listă de etape async (pass-through acum)
    main.py       ► consumă ingress → pipeline → publică egress
  egress/
    http_connector.py  ► POST pe endpoint, pool de conexiuni
    smpp_connector.py  ► pool de binduri, clasificare erori, DLR (doar log)
    shaper.py          ► token bucket pentru TPS agregat pe furnizor
    main.py            · alege conectorul din config (http/smpp)
tools/            · loadgen, http_sink, smpp_sink, bench_sqs, bench_scaling, gen_csv
config/           · ingress/engine/egress *.yaml (+ variante smpp/csv)
```

---

## 7. Decizii de design și de ce

- **Codec SMPP propriu, nu librărie externă.** Ca să înțelegem protocolul și să
  putem adapta comportamentul per furnizor (capcane reale, ex. formatul
  `message_id`). Testat la nivel de octet, nu doar round-trip.
- **Trei procese care comunică doar prin cozi.** Decuplare completă: fiecare
  pornește, se oprește și se scalează independent. Coada e și punctul unde măsurăm
  plafonul.
- **Backpressure real (429 / `ESME_RMSGQFUL`), nu buffere nelimitate.** Sub sarcină
  susținută, un buffer nelimitat duce la OOM. Măsurat: buffer-ul respinge corect,
  fără pierderi (`docs/BENCHMARKS.md`).
- **Fără bază de date în POC.** Scopul era să măsurăm conducta goală; orice
  persistență ar fi contaminat măsurătoarea. Starea persistentă e prima cerință a
  fazei următoare, nu a POC-ului.
- **Scalarea pe procese, nu pe binduri într-un proces.** Măsurat: bindurile
  suplimentare într-un proces nu adaugă debit (un event loop = un nucleu); adaugă
  când sunt plafonate extern per bind (cazul de producție). Instanțele adaugă nuclee.
- **Nu recomandăm schimbarea clientului SQS sau a limbajului pentru debit.**
  Măsurat: optimizarea celei mai vizibile ineficiențe botocore dă ~5%, nu dublare.
  Plafonul e dus-întorsul la broker + protocolul, nu CPU-ul clientului.
- **`received_at` se propagă neschimbat.** E singura sursă de adevăr pentru latența
  end-to-end reală, de la accept la livrare.

Argumentele complete, cu alternativele respinse: `docs/ARCHITECTURE.md`.

---

## 8. Ce am aflat

Povestea, nu tabelele (acelea sunt în `docs/BENCHMARKS.md`).

Am pornit de la un plafon aparent de ~640 msg/s. S-a dovedit că **~2/3 din el era
platforma de test** (Windows, cu un event loop lent la I/O de rețea); pe Linux
plafonul real era ~2.000/s. Surpriză măsurată: **uvloop nu ajută** acest workload
(e marginal mai lent) — gâtuirea nu e event loop-ul.

Concluzia inițială — *„gâtuirea e brokerul, nu Python"* — a fost **revizuită de
trei ori** până s-a susținut pe date. Întâi am crezut că e brokerul; apoi că e
CPU-ul clientului de cozi (botocore); apoi am crezut că sunt două plafoane
distincte. Experimentele decisive au arătat altceva: **codul platformei face
~10.000/s per proces fără broker**, iar limita de ~2.000/s e **interacțiunea cu
brokerul de test** (dus-întors de rețea + protocol), un singur plafon măsurat pe
căi diferite. Sink-urile simulate nu erau limita; codecul SMPP nu era limita;
botocore nu era, în esență, limita.

Faptul că am corectat concluzia de trei ori nu e o slăbiciune a raportului — e ce
îi dă credibilitate. Fiecare revizuire a venit dintr-un experiment care putea
infirma afirmația, nu dintr-o presupunere.

---

## 9. Ce urmează

Primele trei consecințe tehnice (lista completă în `docs/NEXT-STEPS.md`):

1. **Reducerea traversărilor de coadă de la 2 la 1.** Azi fiecare mesaj trece prin
   broker de două ori. O singură trecere ar aproape dubla capacitatea per broker
   și ar înjumătăți costul SQS. Decizie de design, nu optimizare de cod.
2. **Deduplicare pe ULID.** Rezolvă problema duplicatelor (vezi secțiunea 4).
   Obligatoriu înainte de producție.
3. **Backoff adaptiv la `ESME_RTHROTTLED`.** Azi platforma se bazează pe redelivery
   (irosește round-trip-uri); un shaper care reacționează la throttling ar fi robust.

### Întrebarea deschisă — decizie pentru conducere

**Nu am rulat niciodată pe AWS SQS real.** Toate cifrele sunt pe ElasticMQ, un
broker de dezvoltare care rulează într-un singur proces și devine el însuși
gâtuirea. **Ce NU putem afirma fără SQS real:** că debitul crește liniar cu
numărul de instanțe pe un broker care scalează. Pe ElasticMQ crește sub-liniar din
cauza brokerului, nu a codului — dar asta rămâne **ipoteză** până o măsurăm. Cost:
**~30 de minute și sub 5 dolari.** E singura gaură reală rămasă din prototip.

---

## 10. Pentru un asistent AI care preia proiectul

### Ordinea de citire
1. `README.md` (acest fișier) — harta.
2. `docs/RUNBOOK.md` — cum pornești și rulezi tot, comenzi exacte.
3. `docs/ARCHITECTURE.md` — deciziile și alternativele.
4. `docs/SMPP.md` — protocolul, dacă atingi `src/relay/smpp/`.
5. `docs/BENCHMARKS.md` — toate cifrele, măsurat vs dedus.
6. `docs/NEXT-STEPS.md` — ce se construiește mai departe.

### Invarianți — nu strica
- Nimic blocant pe calea de date; tot I/O-ul e async.
- Backpressure real, niciodată buffere nelimitate.
- `submit_sm_resp` sub 100 ms — acceptă, pune în coadă, răspunde; niciodată
  procesare sincronă (blochezi window-ul clientului).
- Retry doar pe erori **temporare**, niciodată pe permanente (vezi `smpp/constants.py`).
- Shaping TPS **agregat pe furnizor**, nu per bind.
- `received_at` se propagă neschimbat prin tot fluxul.
- `ruff` și `mypy --strict` curate înainte de orice commit.

### Capcane care par bug-uri și nu sunt
- `message_id` poate veni **hex în `submit_sm_resp` și zecimal în DLR**;
  `smpp_sink` reproduce ambele intenționat.
- **Diacriticele românești forțează UCS-2** și dublează numărul de segmente —
  corect, nu bug.
- Vectorul `E8329BFD46` pentru „hello" care circulă online **e greșit** pentru
  exact 5 septeți; corect e `...06` (derivat pe hârtie în teste).
- **uvloop e marginal mai lent** pentru acest workload — măsurat, nu greșeală de config.
- **Toate cifrele de benchmark sunt pe ElasticMQ, nu pe SQS real.**

### Convenții
- Commit-uri: **Conventional Commits**. Tipuri: `feat|fix|test|docs|perf|refactor|chore|bench`.
  Scopuri: `queues|smpp|ingress|engine|egress|tools|config|common`.
- Tag-uri de milestone: `m0-sqs-baseline` … `m4-benchmarks`, `poc-complete`.
- Configurări în `config/*.yaml` (interpolare `${VAR:default}`); comutarea pe SQS
  real e schimbare de config, nu de cod (vezi RUNBOOK secțiunea 6).
- Teste: `pytest`. Benchmark-urile se rulează pe **Linux**, nu pe Windows.

### Unde e starea
**Exclusiv în memorie.** Nimic nu supraviețuiește repornirii, prin design. Nu
căuta o bază de date — nu există.

---

## 11. Harta documentației

| Document | Ce conține | Pentru cine |
|---|---|---|
| `README.md` | Harta proiectului, rezultat, ce e/nu e | Toți |
| `docs/BENCHMARKS.md` | Toate cifrele, rezumat executiv, recomandare SQS, limitări | Conducere + tehnic |
| `docs/ARCHITECTURE.md` | Decizii de design, alternative, ce a confirmat/infirmat măsurarea | Tehnic |
| `docs/SMPP.md` | Structura PDU, encoding, capcane de protocol | Cine atinge SMPP |
| `docs/RUNBOOK.md` | Pornire, benchmark-uri, comutare pe SQS real — comenzi exacte | Cine rulează |
| `docs/NEXT-STEPS.md` | Consecințe tehnice pentru faza următoare | Faza următoare |

---

## 12. Glosar

- **A2P** — Application-to-Person: SMS trimise de o aplicație către o persoană (coduri, notificări).
- **SMPP** — Short Message Peer-to-Peer: protocolul telecom standard pentru trimiterea de SMS-uri în volum.
- **PDU** — Protocol Data Unit: un pachet SMPP (header 16 octeți + corp).
- **bind** — o sesiune SMPP autentificată pe o conexiune TCP.
- **ESME** — External Short Message Entity: clientul SMPP (noi, față de operator).
- **TPS** — Transactions Per Second: mesaje/secundă, de obicei plafonat contractual per bind.
- **DLR** — Delivery Receipt: confirmarea că un SMS a ajuns la abonat.
- **MSISDN** — numărul de telefon în format internațional (E.164, ex. +40712345678).
- **UDH** — User Data Header: antet care leagă segmentele unui SMS lung concatenat.
- **MCC/MNC** — Mobile Country/Network Code: identifică țara și operatorul.

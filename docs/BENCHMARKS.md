# BENCHMARKS

Rezultatele măsurate ale proiectului `relay`. Documentul e structurat pe cinci
secțiuni; **măsurătorile** (secțiunea 2) sunt separate strict de **interpretări**
(secțiunea 3). Orice afirmație din secțiunea 3 trimite la o măsurătoare din
secțiunea 2. Ce nu e susținut de date e marcat explicit ca **ipoteză**.

Istoric: M0 (baseline cozi) și M1 (flux complet) au fost măsurate inițial doar pe
ElasticMQ, pe Windows, cu o singură instanță, iar concluzia „gâtuirea e broker-ul,
nu Python" a fost trasă fără dovadă. M1.5 e runda care validează sau infirmă acea
concluzie.

---

## 1. Metodologie

### Hardware
- CPU: 12th Gen Intel Core i7-1260P (16 loguri)
- Mașină: laptop, Windows 11 Pro (10.0.26200)

### Medii software

| Mediu | OS | Python | Event loop | Note |
|---|---|---|---|---|
| **Windows** | Windows 11 | CPython 3.12.4 (python.org) | asyncio (ProactorEventLoop) | fără uvloop (nu există pe Windows) |
| **Linux** | Ubuntu pe WSL2 (kernel 6.18) | CPython 3.12.3 | asyncio (epoll) sau uvloop 0.22 | venv separat |

### Mediu de cozi
- **ElasticMQ 1.6.7 native**, în Docker Desktop, `localhost:9324`. Un singur
  proces JVM, in-memory. Accesibil identic din Windows și din WSL2.
- **AWS SQS real: NEMĂSURAT.** Nu există credențiale AWS pe mașină (a doua oară;
  M0 le cerea). Vezi secțiunea 5.

### Dimensiunea rulărilor
- Baseline cozi (`bench_sqs.py`): 50.000 mesaje throughput, 10.000 latență, c32.
- Flux complet (`bench_scaling.py` + `loadgen.py`): 20.000 (paced) și 60.000
  (saturație) mesaje. Mesaj de test: `to=+40712345678`, text 20 caractere.
- `consumed`/`accepted` confirmă zero pierderi în toate rulările.

### Reproducere
```bash
# baseline cozi, cu alegerea event loop-ului
python tools/bench_sqs.py --endpoint-url http://localhost:9324 --count 50000 --concurrency 32
python tools/bench_sqs.py --endpoint-url http://localhost:9324 --count 50000 --concurrency 32 --uvloop
# scalare orizontală 1/2/4 instanțe
python tools/bench_scaling.py --instances 1 2 4 --count 60000 --serializer json
# latență de tranzit (paced sub plafon)
python tools/loadgen.py --rate 400 --count 20000
```

---

## 2. Rezultate măsurate

**Fără interpretare.** Fiecare cifră poartă mediul în care a fost obținută.

### 2.1 Baseline cozi — `bench_sqs.py`, ElasticMQ, 50k, c32

| Mediu | Producer (msg/s) | Consumer (msg/s) | RT p50 | RT p95 | RT p99 |
|---|---|---|---|---|---|
| Windows · asyncio | 3 593 | 1 550 | 40.3 ms | 75.5 ms | 93.6 ms |
| Linux · asyncio | **8 465** | **4 549** | **13.6 ms** | 20.1 ms | 24.9 ms |
| Linux · uvloop | 8 184 | 4 149 | 16.5 ms | 25.8 ms | 33.4 ms |

### 2.2 Flux complet — latență de tranzit (paced 400 msg/s, sub plafon)

| Mediu | e2e p50 | e2e p95 | e2e p99 | e2e max | Respinse 429 |
|---|---|---|---|---|---|
| Windows · asyncio | 176.0 ms | 339.0 ms | 422.3 ms | 534 ms | 0 |
| Linux · uvloop | **77.5 ms** | **126.8 ms** | **212.0 ms** | 300 ms | 0 |

### 2.3 Flux complet — debit la saturație (submit nelimitat)

| Mediu | Instanțe eng+egr | Mesaje | Debit e2e (msg/s) | Respinse 429 | 429/livrat |
|---|---|---|---|---|---|
| Windows · asyncio | 1 | 100 000 | 641 | 3 914 500 | 39.1× |
| Linux · uvloop | 1 | 60 000 | 2 040 | 112 200 | 1.9× |

### 2.4 Scalare orizontală — Linux · uvloop · ElasticMQ, 60k, saturație

| Instanțe engine+egress | Debit e2e (msg/s) | Factor vs 1 instanță |
|---|---|---|
| 1 | 2 040 | 1.00× |
| 2 | 2 613 | 1.28× |
| 4 | 2 319 | 1.14× |

> Scalare pe **SQS real: nemăsurată** (fără credențiale).

### 2.5 JSON vs msgpack

Cost de serializare, în proces (50.000 iterații, mesaj de test):

| Serializare | Encode | Decode | Dimensiune |
|---|---|---|---|
| json | 6.46 µs/msg | 4.25 µs/msg | 166 B |
| msgpack | 3.90 µs/msg | 2.19 µs/msg | 131 B |

Impact asupra debitului end-to-end (Linux · uvloop · ElasticMQ · 1 instanță · 60k):

| Serializare | Debit e2e (msg/s) |
|---|---|
| json | 2 040 |
| msgpack | 1 979 |

---

## 3. Interpretare

Fiecare afirmație trimite la măsurătoarea care o susține. Deducțiile fără dovadă
completă sunt marcate **[IPOTEZĂ]**.

### 3.1 O mare parte din „plafonul de 640 msg/s" era platforma de test
Din **2.1** și **2.3**: același baseline de cozi dă pe Linux ~2.4× debit la producer
(8465 vs 3593) și ~2.9× la consumer (4549 vs 1550) față de Windows, cu latență de
~3× mai mică (13.6 vs 40.3 ms). Fluxul complet urcă de la 641 la 2040 msg/s
(**3.2×**) doar prin mutarea pe Linux. **Concluzia M1 de 640 msg/s era
contaminată de ProactorEventLoop-ul de pe Windows.** Aproximativ două treimi din
acel plafon erau platforma de test.

### 3.2 uvloop NU ajută acest workload — contrazice presupunerea din prompt
Din **2.1**: pe Linux, uvloop e **mai lent** decât asyncio implicit (producer
8184 vs 8465, consumer 4149 vs 4549, latență p50 16.5 vs 13.6 ms). Motivul
plauzibil: gâtuirea nu e în event loop, ci în CPU-ul Python al lui botocore
(semnare + parsare XML) per apel SQS; I/O-ul mai rapid al lui uvloop nu are ce
optimiza și adaugă un mic overhead. **Presupunerea implicită din prompt — că
uvloop ar ridica plafonul — e infirmată de măsurătoare.** Am păstrat uvloop cablat
(nu strică pe alte workload-uri), dar nu e o pârghie aici.

### 3.3 Pe ElasticMQ, gâtuirea ESTE broker-ul — acum cu dovadă
Din **2.4**: adăugarea de instanțe engine+egress **nu** crește debitul — plafonează
la ~2000–2600 msg/s și chiar regresează la 4 instanțe (contenție pe același broker).
Coroborat cu **2.1**: un singur proces `bench_sqs` saturează deja ElasticMQ la
~8500 operații-trimitere/s și ~4500 mesaje-consum/s (≈9k operații SQS/s). Fluxul
complet la 2040 msg/s × ~5 traversări de broker per mesaj (1 publish ingress +
consume+delete engine + publish engine + consume+delete egress) ≈ 10k operații/s
— **exact plafonul de operații al ElasticMQ.** Deci pe ElasticMQ gâtuirea e
broker-ul (JVM unic), iar codul nostru pass-through adaugă CPU neglijabil.
Concluzia M1 era **corectă pentru ElasticMQ** — dar din motive pe care abia acum
le-am dovedit, nu cum fusese afirmată.

### 3.4 Generalizarea la SQS real rămâne **[IPOTEZĂ]**
Testul de scalare pe ElasticMQ nu poate distinge „broker-bound" de „ar fi
code-bound pe un broker care scalează". SQS real e distribuit și scalează
orizontal pe cozi Standard; pe el, gâtuirea broker-ului ar dispărea și s-ar muta
în altă parte — codul nostru, pool-ul de conexiuni, sau rețeaua. **Dacă
arhitectura noastră scalează pe un broker care scalează e o ipoteză
nemăsurată.** Experimentul care ar valida-o: **repetă 2.4 pe SQS real** și vezi
dacă factorul de scalare devine ~liniar.

### 3.5 Backpressure-ul funcționează; retry-ul din loadgen era artefactul
Din **2.3**: cele 3,9 M de respingeri de pe Windows (39×/mesaj) erau o buclă de
retry strânsă (50 ms fix, deque partajat) în loadgen — un artefact al
generatorului, nu al platformei. După înlocuirea cu backoff exponențial + jitter,
raportul scade la **1.9×/mesaj** (**~20× mai puține**). Cele ~1.9 respingeri
rămase per mesaj sunt backpressure **legitim**: oferta (~6000 msg/s la submit)
depășește plafonul de drenare (~2040 msg/s), deci buffer-ul e plin mare parte din
timp și ingress-ul respinge corect cu 429. **Dimensionarea buffer-ului intern
(20.000) e rezonabilă** — dă ~10 s de tampon la debitul curent; nu e prea mică.
(Observație raportată, nemodificată — conform regulii de scop M1.5.)

### 3.6 La saturație, latența e timp de coadă, nu de tranzit
Din **2.2** vs **2.3**: paced sub plafon, e2e p50 e 77 ms (Linux); la saturație
p50 sărea la 54 s pe Windows. Diferența e timpul de ședere în coadă când oferta
depășește drenarea, nu costul de procesare. De aceea latența se raportează doar
din rulări paced (2.2), iar saturația (2.3) se raportează doar ca debit.

### 3.7 Serializarea nu e pe calea critică — nici în proces, nici end-to-end
Din **2.5**: msgpack e ~1.7× mai rapid la encode și ~21% mai mic, dar debitul
end-to-end e identic (2040 vs 1979, în marja de zgomot), pentru că serializarea
(µs) e cu ordine de mărime sub costul unui apel SQS (ms). **Recomandare: JSON** —
lizibil, ușor de depanat, corp text valid pe sârmă. msgpack ar conta doar dacă
dimensiunea payload-ului ar deveni factor de cost (payload-uri mari, sau costul
de transfer pe SQS real — de remăsurat acolo).

---

## 4. Concluzie despre plafon (revizuită)

> **ÎNLOCUITĂ de secțiunea „Pre-M4 — cinci experimente decisive" de la finalul
> documentului.** Concluzia de mai jos (M1.5) atribuia plafonul broker-ului
> ElasticMQ; experimentele pre-M4 arată că sunt DOUĂ plafoane suprapuse, iar cel
> per-proces e calea clientului SQS (botocore + RTT), nu doar broker-ul.

**Măsurat, cu certitudine:**
- Pe **ElasticMQ**, plafonul fluxului complet e **~2000–2600 msg/s** (Linux),
  dat de capacitatea de operații a broker-ului JVM unic (~9–10k op/s), **nu** de
  codul Python pass-through, **nu** de serializare, **nu** de event loop.
  Dovada: scalarea plafonează (2.4) + aritmetica operațiilor (3.3).
- Platforma de test contează enorm: Windows ProactorEventLoop tăia ~2/3 din debit
  (3.1). uvloop nu ajută (3.2).

**Ipoteză, încă nevalidată:**
- Că arhitectura `relay` scalează orizontal pe un broker care scalează (SQS real).
  ElasticMQ, fiind JVM unic, nu poate răspunde la asta. **Gradul de încredere:
  mediu** — aritmetica operațiilor sugerează că suntem broker-bound, deci pe un
  broker scalabil ar trebui să scalăm, dar contenția observată la 4 instanțe pe
  ElasticMQ (2.4, regres de la 2 la 4) e un semnal că ar putea exista și contenție
  în client (pool de conexiuni, competiție pe aceleași cozi) care s-ar manifesta
  și pe SQS. Nu știm până nu măsurăm.

**Cel mai informativ experiment rămas: scalarea 1/2/4 pe SQS real.**

---

## 5. Ce rămâne nemăsurat

- [ ] **AWS SQS real — a doua oară neîndeplinit.** Fără credențiale pe mașină.
      Blochează: baseline-ul pe SQS real (2.1), scalarea pe SQS real (2.4, testul
      decisiv), impactul dimensiunii payload-ului pe cost (2.5).
      **Cost estimat al rundei complete pe SQS**, când vor exista credențiale:
      rularea de 1M mesaje prin flux ≈ 1M × ~5 operații = ~5M cereri SQS; la
      0,40 USD/milion (Standard, după primul milion gratuit/lună) ≈ **~1,6 USD**;
      cu iterațiile de 100k și scalarea, ordin de mărime **sub 5 USD**. Sub pragul
      de 10 USD din prompt. Transfer de date neglijabil (mesaje ~166 B).
- [ ] **Scalarea pe un broker scalabil** (vezi 3.4) — ipoteza centrală a POC-ului.
- [ ] **Profilare** a proceselor pentru a localiza CPU-ul (botocore signing vs
      restul) — presupus din 3.2, nedovedit cu profiler.
- [ ] **uvloop pe workload cu mai mult I/O de rețea real** (SQS real, cu RTT) —
      s-ar putea comporta diferit decât pe ElasticMQ localhost.
- [ ] Mediul rămâne un singur laptop; fără test pe instanțe separate / rețea reală.

---

# M2 — SMPP egress (măsurători, Linux · uvloop · ElasticMQ)

Flux: HTTP ingress → SQS → engine → SQS → **SMPP egress** → `smpp_sink`.
Uneltele existente (`loadgen`, `smpp_sink`), fără unelte noi de benchmark.

## Măsurat

| Scenariu | Rezultat |
|---|---|
| Debit SMPP egress (20k mesaje, saturație, 2 binduri) | **~2 100 msg/s**, 20 000/20 000 acked, pierderi 0 |
| Debit HTTP egress (referință M1.5) | ~2 040 msg/s |
| Reziliență: `smpp_sink` omorât ~8 s în timpul traficului (8k mesaje) | **8 000/8 000 livrate, pierderi 0**; 4 026 `no_bind` reîncercate prin redelivery; ambele binduri revenite la starea „bound"; 4 conexiuni la sink (2 iniţiale + 2 reconectări) |

## Dedus

- **SMPP egress ≈ HTTP egress ca debit** (2100 vs 2040 msg/s). [din tabel] Nu e
  surprinzător: ambele sunt gâtuite de același ElasticMQ, nu de conector. Codecul
  SMPP rulează pe bytes, sincron și rapid — nu apare ca factor. Consecvent cu
  concluzia M1.5 (gâtuirea e broker-ul pe ElasticMQ).
- **Reconectarea + redelivery-ul garantează livrarea fără pierdere** la o cădere
  bruscă a furnizorului. [din testul de reziliență] Mesajele prinse în fereastra
  de indisponibilitate nu se confirmă (rezultat `no_bind`/`temporary`), deci coada
  le redă după visibility timeout — retry pe temporar, niciodată pe permanent.
- **Notă de contenție client (element deschis M1.5→M2):** nu am observat lock-uri
  sau sesiuni partajate care să limiteze scalarea în conectorul SMPP — token
  bucket-ul e partajat (intenționat, limită pe furnizor) și serializează doar
  emisia, nu procesarea. Rămâne de văzut pe SQS real dacă pool-urile de conexiuni
  (SQS + binduri SMPP) introduc contenția văzută la 4 instanțe în M1.5.

## Nemăsurat (rămâne pentru M3/M4)

- Debit prin **SMPP ingress** (serverul) vs HTTP ingress — M3.
- Comportamentul sub `ESME_RTHROTTLED` real la volum (shaper vs throttle furnizor).
- Toate cele de mai sus pe **SQS real** (fără credențiale).

## M2 (follow-up după feedback) — duplicate și scalare pe binduri

Notă de consecvență: **toate benchmark-urile de flux folosesc uvloop** (implicit
pe Linux via uvicorn + policy în engine/egress). M1.5 a arătat că uvloop e
marginal mai lent pentru acest workload, dar îl fixăm pentru comparabilitate
între milestone-uri.

### Duplicate (măsurat, nu doar acked)

Sink instrumentat să numere ULID-uri unice (TLV 0x1400), disconnect **înainte** de
`submit_sm_resp` (răspuns pierdut), procesul sink rămâne viu ca să dedublice.

| Metrică | Valoare |
|---|---|
| Mesaje logice | 8 000 |
| Primite de sink (cu ULID) | **8 023** |
| Unice | **8 000** |
| **Duplicate** | **23 (~0,29%)** |
| Pierdute | 0 |

Deduc: **garantăm „cel puțin o dată", nu „exact o dată".** Un `submit_sm_resp`
pierdut duce la redelivery → furnizorul primește mesajul de două ori. Zero
pierderi, dar ~0,29% duplicate la această rată de pierdere a răspunsului. Element
deschis pentru faza de idempotență (vezi SMPP.md).

### Scalare pe număr de binduri (măsurat)

Un singur `smpp_sink` neplafonat, saturație, 15.000 mesaje.

| Binduri | Debit e2e (msg/s) |
|---|---|
| 2 | 2 046 |
| 4 | 2 087 |
| 8 | 2 086 |

Deduc: **debitul e PLAT pe numărul de binduri** pe ElasticMQ. Toate bindurile se
leagă, dar throughput-ul nu crește — același plafon de broker ca la scalarea pe
instanțe (M1.5, 2.4). **Premisa că debitul vine din agregarea multor binduri e
NEVALIDATĂ pe ElasticMQ**: bindurile nu sunt limita, broker-ul e. Dacă ar scala pe
SQS real (broker distribuit) rămâne aceeași ipoteză nemăsurată. Ieftin de aflat,
important: dacă mai târziu pe SQS real bindurile tot nu scalează, ar fi un semnal
de contenție în client (element deschis M1.5).

---

# M3 — SMPP server (ingress) și CSV (măsurători, Linux · uvloop · ElasticMQ)

## Măsurat

| Scenariu | Rezultat |
|---|---|
| SMPP ingress — rată de acceptare (driver: 4 binduri × 10, 20k mesaje) | **3 832 submit_sm/s** acceptate, 0 `ESME_RMSGQFUL`, 0 pierdute |
| SMPP ingress — debit end-to-end (SMPP→coadă→engine→HTTP egress→sink) | **1 988 msg/s**, 20 000/20 000 livrate |
| HTTP ingress — debit end-to-end (referință M1.5) | ~2 040 msg/s |
| CSV 5M linii — memorie | **RSS de vârf PLAT ~50 MB** de la 1M la 5M linii; 4 999 951 trimise, **49 linii invalide sărite**, procesare 38 s (in-proces) |
| `submit_sm_resp` sub 100 ms | da (test unitar: <100 ms; sub sarcină, acceptare fără procesare sincronă) |

## Dedus

- **SMPP ingress ≈ HTTP ingress ca debit end-to-end** (1988 vs 2040 msg/s). [tabel]
  Ambele sunt gâtuite de ElasticMQ, nu de conector. Rata de acceptare a SMPP
  ingress (3832/s) e mai mare decât drenarea (1988/s), deci conectorul de intrare
  **nu** e limita — bufferul intern absoarbe, iar la umplere ar returna
  `ESME_RMSGQFUL` (echivalentul SMPP al lui 429; testat unitar). Consecvent cu
  concluzia că broker-ul e plafonul.
- **CSV procesează 5M linii în memorie mărginită (~50 MB, plată).** [tabel] Doar
  un chunk de rânduri + un batch de publicare sunt în memorie; RSS nu crește cu
  dimensiunea fișierului. Liniile invalide se sar și se loghează, nu opresc rularea.
  Cei 131k rânduri/s (in-proces, fără rețea) sunt rata de parsare+publish a
  conectorului, nu a conductei.
- **Backpressure la ingress SMPP e real** (`ESME_RMSGQFUL` pe buffer plin, nu
  bufferare nelimitată), la fel ca 429-ul HTTP.

## Nemăsurat / de reținut

- `ESME_RTHROTTLED` (throttle per credențial la server, și shaper-ul agregat pe
  furnizor la egress) — testat unitar, nemăsurat sub sarcină la volum.
- Reasamblarea multi-part la ingress e prin **pass-through transparent**
  (esm_class + UDH păstrate în `attributes`), nu reasamblare reală — vezi SMPP.md.
- Toate pe ElasticMQ; **SQS real rămâne nemăsurat** (fără credențiale).

---

# Pre-M4 — cinci experimente decisive (revizuiește concluzia despre plafon)

Ipoteza (a șefului): confundasem **două plafoane suprapuse** — unul per-proces și
unul de broker — și poate simulatoarele erau limita. Toate pe Linux · uvloop ·
ElasticMQ (16 nuclee). Fără AWS, fără cod nou de funcționalitate. **Toate cifrele
de mai jos sunt dintr-o singură configurație consecventă** (`wait_time=1`, feed cu
backlog mărginit), ca să fie comparabile.

## Exp. 1 — capacitatea sink-urilor (măsurat)

| Sink (1 proces) | Capacitate |
|---|---|
| http_sink | **6 545 req/s** |
| smpp_sink | **35 261 submit/s** |

**Deduc:** sink-urile NU sunt limita de ~2.000. (Ipoteza „poate e sink-ul" — infirmată.)

## Exp. 2 — ocolirea brokerului, egress izolat (măsurat, reconciliat)

Aceeași cale de egress, coadă **in-memory** (fără broker/botocore) vs ElasticMQ;
feed cu backlog mărginit (coadă superficială, ca în producție); N=40.000.

| Config | 2 binduri | 4 binduri | 8 binduri |
|---|---|---|---|
| **In-memory** | 11 007/s | 9 772/s | 10 128/s |
| **ElasticMQ** | 2 699/s | 2 804/s | 2 678/s |

**Reconciliere (răspuns la observația de 30%):** o rulare anterioară raporta
1.427/s pe ElasticMQ — a fost un **artefact de configurare** al harness-ului
(`wait_time_seconds` lăsat la valoarea implicită botocore de 20, în loc de 1 ca în
producție), NU adâncimea cozii (prefill vs fed dau ambele ~2.800). Corectat,
egress-ul izolat pe ElasticMQ face **~2.750/s**, consecvent cu fluxul complet
(~2.040/s — mai mic acolo pentru că ElasticMQ e împărțit între ingress+engine+egress).

**Deduc — cel mai important rezultat al POC-ului:**
1. **Fără broker, un proces de egress face ~10.300/s — de ~3,7× mai mult** decât pe
   ElasticMQ (~2.750). Plafonul de ~2.000 nu e conectorul SMPP (care poate 10k/s
   singur), ci **calea clientului de cozi** (dus-întorsul la broker + protocolul SQS).
2. **Debit PLAT pe binduri în ambele configurații** — un event loop = un nucleu.

## Exp. 3 — profilare (`tottime`)

**Memory (~10k/s):** codecul SMPP (`_encode_gsm7`, `encode_body`),
`dataclasses.replace`, prometheus, `socket.send`. **Zero botocore.**
**ElasticMQ (~2,7k/s), 25M apeluri (vs 8,7M):** ~60% în `epoll.poll` (așteptarea
dus-întorsului la broker) + CPU botocore (`useragent` 2,7M apeluri, `str.join`,
`dict.get`).

## Exp. 4 — cât din plafon e botocore (spike cu limită de timp)

`bench_sqs` pe ElasticMQ, cu/fără cache pe user-agent-ul botocore, și la concurență mai mare.

| Config | Producer | Consumer |
|---|---|---|
| baseline c=64 | 9 475/s | 4 234/s |
| user-agent cache c=64 | 9 804/s | 4 603/s |
| baseline c=128 | 8 257/s | 3 967/s |

**Deduc:** optimizarea celei mai vizibile ineficiențe botocore (user-agent,
2,7M apeluri) dă **~5–9%, NU dublare**. Concurența mai mare nu ajută. Deci plafonul
per-proces **NU** e CPU-ul botocore care s-ar putea elimina ieftin — e **dus-întorsul
la broker + protocolul SQS** (send = 1 apel/10; receive+delete = 2 apeluri/10). Un
client mai ușor sau alt limbaj ar da câștiguri **marginale** per proces, nu 2×.

## Exp. 5 — bindurile cu plafon TPS per bind (măsurat)

Constrângerea de producție lipsea din testul anterior: furnizorii plafonează
50–500 TPS **per bind**. Cu `smpp_sink --throttle-per-bind 200` (client minimal):

| Binduri | Debit livrat | Așteptat |
|---|---|---|
| 2 | 397/s | ~400 |
| 4 | 794/s | ~800 |
| 8 | 1 584/s | ~1 600 |

**Deduc — corecție la concluzia anterioară:** afirmasem că „premisa multor binduri
e greșită". **Nu e greșită — era netestată**, pentru că testul nu conținea
constrângerea care face bindurile necesare. Cu plafon TPS per bind (cazul de
producție), **bindurile scalează ~liniar** până la plafonul procesului (~10k/s):
ca să treci 2.000 TPS printr-un furnizor de 200 TPS/bind, ai nevoie de ~10 binduri.
Formularea corectă: **bindurile scalează când sunt plafonate extern (producție);
într-un proces nelimitat nu adaugă nimic (un nucleu).**

## Concluzie despre plafon — REVIZUITĂ (înlocuiește secțiunea 4)

Erau **două plafoane suprapuse**, confirmate:

1. **Plafon per-proces pe calea clientului de cozi: ~2.750/s (izolat) / ~2.040/s
   (în flux).** Cauza dominantă: **dus-întorsul la broker + protocolul SQS**
   (receive+delete per mesaj), NU CPU-ul botocore (exp. 4: patch ~5%), NU
   conectorul SMPP (~10k/s singur, exp. 2), NU sink-urile (exp. 1). Bindurile
   nu-l ridică (un nucleu).
2. **Plafon de broker agregat pe ElasticMQ: ~2.600/s** — unde scalarea pe instanțe
   plafona (M1.5).

**Corecție onestă (a doua oară):** „gâtuirea e broker-ul, nu Python" era incompletă;
dar nici „calea e botocore-CPU" nu e corectă — patch-ul pe botocore dă ~5%. Corect:
**calea clientului SQS (dus-întors broker + protocol multi-apel)**, peste care se
suprapune plafonul de broker. Codul nostru (SMPP, engine) nu e limita.

**Premisa arhitecturii — validată în forma reală:** debitul vine din agregarea de
**procese/instanțe** (nuclee) ȘI, la egress, din **binduri plafonate extern**
(exp. 5). NU din binduri într-un proces nelimitat. Fără broker, un proces face
10k/s. Rămâne **[IPOTEZĂ]** dacă pe **SQS real** instanțele scalează liniar sau
lovesc plafonul de RTT/protocol per proces mai devreme.

## Implicație pentru decizia de limbaj/bibliotecă (măsurat + opinie)

**Măsurat:** plafonul per-proces e RTT broker + protocol SQS, nu CPU botocore
(patch ~5%). **Opinie:** a rescrie clientul SQS sau a schimba limbajul ar da
câștiguri marginale per proces — nu merită pentru asta. Pârghiile reale sunt:
**(a) mai multe procese/instanțe** (scalare pe nuclee), **(b) un broker care
scalează** (SQS real vs ElasticMQ single-JVM), **(c) mai puține traversări de broker
per mesaj** (ex. engine care nu re-serializează). Codecul SMPP propriu se justifică
(~10k/s, nu e limita).

## Implicație comercială — duplicate

0,29% duplicate (la pierderi de `submit_sm_resp`) = la **1M mesaje, ~2.900 SMS
trimise de două ori**: abonați deranjați + **cost dublu**. Semantica e „cel puțin o
dată". Dedup pe ULID / idempotență per furnizor e **obligatorie** înainte de
producție.

# NEXT-STEPS — consecințe tehnice pentru faza următoare

Nu un plan de proiect. Lista consecințelor care rezultă din măsurători, fiecare cu
dovada. Materialul din care se construiește faza următoare, cât e proaspăt.
Cifrele sunt în `docs/BENCHMARKS.md`.

## 1. Reducerea traversărilor de coadă (2 → 1)

Azi fiecare mesaj trece prin broker de două ori (ingress→coadă→engine→coadă→egress)
= ~5 operații de broker per mesaj livrat. Măsurat: interacțiunea cu brokerul e
**singura** gâtuire (codul face ~10k/s/proces fără broker; ~2.750/s cu). O
topologie cu **o singură trecere** — ingress care publică direct pe egress, sau
engine integrat în-proces cu un capăt — ar aproape dubla capacitatea per broker și
ar înjumătăți costul SQS (se plătește per cerere). Compromisul: pierzi izolarea și
scalarea independentă a engine-ului. De cântărit când engine-ul capătă logică reală.

## 2. Deduplicare pe ULID (idempotență)

Măsurat: la pierderi de `submit_sm_resp`, ~**0,29% duplicate** (semantică „cel puțin
o dată"). Tradus: la **1M mesaje = ~2.900 SMS trimise de două ori** → abonați
deranjați + **cost dublu** pe acele mesaje. ULID-ul circulă deja end-to-end (TLV
0x1400). Faza următoare: fereastră de deduplicare per furnizor pe ULID (ex. un
store cu TTL) înainte de `submit_sm`. **Obligatoriu înainte de producție**, nu opțional.

## 3. Backoff adaptiv la `ESME_RTHROTTLED`

Măsurat: sub un furnizor plafonat, platforma livrează tot (zero pierderi) dar prin
**redelivery**, generând 71.678 de reîncercări throttled la 60k mesaje — round-trip-uri
irosite. Shaper-ul actual e un token bucket fix, nu reacționează la RTHROTTLED.
Faza următoare: shaper adaptiv (scade rata la RTHROTTLED, o crește înapoi lent) —
sau, minim, setați `tps_limit` egress la TPS-ul contractat ca emisia să se
auto-paseze sub plafon. Prima variantă e robustă la limite necunoscute.

## 4. Scalarea prin procese, nu prin binduri într-un proces

Măsurat: bindurile suplimentare într-un singur proces NU adaugă debit (un event
loop = un nucleu); dar scalează ~liniar când sunt plafonate extern per bind
(200 TPS/bind → 10 binduri = 2.000/s). Instanțele (procese) adaugă nuclee.
Consecință pentru deployment: **scalați pe orizontală cu procese/containere**
(unul per nucleu, sau mai multe pe mașină), nu prin binduri într-un proces.
Bindurile se dimensionează după contractul furnizorului (TPS/bind), nu după debit.

## 5. Lăsat neimplementat intenționat (POC)

- **Reasamblarea multi-part la ingress:** azi e pass-through transparent (esm_class
  + UDH păstrate în `attributes`, SMPP→SMPP intact). Reasamblarea reală (buffer +
  timeout + referințe de concatenare) e necesară doar dacă un capăt non-SMPP
  trebuie să vadă mesajul întreg.
- **Pipeline de DLR:** azi doar parsăm și logăm `deliver_sm` (cu detecția capcanei
  hex/zecimal). Rutarea DLR-urilor înapoi către client + stocarea lor e faza următoare.
- **UCP:** menționat de șef ca „de văzut", neimplementat deliberat. De evaluat ca
  cerință separată dacă apare un furnizor pe UCP.
- **Persistență / bază de date:** în afara scopului POC. Orice stare (dedup, DLR,
  rating) o va cere.

## 6. Ipoteze nevalidate (de închis în faza următoare)

1. **[CEA MAI IMPORTANTĂ] Scalarea liniară pe un broker care scalează.** Pe
   ElasticMQ (single-JVM) agregatul crește sub-liniar (1 proc 2.782/s, 2 procese
   4.342/s). Pe **AWS SQS real** (distribuit) contenția ar dispărea — dar e
   **nemăsurat**. Fără el nu putem afirma că platforma scalează la volume mari.
   Cost: ~30 min / <5 USD. Rulare: benchmark-urile fără `--endpoint-url`, cu
   credențiale (vezi RUNBOOK).
2. **RTT-ul SQS real vs localhost.** Debitul per proces per trecere va fi mai mic
   pe SQS real (RTT rețea > localhost). Cât — nemăsurat.
3. **Comportamentul la scară pe mașini separate / rețea reală.** Tot POC-ul a rulat
   pe un singur laptop, localhost.
4. **msgpack pe SQS real** (unde dimensiunea payload-ului contează și pentru cost):
   in-proces e mai mic/rapid, dar end-to-end pe ElasticMQ nu a contat. Pe SQS real,
   de re-verificat pentru cost de transfer.

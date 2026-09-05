# SMPP — cum funcționează codecul propriu

Document de preluare. Scris astfel încât un coleg care nu cunoaște SMPP să poată
prelua `src/relay/smpp/` și `src/relay/egress/smpp_connector.py`. Codecul e scris
de la zero (fără librărie externă) ca să înțelegem protocolul și să putem adapta
comportamentul per furnizor.

SMPP = Short Message Peer-to-Peer, v3.4. TCP, binar, big-endian. Un ESME (clientul
nostru, „External Short Message Entity") se leagă (bind) la un SMSC (furnizorul) și
trimite `submit_sm`. Noi implementăm și partea de **server** (M3), ca să primim
`submit_sm` de la clienți.

---

## 1. Structura unui PDU

Fiecare PDU are un **header de 16 octeți** urmat de un body opțional. Toate
întregurile sunt big-endian (network order).

```
 offset  câmp              lățime
 ┌──────┬─────────────────┬────────┐
 │  0   │ command_length  │ 4 (u32)│  lungimea TOTALĂ (header + body)
 │  4   │ command_id      │ 4 (u32)│  ce PDU e (submit_sm = 0x00000004)
 │  8   │ command_status  │ 4 (u32)│  cod de eroare pe răspunsuri (0 = OK)
 │ 12   │ sequence_number │ 4 (u32)│  corelează cererea cu răspunsul
 └──────┴─────────────────┴────────┘
```

**Bitul de răspuns:** `command_id` al unui răspuns = `command_id`-ul cererii
`| 0x80000000`. Ex: `submit_sm` = `0x00000004`, `submit_sm_resp` = `0x80000004`.
`generic_nack` = `0x80000000` (răspuns fără cerere).

Exemplu real — `enquire_link` cu `sequence_number = 1` (16 octeți, fără body):

```
00 00 00 10   command_length = 16
00 00 00 15   command_id = enquire_link
00 00 00 00   command_status = 0
00 00 00 01   sequence_number = 1
```

### Tipuri de câmpuri în body

| Tip | Format |
|---|---|
| Integer | lățime fixă, big-endian |
| C-Octet String | octeți terminați cu `00` (NUL) |
| Octet String | lungime fixă, dată de un câmp anterior (ex. `sm_length`) |
| TLV | `tag` (u16) + `length` (u16) + `value` (`length` octeți) |

### `submit_sm` — ordinea câmpurilor contează

Ordinea e fixă și trebuie respectată exact (aceeași pentru `deliver_sm`):

```
service_type (C-Octet)        source_addr_ton (u8)     source_addr_npi (u8)
source_addr (C-Octet)         dest_addr_ton (u8)       dest_addr_npi (u8)
destination_addr (C-Octet)    esm_class (u8)           protocol_id (u8)
priority_flag (u8)            schedule_delivery_time   validity_period (C-Octet)
registered_delivery (u8)      replace_if_present (u8)  data_coding (u8)
sm_default_msg_id (u8)        sm_length (u8)           short_message (sm_length octeți)
[ TLV-uri opționale ]
```

Vector de test scris de mână (vezi `tests/test_smpp_pdu.py`), `submit_sm` către
`12345` cu textul `hello`:

```
00 00 00 2b   command_length = 43
00 00 00 04   submit_sm
00 00 00 00   status
00 00 00 07   sequence_number
00            service_type ""
00 00         source ton/npi
00            source_addr ""
01 01         dest ton=international / npi=ISDN
31 32 33 34 35 00   destination_addr "12345\0"
00 00 00      esm_class / protocol_id / priority
00 00         schedule / validity ""
00 00 00 00   reg_delivery / replace / data_coding / sm_default
05            sm_length = 5
68 65 6c 6c 6f   "hello"
```

### Robustețe la decodare

Un furnizor prost implementat nu are voie să ne omoare conectorul. `pdu.decode`
ridică `PduError` (nu crash) pentru: PDU mai scurt decât header-ul,
`command_length` inconsistent cu lungimea reală, `command_id` necunoscut,
C-Octet String neterminat, TLV trunchiat. Sesiunea răspunde cu `generic_nack`
când framing-ul rămâne consistent (am citit exact `command_length` octeți) și
închide conexiunea când stream-ul se desincronizează.

---

## 2. Bind, sequence number și window

### Bind

Clientul deschide TCP, apoi trimite `bind_transmitter` (doar trimite),
`bind_receiver` (doar primește) sau `bind_transceiver` (ambele). Noi folosim
**transceiver**, ca să primim și DLR-urile pe aceeași conexiune. Body-ul bind:
`system_id`, `password`, `system_type`, `interface_version` (0x34 = 3.4),
`addr_ton`, `addr_npi`, `address_range`. Răspunsul poartă `system_id`-ul SMSC-ului
și `command_status` (0 = OK; altfel bind respins).

Mașina de stări (`session.py`):

```
OPEN ──bind──► BOUND_TX / BOUND_RX / BOUND_TRX ──unbind──► UNBOUND ──► CLOSED
```

### Sequence number

`sequence_number` pe 32 de biți, incremental, de la 1 la `0x7FFFFFFF`, apoi
**wraparound** înapoi la 1 (nu 0). Corelează fiecare cerere cu răspunsul ei.

### Window (fereastră de PDU-uri neconfirmate)

Nu trimitem un `submit_sm` și așteptăm răspunsul înainte de următorul — ar fi
lent. Trimitem până la N `submit_sm` „în zbor" simultan (window-ul), fiecare cu
sequence_number propriu, și corelăm răspunsurile pe măsură ce vin. Implementare:
o `dict[sequence_number → future]` plus un `asyncio.Semaphore(window_size)` care
mărginește câte cereri pot fi în zbor. Când răspunsul sosește, reader-ul rezolvă
future-ul acelui sequence_number și eliberează un slot.

### Timeout pe răspuns — status distinct

Dacă `submit_sm_resp` nu vine în N secunde, scoatem cererea din window și o
marcăm **`SUBMIT_TIMEOUT`** — un status **distinct**, nu un eșec. Furnizorul poate
să fi trimis mesajul și doar să fi pierdut răspunsul; a-l retrimite orbește ar
dubla mesajul. Conectorul tratează timeout ca „retry via redelivery" (nu șterge
din coadă, dar nici nu marchează livrat).

### enquire_link și reconectare

La fiecare 30s trimitem `enquire_link` (ping). Două rateuri consecutive → sesiunea
se închide, iar supervizorul din conector reconectează cu **backoff exponențial +
jitter** (≈1s → 60s, equal jitter). Astfel un furnizor care „îngheață" conexiunea
e detectat și înlocuit, nu ne blochează la infinit.

---

## 3. Encoding și segmentare (`encoding.py`)

### Alegerea alfabetului

- **GSM 03.38** (7-bit) dacă tot textul încape în alfabetul de bază + tabelul de
  extensie. Un mesaj = 160 caractere într-un segment.
- **UCS-2** (UTF-16BE) altfel. Un mesaj = 70 caractere într-un segment.

Detecția e automată: dacă `can_encode_gsm7(text)` → GSM7, altfel UCS-2.

**Diacriticele românești `ă â î ș ț` NU există în GSM 03.38** → orice text
românesc cu diacritice comută pe UCS-2. Consecință practică: un SMS în română
„încape" în doar 70 de caractere per segment, nu 160. Testat explicit.

### Tabelul de extensie — 2 poziții

Caracterele `| ^ € { } [ ] ~ \` nu au cod propriu în alfabetul de bază; se
codează ca `ESC (0x1B)` + un al doilea septet. Deci **ocupă 2 poziții** din cele
160. `€` singur = 2 septeți; 80 de `€` = 160 = un segment, 81 = două.

### Emoji — surrogate pairs

Caracterele din planurile suplimentare (emoji, ex. 😀 U+1F600) sunt în UTF-16
o **pereche surogat** = 2 code units = **2 poziții** din cele 70 UCS-2.

### Împachetarea 7-bit (GSM7)

Cei 7 biți per caracter se împachetează în octeți (8 septeți → 7 octeți), LSB
first. Vector verificat manual: `hello` = `E8 32 9B FD 06`. (Nota: `...06`, nu
`...46` cum se citează adesea greșit — ultimul octet conține doar cei 3 biți
superiori ai lui `o` plus 5 biți de umplere zero. Derivat pe hârtie în teste.)

### Segmentare cu UDH

Peste limită, mesajul se sparge în segmente concatenate. Fiecare segment poartă
un **UDH** (User Data Header) de 6 octeți la începutul `short_message`, iar
`esm_class` are bitul UDHI (`| 0x40`):

```
05 00 03 <ref> <total> <seq>
│  │  │   │      │       └ numărul segmentului (1-based)
│  │  │   │      └ numărul total de segmente
│  │  │   └ referință de concatenare (unică per destinație, modulo 256)
│  │  └ lungimea datelor IE = 3
│  └ IEI = 00 (concatenare pe 8 biți)
└ UDHL = 5 (lungimea header-ului fără acest octet)
```

Limite per segment concatenat: **153** pentru GSM7, **67** pentru UCS-2 (UDH-ul
consumă spațiu). Pentru GSM7 concatenat, datele împachetate sunt deplasate cu **1
bit de umplere** ca primul septet să înceapă pe o graniță de septet după UDH.

Segmentarea nu taie niciodată o pereche escape+cod (GSM7) sau o pereche surogat
(UCS-2) peste graniță — se face pe granițe de caracter.

---

## 4. Capcane întâlnite și cum le-am tratat

1. **`message_id` hex vs zecimal.** Cea mai frecventă capcană de integrare: unii
   furnizori returnează `message_id` în hex în `submit_sm_resp` și în zecimal în
   DLR (sau invers). Corelarea DLR→mesaj eșuează silențios. `smpp_sink` poate
   reproduce exact asta (`--submit-id-format hex --dlr-id-format dec`), iar
   conectorul numără corelările reușite/ratate (`dlr_correlated`/`dlr_missed`),
   testat explicit. **Tratare în POC:** doar detectăm și logăm; un sistem real ar
   normaliza formatul per furnizor din config.

2. **`command_length` inconsistent.** Nu ne încredem în el orbește: verificăm că
   e egal cu lungimea reală citită, altfel `PduError`.

3. **`hello` = `...06`, nu `...46`.** Vectorul „clasic" citat online e greșit
   pentru exact 5 septeți. Derivat pe hârtie, testat la nivel de octet.

4. **Timeout ≠ eșec.** Un `submit_sm_resp` pierdut nu înseamnă că mesajul n-a
   plecat. Status distinct `SUBMIT_TIMEOUT`, retry prin redelivery, nu retrimitere
   oarbă (ar dubla).

5. **Înfometarea window-ului.** Un furnizor lent care nu răspunde umple window-ul
   și oprește tot. Semaforul + timeout-ul pe răspuns eliberează sloturile.

6. **TPS pe cont, nu pe conexiune.** Limita contractuală e agregată pe furnizor.
   Token bucket-ul e **unul singur, partajat de toate binduri**, nu per bind.

---

## 5. Ce am implementat și ce am lăsat deoparte

**Implementat:** header + field codec + TLV, toate PDU-urile necesare, decodare
robustă, GSM7/UCS-2 cu extensie și segmentare UDH, împachetare 7-bit,
sesiune cu window/enquire_link/timeout/reconectare, client egress cu pool de
binduri și shaper agregat, clasificarea erorilor (temporar/permanent/fatal),
corelare `submit_sm_resp → message_id`, recepție și logare DLR.

**Lăsat deoparte (intenționat, POC):**
- **Reasamblarea mesajelor concatenate la recepție** (server ingress): fiecare
  `submit_sm` e tratat ca un segment independent; nu reasamblăm multi-part la
  intrare. De adăugat dacă e nevoie.
- **Pipeline de DLR**: doar logăm și confirmăm cu `deliver_sm_resp`. Fără rutare
  înapoi către client, fără stocare.
- **`query_sm`, `replace_sm`, `cancel_sm`, `submit_multi`, `data_sm`**: nefolosite
  de flux.
- **Ambiguitatea septet-count la data_coding=0**: `sm_length` numără octeți, nu
  septeți; un mesaj al cărui ultim octet are exact 7 biți liberi poate produce un
  `@` fantomă la decodare. Cunoscut, acceptabil pentru POC.
- **UCP**: menționat de șef ca „de văzut", nu cerință. Neimplementat.

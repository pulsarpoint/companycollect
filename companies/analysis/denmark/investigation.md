# Denmark — company open-data investigation

## Conclusion

Denmark is one of the easiest jurisdictions for open company data. A single publisher,
**Erhvervsstyrelsen (Danish Business Authority)**, exposes both base company data and full
financial statements through one Elasticsearch distribution at `distribution.virk.dk`.

- **Base company data** (`cvr-permanent`) — comprehensive, but behind **free HTTP Basic
  credentials** (request by email, sign a protected-data declaration). No payment.
- **Financial data** (`offentliggoerelser`) — **completely open, no auth**, with direct
  download of machine-readable **XBRL / Inline XBRL** annual reports.

Both were live-verified on 2026-06-13.

## What was found

### 1. CVR — Det Centrale Virksomhedsregister (base register)

The CVR is the single authoritative register of all Danish legal entities (active and
historical). Official system-to-system access is an Elasticsearch v1.7.x cluster:

- Base URL: `http://distribution.virk.dk/cvr-permanent` (HTTP only).
- Indexes and live counts:
  - `virksomhed` — **2,194,982** companies
  - `produktionsenhed` — **2,787,126** production units (P-numbers / establishments)
  - `deltager` — **1,772,344** participants (owners, board, persons & legal entities)
- Auth: **HTTP Basic**, credentials free from `cvrselvbetjening@erst.dk`. A declaration about
  handling address-protected persons must be signed.
- Querying: standard `/{index}/_search`, capped at **3,000 documents per query**; the
  **scroll API** is the supported way to extract the full register (there is no plain CSV dump).
- Record content: CVR number, full name history with validity periods, company form
  (virksomhedsform), status (NORMAL / bankruptcy / dissolved…), addresses, main + secondary
  industry codes (DB07/NACE), registered capital and purpose (attributter), employment figures,
  lifecycle dates, and participant/ownership relations (incl. beneficial owners — *reelle ejere*).

A live `match_all` query without credentials returned **HTTP 401**, confirming the auth gate.

### 2. Offentliggørelser / Regnskaber (financial statements) — OPEN

All Danish companies must file an annual report (årsrapport) with Erhvervsstyrelsen. Published
filings are distributed via a **second, open Elasticsearch index**:

- Endpoint: `http://distribution.virk.dk/offentliggoerelser/_search` — **no authentication**.
- Live total: **6,295,759** published filings.
- Each hit carries the CVR number, accounting period (`regnskabsperiode.startDato/slutDato`),
  publication timestamp, and a `dokumenter[]` array. Each document has a `dokumentType`
  (AARSRAPPORT, DELAARSRAPPORT, DELAARSRAPPORT_ESEF, ESEF_EXTENSION…), a MIME type, and a direct
  `dokumentUrl` on `regnskaber.virk.dk`.
- Documents are openly downloadable. Modern filings are **XBRL / Inline XBRL** (`application/xml`,
  `application/xhtml+xml`) built on the Danish **DCCA taxonomy** (`xbrl.dcca.dk/fsa` financial
  statements, `/gsd` general, `/cmn` common); listed groups add IFRS + ESEF. Pre-digital filings
  are PDF/TIFF images.
- The XBRL instances contain the **actual line-item figures** (income statement + balance sheet),
  so financial data is structured and extractable, not just a document link.

Verified end-to-end: queried Maersk (CVR 22756214) → latest = Q1 2026 interim with iXBRL + XBRL +
ESEF documents → downloaded one XBRL doc → decompressed (gzip) → valid Danish iXBRL instance.

### 3. Registreringstekster (registration texts)

Same distribution provides `registreringstekster` — the textual registration/change events per
CVR number. Same free-credential gate as `cvr-permanent`. Useful as an audit/history secondary source.

### 4. Catalog & third-party wrappers

- `datahub.virk.dk` / Virk Data is the open-data catalog confirming the publisher and the
  system-to-system route.
- `cvr.dev` (live cache, official Go/Python clients), `cvrapi.dk`, and `apicvr.dk` are convenience
  REST wrappers over CVR. Fine for single lookups/prototyping; for full ingestion use the official
  `distribution.virk.dk` source.

## What was not found / not pursued

- No official plain-CSV / single-file bulk dump of the whole CVR — the scroll API is the bulk path.
- No payment-walled official data encountered for base + financials (paid offerings exist for
  value-added/historical image services but are not required for open ingestion).

## Recommendation

Use the official Erhvervsstyrelsen distribution for both layers:

1. **Base:** request CVR credentials, scroll `virksomhed` + `produktionsenhed` + `deltager` for a
   full load; refresh incrementally on `sidstOpdateret`.
2. **Financials:** poll `offentliggoerelser` by `cvrNummer` (or incrementally on `sidstOpdateret`)
   to discover filings, then download the XBRL/iXBRL document and parse DCCA-taxonomy facts.

Registry keys: `denmark/cvr` (base) and `denmark/cvrregnskab` (financials). Attribution:
"Kilde: CVR / Erhvervsstyrelsen". Honour the `reklamebeskyttelse` flag for any marketing use.

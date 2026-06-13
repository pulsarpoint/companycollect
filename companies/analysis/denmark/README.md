# Company data sources for Denmark

## Status

- Official bulk/base data: **found** (CVR system-to-system Elasticsearch — free credentials by email, no payment)
- Official API: **found** (CVR-permanent Elasticsearch REST; HTTP Basic auth, free)
- Financial data: **found and OPEN** (Regnskaber / Offentliggørelser Elasticsearch — **no auth**, full XBRL/iXBRL/ESEF/PDF documents)
- Open data portal: **found** (datahub.virk.dk / Virk Data; Erhvervsstyrelsen is the publisher)
- License: **known** — CVR base data is free to reuse (incl. commercial) under CVR-loven; one caveat: *reklamebeskyttelse* (advertising protection) flag must be honoured for direct marketing
- Recommended ingestion path: **CVR scroll bulk (base companies + production units + participants) + Offentliggørelser API for financial filings + per-document XBRL download for figures**

## Best source

**Erhvervsstyrelsen (Danish Business Authority)** operates everything via the `distribution.virk.dk`
Elasticsearch distribution. Two complementary indexes cover the full requirement:

### 1. CVR-permanent — Det Centrale Virksomhedsregister (base company data)

`http://distribution.virk.dk/cvr-permanent` (HTTP only)

- Verified live record counts (index stats):
  - `/virksomhed` — **2,194,982** companies
  - `/produktionsenhed` — **2,787,126** production units (P-numbers / branches)
  - `/deltager` — **1,772,344** participants (owners, management, persons & entities)
- **HTTP Basic auth required** — credentials are **free**: email `cvrselvbetjening@erst.dk`,
  sign a declaration about handling of protected (address-protected) persons. No payment.
- Elasticsearch 1.7.x: standard `/{index}/_search`, **max 3,000 docs/query**, use the
  **scroll API** for full extraction (this is the official "bulk" path — there is no plain CSV dump).
- Rich record: `cvrNummer`, `navne` (historical with validity periods), `virksomhedsform`,
  `virksomhedsstatus`, `beliggenhedsadresse`, `hovedbranche` + bibrancher (NACE/DB07),
  `attributter` (registered capital, purpose), employment, lifecycle dates, relations.

### 2. Offentliggørelser / Regnskaber — digital annual reports (financial data)

`http://distribution.virk.dk/offentliggoerelser/_search` (HTTP only)

- **OPEN — no authentication.** Verified live: **6,295,759** published filings in the index.
- Query by `cvrNummer`; returns filing metadata + an array of `dokumenter`, each with a
  `dokumentType` (AARSRAPPORT, DELAARSRAPPORT, ESEF_EXTENSION…), `dokumentMimeType`, and a
  direct `dokumentUrl` on `regnskaber.virk.dk`.
- Documents are openly downloadable (verified): machine-readable **XBRL / Inline XBRL**
  (`application/xml`, `application/xhtml+xml`) using the Danish **DCCA taxonomy**
  (`xbrl.dcca.dk/fsa` financial statements, `/gsd` general, `/cmn` common) plus IFRS/ESEF
  for listed groups; older filings are PDF/TIFF images.
- The **XBRL files carry the actual figures** (income statement + balance sheet line items),
  so financial data is fully extractable, not just document links.

## Excluded / caveats

- CVR documents are served **gzip-compressed** even when `Content-Type: text/xml` — decompress on ingest.
- Address-protected persons (*reklamebeskyttelse*) must be honoured: such records may not be
  used for direct marketing and must be flagged when redistributed.
- Beneficial-ownership (*reelle ejere*) is part of CVR `deltager`/ownership data and is public,
  but person-level data carries GDPR/protection obligations.
- Third-party convenience APIs exist (cvr.dev, cvrapi.dk, apicvr.dk) — useful for quick single
  lookups, but for full ingestion go straight to the official `distribution.virk.dk` source.

## Next action

Request CVR credentials (`cvrselvbetjening@erst.dk`), then implement: (1) a scroll-based loader
for `virksomhed` + `produktionsenhed` + `deltager`; (2) an Offentliggørelser poller by `cvrNummer`
(or incremental on `sidstOpdateret`) to discover filings; (3) an XBRL downloader + DCCA-taxonomy
parser to extract figures. Map to the internal company model (see `schema_notes.md`). Add
attribution "Kilde: CVR / Erhvervsstyrelsen". Suggested registry keys: `denmark/cvr` (base) and
`denmark/cvrregnskab` (financials).

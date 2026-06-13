# Offentliggørelser / Regnskaber (filing metadata) Field Catalog

## Source Summary

- Country: Denmark
- Source type: official_registry_api (Elasticsearch 1.7.x)
- Organization: Erhvervsstyrelsen (Danish Business Authority)
- URL: http://distribution.virk.dk/offentliggoerelser
- License: Free / open; same CVR reuse terms
- Access: **public, no authentication** (live-verified HTTP 200)
- Freshness: near real-time (annual filing cycle); 6,295,759 filings total
- Record shape: Elasticsearch `hits.hits[]` with `_id` + `_source` filing metadata
- Primary keys: `_id` (filing URN)
- Join keys: `cvrNummer`

> **Basis: real observed records** from `raw/api/regnskab_offentliggoerelse_sample.json`
> (CVR 25313763, a 2008 TIFF filing) and `raw/api/regnskab_maersk_latest.json`
> (CVR 22756214, Maersk Q1-2026 interim with iXBRL/XBRL/ESEF). This source delivers
> **filing metadata + document URLs only** — the actual financial figures are inside the
> linked XBRL documents, cataloged separately under `cvr_regnskab_xbrl`.

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| `hits.hits[]._id` | _id | Filing URN (filing primary key) | string | identifier | urn:ofk:oid:39194942 | Use over `_index` |
| `…_source.cvrNummer` | cvrNummer | Company CVR (join key) | integer | identifier | 22756214 | Join to virksomhed + XBRL |
| `…sagsNummer` | sagsNummer | Case/reference number | string | metadata | 09-63.378, X26-CA-87-GK | Reference only |
| `…regNummer` | regNummer | Legacy reg number (null) | string | identifier | — | Ignore unless set |
| `…regnskab.regnskabsperiode.startDato` | startDato | Accounting period start | date | date | 2026-01-01 | Pair w/ slutDato |
| `…regnskab.regnskabsperiode.slutDato` | slutDato | Accounting period end | date | date | 2026-03-31 | Orders filings |
| `…regnskab.godkendelse` | godkendelse | Approval block (null observed) | object | metadata | — | Optional |
| `…offentliggoerelsestype` | offentliggoerelsestype | Publication type | string | filing | regnskab | Filter to accounts |
| `…omgoerelse` | omgoerelse | Correction/re-publication flag | boolean | metadata | false | true supersedes prior |
| `…offentliggoerelsesTidspunkt` | offentliggoerelsesTidspunkt | Published-at | datetime | date | 2026-05-07T12:30:46.231Z | Sort desc for latest |
| `…sidstOpdateret` | sidstOpdateret | Last-updated | datetime | date | 2026-05-07T12:30:46.930Z | Incremental high-water mark |
| `…indlaesningsTidspunkt` | indlaesningsTidspunkt | System ingest time | datetime | metadata | 2018-04-04T13:40:50.047Z | Provenance only, not the FY |
| `…dokumenter[]` | dokumenter | Documents (type, mime, url) | array | document | AARSRAPPORT/tiff, DELAARSRAPPORT/xml | Download + decompress (gzip) |

## Interpretation Notes

- **Two-step financials.** This index is a *discovery* layer: query by `cvrNummer`
  (or range on `sidstOpdateret` for full/incremental crawls), then download the document
  URLs. Figures are only in the documents.
- **Document types observed.** `AARSRAPPORT` (annual report), `DELAARSRAPPORT` (interim),
  `DELAARSRAPPORT_ESEF` (ESEF-tagged interim), `ESEF_EXTENSION` (zipped ESEF taxonomy
  extension for listed groups). MIME types observed: `image/tiff` (old scans),
  `application/xml` (XBRL), `application/xhtml+xml` (iXBRL), `application/zip` (ESEF).
- **Gzip gotcha.** Documents from `regnskaber.virk.dk` are **served gzip-compressed even
  when `Content-Type: text/xml`** — always attempt gzip-decode on download.
- **Document URLs** embed a base64-ish token path and are stable per document. Old filings
  link only to image/TIFF (no structured figures); modern ones (~2012+) link to XBRL/iXBRL.
- **Pagination.** Elasticsearch `_search` caps at 3,000 docs; use the scroll API for the
  full 6.3M-filing crawl. Per-company queries (`term` on `cvrNummer`) are small.
- **Latest filing.** Sort by `offentliggoerelsesTidspunkt` desc (as the Maersk sample does)
  to get the newest report per company.

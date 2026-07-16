# Norway — company open-data investigation

Date: 2026-06-13
Investigator: Claude Code (company-open-data-discovery skill)
Country: Norway (NO)
Languages: Norwegian (Bokmål/Nynorsk), English

## Conclusion (TL;DR)

Norway is one of the cleanest open-company-data jurisdictions in Europe. A single operator,
the **Brønnøysund Register Centre (Brønnøysundregistrene / Brreg)**, runs both the base
company register and the financial-accounts register, and publishes both as open data under
**NLOD 2.0** with no authentication.

Two services cover the full requirement:

1. **Enhetsregisteret** (Central Coordinating Register for Legal Entities) → base company data,
   bulk + REST API + incremental update feed.
2. **Regnskapsregisteret** (Register of Company Accounts) → financial statement figures,
   open JSON API keyed by organisation number.

The user's hypothesis — "Brreg has everything we need" — is confirmed for base data, and the
**additional financial data is also available from Brreg itself**, so no third-party aggregator
is required.

## What was found

### Enhetsregisteret (base register) — RECOMMENDED

- Base URL: `https://data.brreg.no/enhetsregisteret/api`
- API documentation: `https://data.brreg.no/enhetsregisteret/api/docs/index.html`
  and `https://data.brreg.no/enhetsregisteret/api/dokumentasjon/en/index.html`
- Endpoints verified live:
  - `GET /api/enheter` — search/list entities. `page.totalElements = 1,164,396`.
  - `GET /api/enheter/{orgnr}` — single entity (verified with Equinor 923609016).
  - `GET /api/underenheter` — sub-entities/establishments. `totalElements = 842,538`.
  - `GET /api/enheter/{orgnr}/roller` — roles (CEO, board, auditor) verified.
  - `GET /api/enheter/lastned`, `/lastned/csv`, `/lastned/regneark` — bulk JSON/CSV/XLSX.
  - `GET /api/underenheter/lastned/csv` — bulk sub-entities.
  - `GET /api/oppdateringer/{enheter|underenheter|roller}` — incremental change feed.
- No auth, no key, no registration. (A separate `autorisert-api` with Maskinporten exists for
  role data with national ID numbers — not needed for company data.)
- Format: JSON (HAL/`_links`), CSV (gzip), XLSX. API versioning via `Accept` header (v2).
- Bulk file sizes verified via HTTP HEAD:
  - `enheter` JSON.gz ≈ 197,618,678 bytes
  - `enheter` CSV.gz ≈ 153,742,105 bytes (downloaded; **1,458,299** data rows)
  - `underenheter` CSV.gz ≈ 59,969,635 bytes
- Note on the row-count gap: the API `/enheter` search returns 1,164,396 *currently registered*
  entities; the bulk CSV `enheter_alle` contains 1,458,299 rows because it also includes
  dissolved/deregistered entities retained in the register. Filter on status fields
  (`konkurs`, `underAvvikling`, etc.) as needed.

### Regnskapsregisteret (financial data) — RECOMMENDED (enrichment)

- Endpoint: `https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}`
- GitHub source/docs: `https://github.com/brreg/regnskapsregister-api`
- data.norge.no catalog entry:
  `https://data.norge.no/en/datasets/7c87f169-2520-4e56-ba2a-b7a3cc7de2e9/regnskapsregisteret`
- Verified live for Equinor (923609016) → JSON array of annual accounts with:
  - `regnskapsperiode` (period), `valuta` (currency), `oppstillingsplan`, `regnskapsregler`
  - `resultatregnskapResultat`: driftsinntekter, driftsresultat, finansresultat, årsresultat
  - `eiendeler`: sumEiendeler, omløpsmidler, anleggsmidler
  - `egenkapitalGjeld`: sumEgenkapital, sumGjeld, kort-/langsiktig gjeld
  - `revisjon` (audit), `regnkapsprinsipper` (small-company flag, accounting rules)
- One organisation number per request. Updated from Regnskapsregisteret XML import files.
- The unauthenticated endpoint deliberately returns only the latest approved `SELSKAP` filing.
  Although OpenAPI exposes `år` and `regnskapstype`, the public repository implementation ignores
  both unless the caller is an authorized partner. The restricted partner endpoint is limited
  to public authorities and returns at most the latest three years, including group accounts.
- Standard open key figures are available only for ordinary layouts; banks, insurers, and group
  accounts are excluded.
- A separate public API lists and downloads annual-report copies for the latest 15 years:
  `.../aarsregnskap/kopi/{orgnr}/aar` and `.../kopi/{orgnr}/{aar}`. Live checks returned valid
  PDFs for Equinor from 2011 through 2024. The checked PDFs contained no text layer, requiring
  OCR for extraction.
- Brreg's paid subscription delivers all registered annual accounts (about 300,000/year) as
  daily XML over SFTP, including auditor codes and optional TIFF copies. Current published price
  is NOK 480,000/year per subscriber with five subscribers. The product page describes an
  ongoing feed, not a guaranteed historical dump; historical initialization must be confirmed
  contractually with Brreg.

### data.norge.no (national open data portal) — useful secondary

- `https://data.norge.no` (Norwegian Digitalisation Agency, Digdir) is the national catalog
  (DCAT). Brreg datasets are catalogued here; it confirms publisher + NLOD 2.0 license but the
  actual data is served from `data.brreg.no`. Use as license/provenance reference.

### Other registers (noted, not primary)

- **Foretaksregisteret** (Register of Business Enterprises) — the legally constitutive register
  for commercial entities; its data is surfaced through Enhetsregisteret flags
  (`registrertIForetaksregisteret`). No separate open bulk needed.
- **Register over reelle rettighetshavere** (Beneficial Ownership Register) — live but access is
  controlled/limited; **not** a fully open bulk dataset. Out of scope for open ingestion.
- **MVA-registeret** (VAT register) — surfaced as a flag in Enhetsregisteret
  (`registrertIMvaregisteret`); VAT number is derivable as `NO{orgnr}MVA`.

## Why Brreg over aggregators

Third-party sites (proff.no, purehelp.no, regnskapstall.no, 1881, etc.) repackage exactly this
Brreg data, often behind ads/paywalls and with unclear redistribution terms. Going to the source
gives: better license clarity (NLOD 2.0), full coverage, daily freshness, and no rate-limit/ToS
risk. No reason to use an aggregator.

## Recommended ingestion approach

1. **Initial load**: pull `enheter` + `underenheter` bulk gzip (JSON or CSV). ~1.16M active
   entities + 0.84M sub-entities.
2. **Incremental**: poll `…/api/oppdateringer/enheter?dato=<last_seen>` daily for deltas
   (avoids re-downloading 200 MB).
3. **Latest financial enrichment**: call `…/regnskapsregisteret/regnskap/{orgnr}` only as a
   current-filing source and validation feed.
4. **Historical financials**: prefer the official XML subscription plus a negotiated initial
   historical delivery. If Brreg cannot provide backfill, use the public 15-year PDF archive
   with OCR only after a bounded feasibility test and written confirmation on acceptable bulk
   access, or procure a licensed historical dataset.
5. Map to internal model per `schema_notes.md`; keep `raw_record` for provenance.

## Open questions / risks

- Regnskapsregisteret open API is officially a "temporary/research" distribution — monitor for
  deprecation; the paid Subscription Service is the long-term guaranteed channel for figures.
- The open structured API has no historical depth: it returns one latest filing.
- The public PDF API has a rolling 15-year limit and verified files are image-only.
- The subscription page promises a daily XML feed but does not explicitly promise a retroactive
  dump; confirm backfill years, corrections/withdrawals, licensing, and redistribution before
  using it as the archive system of record.
- Rate limits are not formally published for either API — be polite, set a contact User-Agent,
  prefer the bulk file + update feed over crawling `/enheter` page-by-page.
- Beneficial ownership data is not openly bulk-available.

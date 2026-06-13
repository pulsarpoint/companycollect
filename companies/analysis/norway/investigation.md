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
- One organisation number per request. Updated ~5 days/week from Regnskapsregisteret SFTP
  drops (`yyyyMMddHHmmss-masse.xml`).
- Coverage: companies that file accounts (~80% of accounting-liable entities — AS, ASA, NUF,
  savings banks, etc.). Data from roughly 2018 onward in the open API; banks/insurance excluded
  from standard figures. Returns most-recent filed year(s); historical depth is limited in the
  open API.
- License on the open API distribution: **NLOD 2.0**. Full historical figures + scanned image
  copies (TIF/PDF) are behind the paid **Subscription Service** — not required for figures.

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
3. **Financial enrichment**: for each entity of interest (AS/ASA and other accounting-liable
   forms), call `…/regnskapsregisteret/regnskap/{orgnr}` and store the latest accounts. Throttle
   politely; cache by orgnr + last-filed-year (`sisteInnsendteAarsregnskap` from the base record
   tells you when new accounts exist, so you only re-fetch when it changes).
4. Map to internal model per `schema_notes.md`; keep `raw_record` for provenance.

## Open questions / risks

- Regnskapsregisteret open API is officially a "temporary/research" distribution — monitor for
  deprecation; the paid Subscription Service is the long-term guaranteed channel for figures.
- Historical financial depth in the open API is shallow (recent years). For multi-year history,
  the Subscription Service (fee) or accumulating snapshots over time is needed.
- Rate limits are not formally published for either API — be polite, set a contact User-Agent,
  prefer the bulk file + update feed over crawling `/enheter` page-by-page.
- Beneficial ownership data is not openly bulk-available.

# Norway — search attempts log

## Attempt 1

- Date/time: 2026-06-13
- Search engine or source: Direct knowledge → live verification of Brreg API docs
- Query: `https://data.brreg.no/enhetsregisteret/api/docs/index.html` (WebFetch)
- Language: English/Norwegian
- Why this query was tried: Brreg (Brønnøysundregistrene) is the known official Norwegian
  business register; confirm endpoints, bulk downloads, formats, auth, license.
- Top relevant URLs:
  - https://data.brreg.no/enhetsregisteret/api/docs/index.html
  - https://data.brreg.no/enhetsregisteret/api/dokumentasjon/en/index.html
- Result: Confirmed `/api/enheter`, `/api/underenheter`, `/api/.../roller`, bulk
  `/lastned`, `/lastned/csv`, `/lastned/regneark`, update feeds `/oppdateringer/*`.
  No auth on public endpoints; Maskinporten only on `autorisert-api`. License NLOD 2.0.
- Decision: Enhetsregisteret = primary base-data source.

## Attempt 2

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `Brreg Regnskapsregisteret API regnskap open data financial statements orgnr JSON`
- Language: English/Norwegian
- Why: User explicitly needs financial data; locate Brreg's accounts register API.
- Top relevant URLs:
  - https://github.com/brreg/regnskapsregister-api
  - https://github.com/brreg/regnskapsregister-api/blob/main/docs/for-devs.md
  - https://data.norge.no/en/datasets/7c87f169-2520-4e56-ba2a-b7a3cc7de2e9/regnskapsregisteret
  - https://data.brreg.no/regnskapsregisteret/regnskap/{orgNr}
- Result: Endpoint `https://data.brreg.no/regnskapsregisteret/regnskap/{orgnr}` returns full
  annual-accounts figures (income statement + balance sheet) as JSON. ~80% coverage of
  accounting-liable companies, data ~2018+, banks/insurance excluded. NLOD 2.0 on open API.
- Decision: Regnskapsregisteret = financial-data source (enrichment by orgnr).

## Attempt 3 — live API verification (curl)

- Date/time: 2026-06-13
- Source: direct HTTP to data.brreg.no
- Calls + results:
  - `GET /enhetsregisteret/api/enheter?size=1` → 200, `totalElements=1,164,396`
  - `GET /enhetsregisteret/api/enheter/923609016` (Equinor) → 200, full entity record
  - `GET /regnskapsregisteret/regnskap/923609016` → 200, full FY2024 accounts (USD)
  - `GET /enhetsregisteret/api/underenheter?size=1` → 200, `totalElements=842,538`
  - `GET /enhetsregisteret/api/enheter/923609016/roller` → 200, roles (Daglig leder/Styre/Revisor)
- Decision: All endpoints live and key-less. Saved raw samples to `data/norway/raw/api/`.

## Attempt 4 — bulk download HEAD + download

- Date/time: 2026-06-13
- Source: data.brreg.no bulk endpoints
- HEAD results:
  - `enheter/lastned` (JSON.gz) → 200, ~197.6 MB, `enheter_alle.json.gz`
  - `enheter/lastned/csv` (CSV.gz) → 200, ~153.7 MB
  - `underenheter/lastned/csv` (CSV.gz) → 200, ~60.0 MB
- Downloaded `enheter/lastned/csv` → verified gzip OK, **1,458,299** data rows, sha256 recorded.
- Decision: Bulk path works end-to-end. Saved to `data/norway/raw/bulk/`.

## Attempt 5 — license / catalog confirmation

- Date/time: 2026-06-13
- Source: WebFetch data.norge.no Regnskapsregisteret dataset page
- Result: License = **Norwegian Licence for Open Government Data (NLOD 2.0)** on open
  distributions; paid Subscription Service (XML/TIF) for full history + image copies;
  open API flagged as temporary/research distribution.
- Decision: Recorded in `license_notes.md`.

## Attempt 6 — historical annual-accounts verification

- Date/time: 2026-07-16
- Sources: live BRREG OpenAPI, official `brreg/regnskapsregister-api` repository, live report-copy
  API, and the official annual-accounts subscription page and agreement.
- Queries and probes:
  - `GET /regnskapsregisteret/regnskap/v3/api-docs/regnskapsregisteret`
  - `GET /regnskapsregisteret/regnskap/923609016?år=2018`
  - `GET /regnskapsregisteret/regnskap/923609016?regnskapstype=KONSERN`
  - `GET /regnskapsregisteret/regnskap/aarsregnskap/kopi/923609016/aar`
  - PDF downloads for Equinor reporting years 2011, 2018, and 2024
  - `https://www.brreg.no/en/use-of-data-from-the-bronnoysund-register-centre/subscription/subscription-to-annual-accounts/`
- Result:
  - Public key-figures calls always returned the latest 2024 `SELSKAP` filing; year and group
    filters were ignored.
  - BRREG source confirms public/default queries select the latest company filing. Year/type
    filtering is implemented only in the authorized partner branch, which is for public
    authorities and covers the latest three years.
  - The public report-copy API listed 2011–2024 for Equinor. Downloaded PDFs were valid but had
    no text layer in the first pages, so extraction requires OCR.
  - Official file-API documentation states a rolling 15-year availability window.
  - The paid subscription delivers all registered annual accounts as daily XML over SFTP,
    approximately 300,000 filings/year. It does not explicitly promise historical backfill.
- Decision: Do not use the current open JSON bootstrap as a historical archive. Prefer the paid
  XML feed with a negotiated historical delivery; use PDF/OCR only as a fallback after confirming
  acceptable bulk access and economics.

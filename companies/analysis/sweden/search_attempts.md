# Sweden — search attempts log

## Attempt 1

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `Bolagsverket Sweden company register open data API bulk download näringslivsregistret`
- Language: English/Swedish
- Why: Bolagsverket is the known official Swedish company register; check open-data/API/bulk status.
- Top relevant URLs:
  - https://bolagsverket.se/apierochoppnadata.2531.html
  - https://opencorporates.com/registers/249
  - https://en.wikipedia.org/wiki/Swedish_Companies_Registration_Office
- Result: Historically **paid** — XML bulk packet for a fee, API with ~SEK 6,250 onboarding + usage,
  only sparse CSV open data. Pointed to the APIer-och-öppna-data hub.
- Decision: Dig into whether the EU high-value-datasets rule changed this (it did).

## Attempt 2

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `Sweden SCB statistical business register företagsregister open data download companies`
- Language: English/Swedish
- Why: SCB Företagsdatabasen (FDB) is the statistical business register; check open access.
- Top relevant URLs:
  - https://www.scb.se/vara-tjanster/bestall-data-och-statistik/foretagsregistret/
  - https://www.scb.se/en/services/ordering-data-and-statistics/statistics-swedens-business-register/
- Result: **Government decision removed fees**; SCB now offers a **free API** for the business register
  under **CC0**. Register: ~1,804,297 companies, ~1,436,285 local units; weekly updates from Skatteverket.
- Decision: SCB free API = strong secondary/seed source.

## Attempt 3

- Date/time: 2026-06-13
- Search engine or source: WebSearch
- Query: `Sweden company financial statements annual accounts årsredovisning API Bolagsverket open data`
- Language: English/Swedish
- Why: User explicitly needs **financial data**.
- Top relevant URLs:
  - https://bolagsverket.se/apierochoppnadata/vardefulladatamangder/apiforvardefulladatamangder.5513.html
  - https://bolagsverket.se/en/foretag/aktiebolag/arsredovisningforaktiebolag.759.html
  - https://media.bolagsverket.se/diar/services/1.2/hamtaArsredovisningsinformation-1.2-en.html
- Result: Bolagsverket "valuable datasets" are **free** (EU directive), REST/JSON, with **digitally
  submitted annual reports in iXBRL** retrievable by document number (ZIP). iXBRL = the financial data.
- Decision: Bolagsverket Värdefulla datamängder API = primary source incl. financials.

## Attempt 4

- Date/time: 2026-06-13
- Source: WebSearch + WebFetch
- Query: `Bolagsverket "värdefulla datamängder" API developer portal swagger ... registrering`
  and `SCB företagsregistret öppna data API ... CC0 gratis`
- Result:
  - Bolagsverket VDM: free, **no contract**; register via *Kundanmälan* (email+phone) →
    `client_id`/`client_secret` for test+prod by email/SMS. JSON; annual reports iXBRL.
  - SCB free API: REST JSON/XML, **certificate** auth (API key from **Sept 2026**), **2,000 rows/req**,
    **10 req/10 s**, CC0; contact `scbforetag@scb.se`. Field docs: postbeskrivning-foretag/-arbetsstalle PDFs.
  - Launch date confirmed: **26 June 2025**.
- Decision: Capture base URLs and endpoints next.

## Attempt 5

- Date/time: 2026-06-13
- Source: WebSearch + WebFetch (community client `bolagsverket_ex` on hexdocs)
- Query: `Bolagsverket "vardefulla-datamangder" API base url gw.api.bolagsverket.se swagger token client_id`
- Result — official access surface:
  - Base URL: `https://gw.api.bolagsverket.se/vardefulla-datamangder/v1`
  - OAuth2 client_credentials, scope `vardefulla-datamangder:read`
  - Endpoints: `GET /isalive`, `POST /organisationer`, `POST /dokumentlista`, `GET /dokument/{id}` (ZIP/iXBRL)
- Note: `apiverket.se` exposes `/v1/companies/{orgnr}` etc. — that is a **third-party wrapper**, not the
  official endpoints. Recorded as a commercial aggregator.
- Decision: Verify endpoints live.

## Attempt 6 — live endpoint verification (curl, no credentials)

- Date/time: 2026-06-13
- Source: direct HTTPS to `gw.api.bolagsverket.se`
- Calls + results (saved to `data/sweden/raw/api/`):
  - `GET  /vardefulla-datamangder/v1/isalive`        → **HTTP 401** `900902 Missing Credentials` (WSO2; exists)
  - `POST /vardefulla-datamangder/v1/organisationer` → **HTTP 401** `900902 Missing Credentials` (exists)
  - `POST /oauth2/token` (no creds)                  → HTTP 404 (token path differs / issued at registration)
- Decision: Data plane confirmed live and OAuth-gated. Cannot pull authenticated samples without free
  registration; documented the registration path instead. Saved `_PROBE_NOTES.md`.

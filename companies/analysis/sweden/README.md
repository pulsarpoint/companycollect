# Company data sources for Sweden

## Status

- Official bulk data: **partial** — no single free full-register dump; SCB free API lets you
  pull the whole register in 2,000-row pages (CC0). Bolagsverket bulk XML packet is paid.
- Official API: **found** — two complementary **free** official APIs launched **26 June 2025**
  under the EU high-value-datasets rule (see below).
- Financial data: **found** — annual reports (årsredovisning) are delivered as **iXBRL** via the
  Bolagsverket *Värdefulla datamängder* API (`/dokumentlista` + `/dokument/{id}`), free of charge.
- Open data portal: **found** — `dataportal.se` (national DCAT catalog) lists both datasets.
- License: **known** — SCB business-register data is **CC0**; Bolagsverket high-value datasets are
  free/no-contract under the EU Open Data Directive (attribution-free in practice; confirm per dataset).
- Recommended ingestion path: **Bolagsverket *Värdefulla datamängder* API as the primary source**
  (company base data + annual-report documents in one OAuth2 API), with the **SCB free API** as a
  complementary source for workplace/establishment (arbetsställe) + employee-size + SNI coverage.

## Background — why Sweden changed in 2025

Sweden was historically a **paid-data** country: Bolagsverket sold company data (XML packet, ~SEK 6,250
onboarding + usage) and SCB charged for the business register. The EU Open Data Directive
(2019/1024) **high-value datasets** implementing regulation forced "company and company ownership"
data to be made available **free of charge via API and bulk**. On **26 June 2025** Bolagsverket and
SCB jointly launched *Värdefulla datamängder* ("valuable datasets"), making the core company data —
including digitally submitted annual reports — free. This is the single most important fact for Sweden.

## Best source

**Bolagsverket — *Värdefulla datamängder* (Valuable Datasets) API v1** — the official Swedish company
register operator's free high-value-dataset API. It also surfaces SCB data behind the same gateway.

- Base URL (verified live, WSO2 gateway, returns 401 *Missing Credentials* without a token):
  `https://gw.api.bolagsverket.se/vardefulla-datamangder/v1`
- Auth: **OAuth2 client_credentials**, scope `vardefulla-datamangder:read`. Credentials
  (`client_id`/`client_secret`) are issued **free, no contract** after a self-service *Kundanmälan*
  (email + phone; keys delivered by email/SMS for both test and production).
- Endpoints (from the official client library docs):
  - `GET  /isalive` — health check
  - `POST /organisationer` — company base data by organisationsnummer (name, address, legal form,
    status, SNI/industry codes, …); responses **JSON**.
  - `POST /dokumentlista` — list available documents (digitally submitted **annual reports**) for an org.
  - `GET  /dokument/{id}` — download a document; returns a **ZIP** containing the annual report in **iXBRL**.
- Financial data = the **iXBRL annual reports** (årsredovisning): income statement + balance sheet
  tagged against the Swedish K2/K3 taxonomies published at `taxonomier.se`. iXBRL is machine-parseable XBRL.

## Second source (complementary)

**SCB — Företagsdatabasen (FDB) / Företagsregistret free API** — Statistics Sweden's statistical
business register, **fee-free since 26 June 2025**, **CC0**.

- REST, **JSON or XML over HTTPS**. Access today via a **client certificate** (request from
  `scbforetag@scb.se`); SCB has announced an **API-key model from September 2026**.
- Limits: **max 2,000 rows per request**, **10 requests / 10 seconds** per user.
- Strengths the Bolagsverket API does not cover as well: **arbetsställen (local units/workplaces)**
  with **CFAR** workplace IDs, **employee size-classes**, and SNI down to workplace level.
- Register size (live figures): **~1,804,297 companies** and **~1,436,285 local units**; updated nightly/weekly.

## Excluded / caveats

- **No free single-file full dump.** Full-register access is via paged API pulls (SCB 2,000-row pages,
  or per-orgnr Bolagsverket calls). Bolagsverket's one-shot **bulk XML packet is a paid product**.
- **Financial depth.** The free annual reports cover companies that filed **digitally** (growing share,
  mandatory push ongoing); older paper-only filings won't appear as iXBRL. No pre-computed financial
  ratios — you parse the iXBRL yourself.
- **Beneficial ownership** (*verklig huvudman*) register exists at Bolagsverket but is **not** part of
  the free open API set; treated as out of scope.
- Many third-party APIs exist (allabolag, BolagsAPI/bolagsapi.se, apiverket.se, foretagsapi.se) — these
  are **commercial aggregators**, useful as fallbacks/comparison only, not primary official sources.

## Next action

1. Submit the Bolagsverket *Kundanmälan* to obtain free `client_id`/`client_secret` (test + prod).
2. Build an OAuth2 client-credentials loader: token → `POST /organisationer` per orgnr for base data →
   `POST /dokumentlista` → `GET /dokument/{id}` to pull iXBRL annual reports; parse iXBRL for financials.
3. Request the SCB certificate (`scbforetag@scb.se`) and page the full register (2,000 rows/call) for
   the company/workplace universe + employee-size + SNI, to seed the orgnr list to enrich via Bolagsverket.
4. Map fields to the internal company model (see `schema_notes.md`). Suggested registry keys:
   `sweden/bolagsverket_vdm` (base + financials) and `sweden/scb_fdb` (register/workplaces).

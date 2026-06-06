# Serbia — Company Open Data Investigation

Date: 2026-06-06
Country: Serbia (RS)
Languages searched: English, Serbian (Latin + Cyrillic)

## Summary

Serbia has an **official, free, public-domain open-data API** for company data,
operated by the **Agencija za privredne registre (APR / Serbian Business
Registers Agency)** — the authoritative national business registry. The data is
cataloged on the national open data portal `data.gov.rs` and served as full JSON
snapshots from `https://openapi.apr.gov.rs`. This is the recommended source and
the bulk data has been downloaded.

## What was found

### 1. APR OpenAPI (RECOMMENDED — official, public domain)

Three endpoints, each a full monthly snapshot (`DatumPreseka` = 2026-05-31 at
time of download), keyed by **matični broj** (8-digit registration number):

- **Companies** — `https://openapi.apr.gov.rs/api/opendata/companies`
  - 133,357 entities in status Активан / У стечају / У ликвидацији / У принудној ликвидацији.
  - Fields: PoslovnoIme, SifraOpstine, NazivOpstine, NazivStatus, DatumOsnivanja,
    NazivPravneForme, SifraDelatnosti.
  - Saved: `raw/api/apr_companies_full.json` (57.5 MB).
- **Financial statements** — `.../api/opendata/companies/financial-statements`
  - 123,455 entities, latest available annual RGFI.
  - Fields: GodinaFi, PoslovnoIme, SifraOpstine, NazivOpstine, PoslovnaImovina,
    Kapital, Gubitak, UkupniPrihodi, NetoDobitak, NetoGubitak, ProsecanBrojZaposlenih.
  - Saved: `raw/api/apr_financial_statements_full.json` (57.0 MB).
- **NGO** (associations/foundations/endowments) — `.../api/opendata/ngo`
  - 40,547 entities.
  - Fields: Naziv, SifraMesta, SifraDelatnosti, DatumOsnivanja, TipLica,
    OblastiOstvarivanjaCiljeva.
  - Saved: `raw/api/apr_ngo_full.json` (31.8 MB).

Catalog pages on `data.gov.rs` declare license = **public_domain** ("Јавни
подаци"), publisher = **Агенција за привредне регистре**, update frequency =
**once a month**. No authentication or API key; a single GET returns the whole
dataset.

### 2. APR automated data delivery web-service (paid / restricted — secondary)

APR also operates a real-time web-service ("Automatizovano izdavanje podataka")
covering **all** status registers, including **entrepreneurs/sole traders
(preduzetnici)** and the **Central Register of Beneficial Owners**. Free for
state bodies; **banks and other businesses pay a prescribed fee**. Contact:
`apr-podaci@apr.gov.rs`. This is the route for data that the open API omits
(notably preduzetnici and beneficial ownership).

### 3. Secondary / comparison sources

- **OpenCorporates** — mirrors the APR register (register id 224). Aggregator,
  not the primary source; useful for cross-checking and for an English UI.
- **Statistical Office of Serbia (data.stat.gov.rs / opendata.stat.gov.rs)** —
  has *aggregate* business-demography statistics (employees in legal entities,
  by activity/municipality), **not** an entity-level company list.
- Commercial aggregators (CompanyWall, businessdataguide, Infobel) — paid,
  not used.

## What was NOT found

- **No entrepreneurs (preduzetnici) open dataset.** APR registers sole traders,
  but they are absent from the open-data API; only companies (privredna društva)
  are published openly. Use the paid web-service for them.
- **No PIB (tax ID) / VAT number in the open data.** The open API exposes only
  the matični broj, not the PIB. PIB must come from another source if needed.
- **No CSV/XML bulk file** — the open data is JSON-only via the API.
- **`data.apr.gov.rs` / `opendata.apr.gov.rs` do not exist** (NXDOMAIN). An early
  web-search result claiming an "APR open data portal at data.apr.gov.rs with CSV
  bulk downloads" was a hallucination and was discarded after DNS verification.
- No beneficial-ownership data in the open API (paid web-service only).

## Recommendation

Use the **APR OpenAPI companies + financial-statements endpoints** as the primary
ingestion source. They are official, free, public-domain, country-complete for
legal entities, machine-readable, and refreshed monthly. Join the two on the
matični broj key. Treat NGO as an optional extra register. If sole traders,
PIB/VAT, or beneficial ownership are in scope, plan a separate paid integration
with APR's web-service.

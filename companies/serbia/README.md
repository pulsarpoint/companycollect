# Company data sources for Serbia (RS)

## Status

- Official bulk data: **found** (full JSON dumps via official APR OpenAPI)
- Official API: **found** (`https://openapi.apr.gov.rs/api/opendata/...`)
- Open data portal: **found** (datasets cataloged on `data.gov.rs`, published by APR)
- License: **known** — Public Domain / "Јавни подаци" (declared `public_domain` on data.gov.rs)
- Recommended ingestion path: **bulk (single JSON snapshot, refreshed monthly)**

## Best source

**Agencija za privredne registre (APR) — Serbian Business Registers Agency**, the
official national business registry. APR publishes three open-data JSON endpoints
through its OpenAPI host and catalogs them on the national open data portal
`data.gov.rs` with a `public_domain` license:

| Endpoint | Content | Records (2026-05-31) |
|---|---|---|
| `https://openapi.apr.gov.rs/api/opendata/companies` | Active/insolvency/liquidation **companies** (privredna društva) | 133,357 |
| `https://openapi.apr.gov.rs/api/opendata/companies/financial-statements` | Latest annual financial statements (RGFI) per company | 123,455 |
| `https://openapi.apr.gov.rs/api/opendata/ngo` | Associations, foundations, endowments | 40,547 |

Each endpoint returns a single JSON object: `DatumPreseka` (snapshot date) plus
`Podaci`, a map keyed by **matični broj** (8-digit registration number). Data is
refreshed **once a month**. Authentication is not required; a plain HTTP GET
returns the entire dataset (~57 MB for companies).

This is official, free, machine-readable, public-domain, and covers the whole
country — it is the recommended source.

## Next action

Schedule a monthly GET of the three endpoints, store each snapshot keyed by
`DatumPreseka`, and upsert into the company model (see `schema_notes.md`). Join
companies ↔ financial statements on the matični broj key. If sole traders
(preduzetnici) are required, they are **not** in the open data — pursue APR's
paid web-service (see `license_notes.md` / `investigation.md`).

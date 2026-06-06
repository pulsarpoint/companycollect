# OpenCorporates Field Catalog (PLANNING-ONLY / RESTRICTED)

> **Planning-only, restricted source.** Status: `blocked_by_license_uncertainty`. OpenCorporates aggregates all 50 state registers into one standardized schema, but bulk/commercial use **requires a paid license** and some data carries **share-alike** obligations. It was **not downloaded**; fields below come from the public OpenCorporates API schema only — no raw records or extracted values are stored. Use as comparison/fallback within free-tier terms; **do not bulk-ingest without a license.**

## Source Summary

- Country: United States
- Source type: third_party_aggregator
- Organization: OpenCorporates Ltd
- URL: https://api.opencorporates.com/
- License: restricted; bulk/commercial requires payment; some share-alike data
- Access: partial public API (free tier with key); bulk requires license
- Freshness: varies (mirrors underlying state registers)
- Record shape: standardized JSON company object across jurisdictions
- Primary keys: `company_number` + `jurisdiction_code`
- Join keys: `company_number`, `jurisdiction_code`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| results.company.company_number | company_number | Registry company number | string | identifier | Mirrors state entity id |
| results.company.jurisdiction_code | jurisdiction_code | Jurisdiction (e.g. us_co, us_de) | string | geography | Maps to a US state |
| results.company.name | name | Standardized name | string | legal_name | |
| results.company.incorporation_date | incorporation_date | Normalized incorporation date | date | date | |
| results.company.current_status | current_status | Normalized status | string | status | Common vocabulary across states |

## Interpretation Notes

- **Value proposition:** the single best *cross-state* normalized view — all 50 states + DC in one schema with a consistent identifier (`company_number` + `jurisdiction_code`), normalized status, and incorporation date. This solves the "no national register" problem in one source.
- **Why planning-only:** licensing. The free API tier is limited and bulk/commercial ingestion requires a paid agreement; some records are share-alike. Treat OpenCorporates strictly as a **comparison/fallback/deduplication aid**, not a primary free bulk source.
- **Not authoritative:** OC mirrors the underlying state registers. Where possible, prefer the authoritative state source (e.g. Colorado open data) and use OC only to fill gaps or to obtain a normalized status when raw state data is unavailable.
- No `sample_record.json` — restricted/license-uncertain source.

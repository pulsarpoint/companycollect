# APR Companies (Open Data API) Field Catalog

## Source Summary

- Country: Serbia
- Source type: official_registry
- Organization: Agencija za privredne registre (APR) / Serbian Business Registers Agency
- URL: https://openapi.apr.gov.rs/api/opendata/companies
- License: Serbian Open Data License (`sodl` / SODL_1_0)
- Access: public (plain GET, no auth)
- Freshness: monthly (DatumPreseka 2026-07-31 at latest retrieval)
- Record shape: JSON `{DatumPreseka, Podaci:{<maticni_broj>:{...}}}` — map keyed by matični broj
- Primary keys: `maticni_broj`
- Join keys: `maticni_broj`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Podaci.<mb> (key) | maticni_broj | Registration number | string | identifier | 21141666 | company id; join key |
| …PoslovnoIme | PoslovnoIme | Business name | string | legal_name | ENEKS MONT PLUS DOO KRUŠEVAC | Latin, Cyrillic, or mixed script |
| …SifraOpstine | SifraOpstine | Municipality code | string | geography | 70670 | RZS/APR code |
| …NazivOpstine | NazivOpstine | Municipality name | string | geography | КРУШЕВАЦ | Cyrillic |
| …NazivStatus | NazivStatus | Status | string | status | Активан | Cyrillic; map to enum |
| …DatumOsnivanja | DatumOsnivanja | Incorporation date | date | date | 2015-10-09 | ISO |
| …NazivPravneForme | NazivPravneForme | Legal form | string | legal_form | Друштво са ограниченом одговорношћу | Cyrillic |
| …SifraDelatnosti | SifraDelatnosti | Activity code | string | activity | 4322 | KD2010 ≈ NACE Rev.2 |

## Interpretation Notes

- **133,634 companies** (2026-07-31). The national open company snapshot
  (privredna društva), licensed under SODL, in a single GET (~58 MB).
- **Complete-snapshot profile**: all 133,634 records contain all seven fields as
  non-empty strings. Registration numbers are unique eight-digit strings; 17,775
  start with zero and must never be parsed as integers.
- **Script mix**: `PoslovnoIme` is not reliably Latin-only: 10,602 names contain
  at least one Cyrillic code point and some are mixed-script. `NazivOpstine`,
  `NazivStatus`, and `NazivPravneForme` are Cyrillic. Preserve source text and
  derive separate normalized/search values when needed.
- **Date range**: `DatumOsnivanja` spans 1918-12-22–2026-07-31, so use ClickHouse
  `Date32`, not `Date`.
- **What's missing** vs a full register: **no PIB/VAT**, **no street address**
  (only municipality), **no directors/shareholders**, **no beneficial owners**,
  and **no sole traders (preduzetnici)**. Those require the paid APR web service.
- Join to `apr_financial_statements` on the matični broj key for financials.
- `sample_record.json` is a real record (ENEKS MONT PLUS DOO, MB 21141666).
- Full statistics and proposed ClickHouse DDL are in
  `../../apr_companies_full_analysis_and_clickhouse.md`.

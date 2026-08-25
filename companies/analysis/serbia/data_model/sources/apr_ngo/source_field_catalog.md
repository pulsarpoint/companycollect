# APR NGO (Open Data API) Field Catalog

## Source Summary

- Country: Serbia
- Source type: official_registry
- Organization: Agencija za privredne registre (APR) — Registar udruženja, zadužbina i fondacija
- URL: https://openapi.apr.gov.rs/api/opendata/ngo
- License: Serbian Open Data License (`sodl`)
- Access: public (plain GET)
- Freshness: monthly (DatumPreseka 2026-05-31)
- Record shape: JSON `{DatumPreseka, Podaci:{<maticni_broj>:{...}}}`
- Primary keys: `maticni_broj`
- Join keys: `maticni_broj`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| Podaci.<mb> (key) | maticni_broj | Registration number | string | identifier | 28000332 | |
| …Naziv | Naziv | Name | string | legal_name | Братоношка… | |
| …SifraMesta | SifraMesta | Place code | string | geography | 791032 | place (not municipality) |
| …SifraDelatnosti | SifraDelatnosti | Activity code | string | activity | 9499 | KD2010 |
| …DatumOsnivanja | DatumOsnivanja | Founding date | date | date | 2009-10-25 | |
| …TipLica | TipLica | Entity type | string | legal_form | Удружење | association/foundation/endowment |
| …OblastiOstvarivanjaCiljeva[] | OblastiOstvarivanjaCiljeva | Goal areas | array | activity | [{Naziv…, Opis…}] | nested |

## Interpretation Notes

- **40,547 entities** (2026-05-31): associations (*udruženja*), foundations and
  endowments (*zadužbine i fondacije*), and foreign equivalents — **not commercial
  companies**, but the same `maticni_broj` id space, useful for full legal-entity
  coverage.
- **Field-name note**: the goal-area items use
  `NazivOblastiOstvarivanjaCiljeva` (area name) and
  `OpisOblastiOstvarivanjaCiljeva` (description) — longer than the abbreviated
  `Naziv`/`Opis` in the earlier notes; the catalog reflects the **observed** names.
- `SifraMesta` is a **place** code here (vs `SifraOpstine` municipality code in
  the companies feed).
- No financials for NGOs in the open data.
- `sample_record.json` is a real record (MB 28000332).

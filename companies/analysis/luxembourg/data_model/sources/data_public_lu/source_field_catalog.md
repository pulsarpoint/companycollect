# data.public.lu / STATEC — statistical enterprise data Field Catalog

## Source Summary

- Country: Luxembourg
- Source type: open_data_portal
- Organization: STATEC / data.public.lu
- URL: https://data.public.lu/ (uData API: /api/1/)
- License: CC0 / open (per dataset)
- Access: public
- Freshness: periodic
- Record shape: uData catalog (STATEC aggregate enterprise statistics)
- Primary keys: `dataset_id`
- Join keys: none

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| (dataset rows) | (per dataset) | Aggregate enterprise statistics | object | metadata | — | not company data |

## Interpretation Notes

- **Not the company register.** data.public.lu is the national open-data portal (uData), but for companies it
  only carries **STATEC statistical aggregates** (enterprise demography, structural business statistics,
  creations/cessations). Verified: `entreprises` = 71 datasets, all statistical; `RCS`/`registre commerce`/`LBR`/
  `société`/`TVA` = **0**. It does **not** expose the RCS register or financials. Included for completeness only.

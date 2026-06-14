# ARES — open data bulk export (otevřená data) Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: official_registry
- Organization: Ministerstvo financí ČR (Ministry of Finance)
- URL: https://ares.gov.cz/stranky/otevrena-data ; https://data.mf.gov.cz/topics/ares
- License: Open data (MF ČR; confirm exact terms)
- Access: public
- Freshness: periodic bulk refresh
- Record shape: bulk export of ARES economic subjects (same fields as the API)
- Primary keys: `ico`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ico | ico | Company id | string | identifier | — | join key |
| (ares_fields) | (same as ARES API) | Full ARES record set | object | raw_extension | — | see `ares_api` |

## Interpretation Notes

- The **full-population** alternative to per-IČO API calls: MF ČR publishes a **bulk export** of ARES economic
  subjects on its open-data portal. Field semantics are the same as the **ARES API** — see the `ares_api`
  catalog for the per-field detail. The portal is JS-rendered; **resolve the exact dataset/resource URLs via
  the portal** (`data.mf.gov.cz/topics/ares`). Use this for initial full loads, then the API for refresh.
- For the **deepest** register fields (officers, shareholders, share capital), prefer `justice_vr_bulk`; this
  source covers the ARES-side aggregated population.

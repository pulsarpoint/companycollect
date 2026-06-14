# data.gov.gr — national open data portal Field Catalog

## Source Summary

- Country: Greece
- Source type: open_data_portal
- Organization: Greek Government (Υπουργείο Ψηφιακής Διακυβέρνησης)
- URL: https://data.gov.gr/ (API: https://data.gov.gr/api/v1/, token-gated)
- License: open (per dataset)
- Access: public (free token)
- Freshness: varies
- Record shape: curated statistical datasets
- Primary keys: none
- Join keys: none

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| (dataset rows) | (per dataset) | Statistical datasets | object | metadata | — | not company data |

## Interpretation Notes

- **Not the company register.** data.gov.gr is a curated **statistical** open-data API (crime, traffic, health,
  economy, …), token-gated. Verified root reachable (HTTP 200). It does **not** expose GEMI/company bulk.
  Included for completeness/context only; no company fields.

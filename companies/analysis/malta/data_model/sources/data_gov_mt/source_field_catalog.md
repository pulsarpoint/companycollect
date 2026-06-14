# data.gov.mt — national open data portal Field Catalog

## Source Summary

- Country: Malta
- Source type: open_data_portal
- Organization: Government of Malta
- URL: https://data.gov.mt/
- License: per dataset
- Access: public (WAF-blocked to bots)
- Freshness: varies
- Record shape: custom portal (non-standard API)
- Primary keys: none
- Join keys: none

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| (datasets) | (per dataset) | Government open datasets | object | metadata | — | not company data |

## Interpretation Notes

- **Not the company register.** Verified: data.gov.mt returns **HTTP 403** (WAF) to non-browser clients and the
  standard CKAN/DKAN/uData API paths return **404** (non-standard custom portal). It does **not** publish the MBR
  company register or financials as open bulk. Included for completeness only; resolve any datasets via the
  portal UI.

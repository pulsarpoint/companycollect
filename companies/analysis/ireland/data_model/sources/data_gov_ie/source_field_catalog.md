# data.gov.ie — national open data portal Field Catalog

## Source Summary

- Country: Ireland
- Source type: open_data_portal
- Organization: Government of Ireland
- URL: https://data.gov.ie/dataset/companies (CKAN)
- License: CC-BY 4.0 (per dataset)
- Access: public
- Freshness: mirrors CRO
- Record shape: CKAN catalog mirroring the CRO datasets
- Primary keys: `dataset_id`
- Join keys: `company_num`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| result.resources[].url | resource url | Mirror of CRO resources | string | metadata | — | use opendata.cro.ie |

## Interpretation Notes

- **Mirror / discovery, not a separate source.** data.gov.ie harvests the CRO Open Data Portal datasets
  (Company Records + Financial Statements, CC-BY 4.0) and publishes them onward to data.europa.eu. The
  **authoritative source is opendata.cro.ie**; use data.gov.ie only for discovery or EU-wide federation.

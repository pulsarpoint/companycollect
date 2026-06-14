# data.gov.lv — national open data portal Field Catalog

## Source Summary

- Country: Latvia
- Source type: open_data_portal
- Organization: Vides aizsardzības un reģionālās attīstības ministrija (VARAM)
- URL: https://data.gov.lv/ (CKAN API: /dati/lv/api/3/action/)
- License: per dataset (UR datasets = CC0-1.0)
- Access: public
- Freshness: varies
- Record shape: CKAN catalog (org `ur` hosts ~35 datasets)
- Primary keys: `dataset_id`, `resource_id`
- Join keys: none (catalog)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| result.resources[].url | url | Resource download URL | string | metadata | — | resolve UR dataset URLs |
| result.license_id | license_id | Per-dataset licence | string | license_or_terms | CC0-1.0 | confirms CC0 |

## Interpretation Notes

- **The CKAN access point.** data.gov.lv hosts the UR datasets (org slug `ur`, ~35 datasets, **CC0-1.0**). Use
  the CKAN API (`package_show` / `resource`) to resolve and refresh the canonical resource URLs for the register,
  financials, beneficial owners, officers and members. The authoritative data is the UR; this is the portal/API
  layer.

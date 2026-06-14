# data.gov.hr (CKAN) — Field Catalog

> National open-data **catalog** (CKAN). Hosts the **Sudski registar** and **RGFI javna objava** datasets
> under the **Otvorena dozvola** (confirmed via `package_show`). A **discovery hub** — the actual data is in
> the registration-gated portals (sudreg-data.gov.hr; rgfi.fina.hr). Not a data source itself.

## Source Summary

- Country: Croatia
- Source type: open_data_portal
- Organization: Republic of Croatia (open data)
- URL: https://data.gov.hr/ ; CKAN API https://data.gov.hr/ckan/api/3/action
- License: Otvorena dozvola (per dataset)
- Access: public, no auth (CKAN API)
- Freshness: varies
- Record shape: CKAN datasets + resources
- Primary keys: dataset_id + resource_id
- Join keys: none (catalog)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| dataset.title | title | Dataset title | string | metadata | Sudski registar / RGFI |
| dataset.license_id | license_id | License | string | license_or_terms | Otvorena dozvola |
| resources[].url | url | Resource URL | string | document | → gated portals |
| resources[].format | format | Format | string | metadata | CSV/HTML |

## Interpretation Notes

- **Use it to discover + confirm licensing** (both core datasets are Otvorena dozvola), then fetch the actual
  data from `sudski_registar` (API) and `fina_rgfi` (CSV) — the CKAN resource links point to those
  registration-gated portals, not direct files.
- CKAN API is open (no auth); `package_show` works. No `sample_record.json` (catalog, not company data).

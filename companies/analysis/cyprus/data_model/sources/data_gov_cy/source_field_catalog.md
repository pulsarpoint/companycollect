# data.gov.cy — national open data portal Field Catalog

## Source Summary

- Country: Cyprus
- Source type: open_data_portal
- Organization: Republic of Cyprus (Deputy Ministry of Research, Innovation and Digital Policy)
- URL: https://www.data.gov.cy/ (DRCIP Registrar group: https://www.data.gov.cy/en/group/30)
- License: per dataset (open data)
- Access: public
- Freshness: varies per dataset
- Record shape: CKAN-like catalog of datasets/resources
- Primary keys: `dataset_id`, `resource_id`
- Join keys: none (discovery hub, not company data)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| result.datasets[].name | name | Dataset slug | string | metadata | (none) | locate DRCIP group #30 |
| result.resources[].url | url | Resource download URL | string | metadata | (none) | resolve DRCIP CSV here |
| result.resources[].format | format | Resource format | string | metadata | CSV | prefer CSV |
| result.datasets[].license_title | license_title | Per-dataset licence | string | license_or_terms | (none) | confirm before reuse |

## Interpretation Notes

- **Discovery hub, not a company master.** The portal is used to **resolve the DRCIP company CSV resource URL**
  and to **confirm the per-dataset licence**. It does not itself add company fields.
- **Non-standard CKAN path.** `/api/3/action/*` returned **HTTP 404** during discovery and pages are
  JS-rendered, so the resource URL was not resolved programmatically — resolve via the portal UI
  (`/en/group/30`).

# data.overheid.nl — national open data portal Field Catalog

## Source Summary

- Country: Netherlands
- Source type: open_data_portal
- Organization: Government of the Netherlands (DONL)
- URL: https://data.overheid.nl/ (CKAN: /data/api/3/action/)
- License: per dataset (KvK open datasets = CC-BY 4.0)
- Access: public
- Freshness: varies
- Record shape: CKAN catalog
- Primary keys: `dataset_id`, `resource_id`
- Join keys: none (catalog)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| result.resources[].url | url | Resource URL | string | metadata | — | KvK download/API URLs |
| result.license_id | license_id | Per-dataset licence | string | license_or_terms | CC-BY 4.0 | confirms CC-BY |

## Interpretation Notes

- **Discovery hub.** The national open-data catalog (CKAN) indexes the KvK open datasets
  (`kvk-handelsregister-open-dataset-basis-bedrijfsgegevens`, `…-jaarrekeningen`), both **CC-BY 4.0**, linking to
  the kvk.nl bulk downloads and the opendata.kvk.nl HVDS APIs. Use it to resolve/refresh the resource URLs and
  confirm licences. The authoritative downloads are on kvk.nl / opendata.kvk.nl.

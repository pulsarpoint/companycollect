# avaandmed.eesti.ee — national open data portal Field Catalog

## Source Summary

- Country: Estonia
- Source type: open_data_portal
- Organization: Republic of Estonia
- URL: https://avaandmed.eesti.ee/
- License: per dataset
- Access: public
- Freshness: varies
- Record shape: CKAN catalog of datasets/resources
- Primary keys: `dataset_id`
- Join keys: none (discovery hub)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| result.resources[].url | url | Resource download URL | string | metadata | — | resolve EMTA/secondary |
| result.license_title | license_title | Per-dataset licence | string | license_or_terms | — | confirm per dataset |

## Interpretation Notes

- **Discovery hub.** The national open-data catalog (CKAN) indexes the e-Business Register datasets and EMTA tax
  datasets. Use it to resolve secondary dataset URLs and licences. The **authoritative company source is
  avaandmed.ariregister.rik.ee directly** — this portal is for discovery, not the primary feed.

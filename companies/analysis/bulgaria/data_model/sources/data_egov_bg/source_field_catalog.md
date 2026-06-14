# data.egov.bg — Registry Agency Publications (CC-BY) — Field Catalog

> The **open (CC-BY)** path to Commercial Register data: the Registry Agency's **daily publications**
> (registered acts/changes), published on Bulgaria's national open-data portal. A **change/event stream**
> keyed on EIK — accumulate to build a master. WAF-blocked from automated access in this environment →
> resolve resource URLs via the portal UI / api_key; no sample pulled.

## Source Summary

- Country: Bulgaria
- Source type: open_data_portal
- Organization: State e-Government Agency / Агенция по вписванията
- URL: https://data.egov.bg/ (Търговски регистър dataset); API with api_key
- License: **CC-BY**
- Access: public (api_key for resource data; WAF-protected)
- Freshness: daily
- Record shape: dataset of CSV/XML/JSON resources (daily publications)
- Primary keys: `eik + publication_id`
- Join keys: `eik`

## Fields

| Path | Source field (BG) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| eik | ЕИК | Entity | string | identifier | join; accumulate per EIK |
| naimenovanie | наименование | Name | string | legal_name | Cyrillic |
| vid_vpisvane | вид вписване/обявяване | Act type | string | filing | incl. 'обявяване на ГФО' |
| data | дата | Date | date | date | daily stream |
| data_fields | changed attributes | Changed fields | object | raw_extension | latest-wins |
| resource_meta | resource metadata | Resource metadata | object | metadata | provenance |

## Interpretation Notes

- **Open bulk path** — the **CC-BY** daily publications are the open way to get Commercial Register data
  without a data-sharing agreement, but they are a **change stream**, not a single master snapshot: build &
  maintain the company master by **accumulating publications per EIK** (latest-wins per field).
- An act of type **"обявяване на ГФО"** signals that a **financial statement was filed** → a trigger to fetch
  the ГФО document (`gfo_financial_statements`).
- **Access**: the portal is WAF-protected and resource data may need an **api_key** — resolve the exact
  Registry Agency dataset + resource URLs from the portal before implementing. Cyrillic throughout.
- No `sample_record.json` (data.egov.bg returned HTTP 403 to automated access here).

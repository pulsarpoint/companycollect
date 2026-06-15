# dataportal.se — national open data catalog (DCAT) Field Catalog

## Source Summary

- Country: Sweden
- Source type: open_data_portal_catalog (DCAT)
- Organization: Agency for Digital Government (DIGG)
- URL: https://www.dataportal.se/datasets/612_5428
- License: catalog metadata; points to the source licenses (Bolagsverket VDM free-reuse; SCB CC0)
- Access: public
- Freshness: n/a (catalog)
- Record shape: DCAT dataset metadata — **not a company-data source**. Describes/links the Bolagsverket VDM and SCB datasets.
- Primary keys: dataset_id
- Join keys: none

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| dataset.title | title | Dataset title | string | metadata | — | Confirms dataset identity |
| dataset.publisher | publisher | Publishing org | string | metadata | Bolagsverket, SCB | Confirms official publishers |
| dataset.license | license | Formal license string | string | license_or_terms | — | **Record exact Bolagsverket VDM terms here** |
| dataset.distribution.accessURL | distribution accessURL | Endpoint the catalog points to | string | metadata | https://gw.api.bolagsverket.se/vardefulla-datamangder/v1 | Discovery only |

## Interpretation Notes

- **This is a catalog, not company data.** It yields no company records. It is documented only to
  capture publisher and license provenance and to resolve the open license-uncertainty for the
  Bolagsverket VDM datasets (the formal license string lives here).
- The only live response captured (`raw/api/dataportal_search.json`) is the error
  `{"error":"Mandatory parameter 'type' is missing"}` — the search endpoint
  (`admin.dataportal.se/store`) requires a `type` parameter, and the dataset page is a JS SPA.
- **Action:** read the exact license string for dataset `612_5428` from the SPA or store API and
  record it verbatim into `license_notes.md`.

No `sample_record.json` is provided: no usable catalog record was retrieved (the probe returned a
parameter error), and this source carries no company fields.

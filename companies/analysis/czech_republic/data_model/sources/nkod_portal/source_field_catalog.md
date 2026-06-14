# Národní katalog otevřených dat (NKOD) / data.gov.cz Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: open_data_portal
- Organization: Ministerstvo vnitra ČR / Digitální a informační agentura
- URL: https://data.gov.cz/ (DCAT-AP + SPARQL endpoint)
- License: per dataset
- Access: public
- Freshness: varies
- Record shape: DCAT-AP catalog of datasets/distributions
- Primary keys: `dataset_iri`
- Join keys: none (discovery hub)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| dataset.iri | dataset IRI | Catalog dataset id | string | metadata | — | locate ARES/Justice/RES |
| distribution.downloadURL | downloadURL | Distribution download URL | string | metadata | — | resolve bulk URLs |
| distribution.license | license | Per-distribution licence | string | license_or_terms | — | confirm reuse |

## Interpretation Notes

- **Discovery hub, not company data.** The national open-data catalog (DCAT-AP, with a SPARQL endpoint) indexes
  ARES, the Justice Veřejný rejstřík, ČSÚ RES and many other datasets. Use it to **resolve exact resource URLs
  and licences** — in particular to confirm the Justice VR licence (empty in the CKAN package) and the ARES
  bulk export URL.

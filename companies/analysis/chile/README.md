# Company data sources for Chile

## Status

- Official bulk data: **found** (Registro de Empresas y Sociedades CSV by year; SII bulk ZIP lists)
- Official API: **partial** (CKAN package API for metadata; bulk files for records)
- Open data portal: **found** (`datos.gob.cl`)
- License: **open-data portal terms; confirm per-resource reuse before redistribution**
- Recommended ingestion path: **bulk snapshots** from RES, enriched with SII legal-entity/company tax files

## Best source

The **Registro de Empresas y Sociedades** dataset on `datos.gob.cl` is the best
registry starting point. It publishes yearly CSV files for company incorporations,
including RUT, legal name, incorporation/registration dates, legal form code,
capital, commune, and region. The 2026 file is available through May 31, 2026.

For enrichment, the **Servicio de Impuestos Internos (SII)** publishes bulk ZIP
lists for legal entities, addresses, economic activities, company names, company
size/sales/worker bands, and composition of companies.

## Next action

Implement a bulk loader for the yearly RES CSVs first, then add SII ZIP files as
tax/activity/address enrichment keyed by Chilean RUT.

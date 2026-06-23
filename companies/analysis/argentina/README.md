# Company data sources for Argentina

## Status

- Official bulk data: **found** (Registro Nacional de Sociedades ZIP/CSV resources)
- Official API: **found** (`datos.gob.ar` CKAN API)
- Open data portal: **found**
- License: **CC BY 4.0**
- Recommended ingestion path: **bulk snapshot**

## Best source

The **Registro Nacional de Sociedades** from the Ministry of Justice is the best
registry source. Contrary to the initial quick impression, the catalog currently
contains fresh 2026 resources, including ZIPs and a CSV sample. The sample includes
CUIT, legal name, contract/incorporation timestamp, legal form, update timestamp,
fiscal/legal address fields, and activity fields.

## Next action

Use the CKAN API to enumerate yearly/semester ZIP resources, download the current
year ZIP, and profile all contained CSV files. Preserve CUIT as both registration
and tax id.

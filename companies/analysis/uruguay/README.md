# Company data sources for Uruguay

## Status

- Official bulk data: **partial** (industrial company directory and statistical/business datasets)
- Official API: **found** (CKAN API on `catalogodatos.gub.uy`)
- Open data portal: **found**
- License: **Uruguay open-data license**
- Recommended ingestion path: **partial/sector source**, not full registry

## Best source

The best entity-level source found is the **Directorio de Empresas Industriales
(DEI)** from the Ministry of Industry, Energy and Mining. It publishes CSV/XLSX/XML
files with RUT, legal name, trade name, company size, activities, CIIU code,
establishment address, public email/website/phone, and registration/expiration
dates.

This is useful but sector-scoped. It is not the national company registry.

## Next action

Keep Uruguay behind Argentina. Implement DEI only if partial industrial coverage
is useful, or continue searching for a full DGI/DGR-style public registry source.

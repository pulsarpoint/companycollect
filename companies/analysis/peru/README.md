# Company data sources for Peru

## Status

- Official bulk data: **found** (SUNAT Padron Reducido RUC ZIP)
- Official API: **not needed for first pass**; bulk ZIP is the main source
- Open data portal: partial; best source is direct SUNAT download page
- License: **public tax-register file; reuse terms need review**
- Recommended ingestion path: **bulk snapshot**

## Best source

**SUNAT Padron Reducido RUC** is the best source. The download page exposes:

- `padron_reducido_RUC.zip` - about 370 MB as of 2026-06-23, last modified
  2026-06-22.
- `padron_reducido_Local_Anexo.zip` - about 13 MB as of 2026-06-23.

The page describes a reduced RUC register suitable for identifying taxpayers and
companies.

## Next action

Implement a controlled bulk download for the SUNAT ZIP, then unzip/profile the
contained text file before mapping. The discovery run intentionally did not fetch
the 370 MB file.

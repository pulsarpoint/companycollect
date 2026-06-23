# Argentina investigation

## Conclusion

Argentina is stronger than expected and should move ahead of Uruguay and Jordan
in implementation priority. The national registry has current 2026 resources and
CC BY 4.0 metadata.

## Evidence

- CKAN package API returned `license_id: CC-BY-4.0`.
- Resources include `Registro Nacional de Sociedades - 2026`, 2026 semester ZIP,
  nonprofit association resources, and yearly ZIPs back to 2019.
- A CSV sample was downloaded and normalized.

## Recommended ingestion

Bulk snapshot by CKAN resource enumeration. Start with the sample CSV and current
year ZIP, then backfill prior yearly ZIPs if the schema is stable.

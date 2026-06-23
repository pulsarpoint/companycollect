# Company data sources for Colombia

## Status

- Official bulk/API data: **found** (`datos.gov.co` Socrata API/CSV)
- Official API: **found** (Socrata API for RUES-style registry extract)
- Open data portal: **found**
- License: **CC BY-SA 4.0** on the inspected dataset metadata
- Recommended ingestion path: **API snapshot / CSV export**

## Best source

The best registry source found is the `datos.gov.co` dataset **Personas Naturales,
Personas Juridicas y Entidades Sin Animo de Lucro** (`c82u-588k`). It exposes
registry fields such as chamber, registration number, legal name, NIT, CIIU,
registration/renewal/cancellation dates, legal organization, status, and legal
representative fields.

## Next action

Ingest from Socrata using `$limit`/`$offset` or CSV export, filtering/handling
natural-person merchants and representative fields as personal data.

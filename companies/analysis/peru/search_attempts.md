# Peru search attempts

## Attempt 1

- Date/time: 2026-06-23
- Source: SUNAT Padron Reducido page
- Query: direct SUNAT download page inspection
- Result: Found main RUC ZIP and local-anexo ZIP links.
- Decision: Recommended.

## Attempt 2

- Date/time: 2026-06-23
- Source: HTTP HEAD checks
- Query: `padron_reducido_ruc.zip`, `padron_reducido_local_anexo.zip`
- Result: Confirmed public ZIP downloads and sizes.
- Decision: Save page metadata only; defer full download to ingestion task.

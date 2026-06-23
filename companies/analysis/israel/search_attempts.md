# Israel search attempts

## Attempt 1

- Date/time: 2026-06-23
- Source: data.gov.il package search
- Query: Hebrew term for Companies Registrar
- Result: Found company, partnership, and change datasets.
- Decision: Recommended.

## Attempt 2

- Date/time: 2026-06-23
- Source: CKAN datastore API
- Query: `datastore_search` for `ica_companies` resource
- Result: Returned fields and sample records.
- Decision: Use datastore API.

## Attempt 3

- Date/time: 2026-06-23
- Source: direct CSV download
- Query: company CSV resource URL
- Result: Returned anti-bot/challenge HTML in this environment.
- Decision: Document as a direct-download caveat; API remains usable.

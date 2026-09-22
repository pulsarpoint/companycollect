# S3 results viewer validation — 2026-09-19

Backoffice now opens archived crawl results directly through the ClickHouse
`corpscout.website_crawl_results_s3` mapping. No new crawl, S3 mutation, or import
into the stored results table was needed for this validation.

## Mapping

Applied migration 426 with the normal migration tool; the live ledger is clean
at 426. Current attempt paths previously fell back to `0001` as the request ID.
The view now resolves both the request ID and nullable attempt number, while
preserving legacy paths and explicit payload IDs.

Validated object:

```text
crawls/company-crawls/backoffice-novelic-full-20260919-1789813689966/attempts/0001/result.json.gz
```

The filtered view returned domain `novelic.com`, request ID
`backoffice-novelic-full-20260919-1789813689966`, attempt 1, status `partial`, and
five documents. The crawl stopped at its configured page budget.

## Backoffice

Opened **S3 saved · View** from the Novelic attempt in the live local Backoffice.
Confirmed request identity, attempt, status, page count, model usage and stop
reason. Selected the contact page and inspected extracted contact observations,
simplified HTML source and page links. Saved HTML is rendered as escaped source.

The JSON download endpoint returned HTTP 200, an attachment with JSON content
type and `nosniff`, and 2,301,147 bytes. Its parsed JSON exactly matched the
original local result retained from the earlier JetStream validation.

## Automated checks

- Backoffice result/history tests: 14 passed, including exact path parameters,
  input rejection, safe errors, lossless JSON download and inert HTML handling.
- Backoffice typecheck and production build passed.
- ClickHouse mapping tests: 14 passed with the live connection enabled. Tests
  cover legacy/current path identity, explicit payload IDs, lossless sections,
  imports and direct/filtered S3 reads in an isolated temporary database.

The crawler runtime and browser service were unchanged for this feature.

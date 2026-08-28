# Sweden Ratsit fixed-company scan

## Source and execution boundary

`se_ratsit_scan_dispatch` renders the same 20 verified active Swedish companies
on every materialization. One headless CloakBrowser and one page are reused for
the batch. Request starts are separated by at least two seconds and the Dagster
pool `sweden_ratsit_browser` prevents concurrent browser runs. The asset has no
schedule; `se_ratsit_scan_dispatch_job` is launched manually while the pilot is
evaluated.

There is no separate coverage asset or scan-summary table. Company coverage and
completed scan summaries are derived from `se_company_ratsit` by
grouping its `scan_id`; Dagster owns run-level status and failures that occur
before any company result can be persisted.

## S3 objects and content deduplication

The Dagster run UUID is the scan ID. Results live under a directly addressable
per-company prefix in `source-sweden-ratsit`:

- changed success:
  `sweden_ratsit/pilot/company_id=<id>/<scan_id>_report.json`;
- navigation, HTTP, or parse failure:
  `sweden_ratsit/pilot/company_id=<id>/<scan_id>_error.json`;
- parse diagnostic, when rendered HTML exists:
  `sweden_ratsit/pilot/company_id=<id>/<scan_id>_diagnostic.html.gz`.

The success JSON envelope deliberately excludes the scan ID, fetch time, and
rendered-HTML hash. Its SHA-256 therefore changes only when the normalized report
or its stable source metadata changes. Before writing a success, dispatch looks
up all prior successful hashes for the company:

- a new hash writes the current `<scan_id>_report.json`;
- an identical hash writes no JSON object; the new ClickHouse result row points
  to the earlier object's exact bucket and key and sets `report_reused = 1`.

Failures are scan-specific and always write a new error JSON. Successful rendered
HTML is not retained. HTML is compressed only for parse failures, where it can
explain a selector or page-shape problem without another request.

## ClickHouse history

`corpscout.se_company_ratsit` has one row per
`(scan_id, company_id)`. Every successful or failed company result contains the
exact S3 bucket, object key, JSON SHA-256, byte size, fetch metadata, and optional
diagnostic key. Reused rows retain the new scan ID while pointing to the old S3
report object.

`outcome` is either `success` or `failure`. For failed rows, `failure_type`
identifies `navigation`, `http`, or `parse`; successful rows have an empty
`failure_type`. Consumers select the latest successful report directly from this
single table, so a newer failure does not hide the last usable S3 pointer.

## Parser output

The pure `lxml` parser extracts company identity, status, legal form, address,
industry, description, responsible people, workplaces, people at the address,
coordinates, and available company/consolidated financial periods. A page is
accepted only when it contains a non-empty company name and an organization
number matching the requested ID.

## Verification

- Contract tests: `tests/test_sweden_ratsit_pilot.py`.
- Migration contract: migration `000336` and `tests/test_clickhouse_migrations.py`.
- Definitions: `uv run dg check defs`.
- Runtime: materialize `se_ratsit_scan_dispatch_job`, then inspect its 20 rows in
  `corpscout.se_company_ratsit` by Dagster run ID.

# Sweden Ratsit fixed-company scan

## Source and execution boundary

`se_ratsit_scan_dispatch` renders the same 100 verified active Swedish companies
on every materialization. Four headless CloakBrowsers run concurrently: one uses
the server's direct connection and three use `crawl_proxy1`, `crawl_proxy2`, and
`crawl_proxy3` from the Dagster `.env`. Each browser and page is reused for its
25-company shard. The Dagster pool `sweden_ratsit_browser` prevents two Ratsit
asset runs from launching browser pools concurrently. The asset has no schedule;
`se_ratsit_scan_dispatch_job` is launched manually while the pilot is evaluated.

Companies are assigned deterministically by their position in the fixed list:
worker `i` receives positions `i, i + 4, i + 8, ...`. This round-robin sharding
keeps all four workers balanced without shared scheduling state and makes the
route for any company reproducible. Request starts remain at least two seconds
apart within each browser, so the four-worker pool can start up to four Ratsit
requests per two-second interval. That is an effective average interval of 0.5
seconds per request, four times faster than the original single-browser scan,
without reducing the two-second pause seen by any one network route.

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
- company not found on Ratsit:
  `sweden_ratsit/pilot/company_id=<id>/<scan_id>_not_found.json`;
- failure diagnostic, when rendered HTML exists for a parse failure or missing
  company redirect:
  `sweden_ratsit/pilot/company_id=<id>/<scan_id>_diagnostic.html.gz`.

The success JSON envelope deliberately excludes the scan ID, fetch time, and
rendered-HTML hash. Its SHA-256 therefore changes only when the normalized report
or its stable source metadata changes. Before writing a success, dispatch looks
up all prior successful hashes for the company:

- a new hash writes the current `<scan_id>_report.json`;
- an identical hash writes no JSON object; the new ClickHouse result row points
  to the earlier object's exact bucket and key and sets `report_reused = 1`.

Failures and not-found outcomes are scan-specific and always write a new JSON.
Successful rendered HTML is not retained. HTML is compressed for parse failures
and missing-company redirects, where it can explain the result without another
request.

## ClickHouse history

`corpscout.se_company_ratsit` has one row per
`(scan_id, company_id)`. Every company result contains the exact S3 bucket,
object key, JSON SHA-256, byte size, fetch metadata, and optional diagnostic key.
Reused rows retain the new scan ID while pointing to the old S3 report object.

`outcome` is `success`, `failure`, or `not_found`. For failed rows,
`failure_type` identifies `navigation`, `http`, or `parse`; success and
`not_found` rows have an empty `failure_type`. `connection_mode` identifies a
direct or proxied request, and `proxy_name` is empty for direct requests or
contains the safe logical worker name `crawl_proxy1`, `crawl_proxy2`, or
`crawl_proxy3`. Credential-bearing proxy URLs are never persisted. Consumers
select the latest successful report directly from this single table, so a newer
failure or not-found result does not hide the last usable S3 pointer.
Materialization metadata includes not-found, HTTP 429, and per-route 429 counts.

## Parser output

HTTP 404 responses and Ratsit's successful redirect to `/foretag?saknas` are
stored as the distinct `not_found` outcome. The not-found JSON records whether
the evidence was `http_not_found` or `ratsit_missing`. The redirect response
keeps compressed rendered HTML for diagnosis, without waiting for the normal
company-content selector.

The pure `lxml` parser extracts company identity, status, legal form, address,
industry, description, responsible people, workplaces, people at the address,
coordinates, and available company/consolidated financial periods. A page is
accepted only when it contains a non-empty company name and an organization
number matching the requested ID.

## Verification

- Contract tests: `tests/test_sweden_ratsit_pilot.py`.
- Migration contract: migrations `000336`, `000340`, `000341`, `000342`,
  `000343`, and `tests/test_clickhouse_migrations.py`.
- Definitions: `uv run dg check defs`.
- Runtime: materialize `se_ratsit_scan_dispatch_job`, then inspect its 100 rows in
  `corpscout.se_company_ratsit` by Dagster run ID.

The proposed downstream JSON-to-ClickHouse model is documented in
`sweden-ratsit-normalization-schema-proposal.md`.

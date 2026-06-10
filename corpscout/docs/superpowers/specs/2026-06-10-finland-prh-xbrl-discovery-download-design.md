# Finland PRH Financial XBRL Discovery Download Design

## Goal

Add the first production-shaped slice for freely available Finland financial
statements from PRH Open Data XBRL.

This phase only discovers and downloads raw statement XML for small explicit date
windows. It must give us real XML samples, file sizes, API failure behavior, and
resume/dedupe behavior before we design parsing, ClickHouse financial tables, or
company explorer revenue columns.

## Non-Goals

This phase does not:

- parse XBRL facts;
- create ClickHouse financial tables;
- map revenue, employee count, company value, or other metrics;
- extend the Finland company explorer cache;
- integrate Virre paid documents.

Virre remains a paid fallback source for statements outside PRH's free digital XBRL
coverage.

## Source Naming

Create a normal source entry in the universal source catalog and `data_sources`
table.

```text
name: finland_prh_xbrl
registry_key: finland/prh_xbrl
country_code: FI
display_name: Finland PRH financial XBRL
source_type: official_registry_api
source_url: https://avoindata.prh.fi/opendata-xbrl-api/v3
docs_url: https://avoindata.prh.fi/en
requires_authentication: false
requires_payment: false
license: CC-BY-4.0
```

The source name intentionally does not include `ytj`. YTJ is the company/basic
registry API. The financial statement API is a separate PRH Open Data XBRL API that
joins to companies by Finnish Business ID.

## PRH API Surface

The download workflow uses PRH's registration-date discovery endpoint:

```text
GET /all_financial_statements?registeredDateStart=<YYYY-MM-DD>&registeredDateEnd=<YYYY-MM-DD>&page=<N>
```

The response contains:

```text
totalResults
financials[]:
  businessId
  financialDate
  registrationDate
```

Raw statement XML is downloaded with:

```text
GET /financial?businessId=<business_id>&financialDate=<YYYY-MM-DD>
```

PRH's OpenAPI documents `429`, `500`, and `503` responses. The downloader must keep
per-statement failures in the artifact ledger so a later retry can resume without
re-downloading successful statements.

## Postgres Tracking

Keep source/action/file metadata in the existing universal tables:

- `data_sources`
- `data_source_actions`
- `data_source_files`
- `data_source_action_runs`
- `data_source_file_runs`

Add source-specific operational state under a domain schema:

```sql
CREATE SCHEMA IF NOT EXISTS financial_xbrl;
```

### `financial_xbrl.finland_prh_xbrl_discovery_windows`

Tracks scanned registration-date coverage and idempotency. It does not duplicate
Temporal workflow status.

```text
id
source_id                         -- FK to public.data_sources(id)
registered_date_start
registered_date_end
action_run_id                     -- FK to public.data_source_action_runs(id)
temporal_workflow_id
temporal_run_id
total_results
pages_discovered
statements_discovered
last_completed_page
completed_at
created_at
updated_at

unique(source_id, registered_date_start, registered_date_end)
```

Status interpretation:

- if `completed_at IS NOT NULL`, the date-window coverage completed;
- otherwise workflow status comes from Temporal and `data_source_action_runs`;
- workflow errors belong in `data_source_action_runs.error_message`, not this table.

`last_completed_page` allows a failed run to resume discovery paging if the next
implementation chooses to support page-level continuation. The first implementation
may re-run discovery from page 1 as long as statement artifact upserts are
idempotent.

### `financial_xbrl.finland_prh_xbrl_statement_artifacts`

Tracks each discovered statement XML artifact by stable source key.

```text
id
source_id                         -- FK to public.data_sources(id)
business_id
financial_date
registration_date
source_url
xml_path
xml_sha256
xml_size_bytes
download_status                   -- pending, downloading, succeeded, failed
attempts
last_attempt_at
downloaded_at
last_error_message
first_discovered_run_id           -- FK to public.data_source_action_runs(id)
latest_action_run_id              -- FK to public.data_source_action_runs(id)
created_at
updated_at

unique(source_id, business_id, financial_date)
```

`download_status` is intentionally kept in Postgres. Temporal can answer parent
workflow status, but this table needs durable per-statement skip/retry state across
many discovery windows and runs.

Allowed `download_status` values:

```text
pending
downloading
succeeded
failed
```

## Source Files

The source catalog should define at least one file for the action UI:

```text
file_key: statements_manifest
kind: source_manifest
relative_path: statements.ndjson
required: true
```

Raw XML files are many individual artifacts under the run directory. They are
tracked in `financial_xbrl.finland_prh_xbrl_statement_artifacts`, not as one
`data_source_files` row per statement.

Run directory layout:

```text
runs/<run-id>/
  statements.ndjson
  statements/<business-id>/<financial-date>.xml
```

`statements.ndjson` should contain one line per discovered statement:

```json
{
  "business_id": "0100130-4",
  "financial_date": "2024-12-31",
  "registration_date": "2025-04-15",
  "source_url": "https://avoindata.prh.fi/opendata-xbrl-api/v3/financial?businessId=0100130-4&financialDate=2024-12-31",
  "download_status": "succeeded",
  "xml_path": "runs/<run-id>/statements/0100130-4/2024-12-31.xml",
  "xml_sha256": "<sha256>",
  "xml_size_bytes": 123456
}
```

## Temporal Action

Use the existing company-source Temporal surface and source action model.

Action:

```text
pull_source
```

Workflow:

```text
CompanySourceDownloadWorkflow
```

For `finland_prh_xbrl`, the download action requires explicit date input for this
phase:

```json
{
  "registered_date_start": "2026-06-01",
  "registered_date_end": "2026-06-03",
  "max_statements": 50,
  "retry_failed": false
}
```

Input rules:

- `registered_date_start` and `registered_date_end` are required ISO dates.
- `registered_date_start <= registered_date_end`.
- A small window is expected for initial tests.
- `max_statements` caps XML downloads after discovery upserts. Discovery may still
  read enough pages to know total results for the requested window.
- if `retry_failed=false`, rows with `download_status = 'failed'` are left as-is;
  if `retry_failed=true`, failed rows are eligible for download again.

Workflow behavior:

1. Load source metadata for `finland_prh_xbrl`.
2. Upsert or load the discovery window for the requested registration-date range.
3. Page through `/all_financial_statements`.
4. Upsert statement artifact rows using `source_id + business_id + financial_date`.
5. Write `statements.ndjson` under the current run directory.
6. Select artifacts to download:
   - never download rows already `succeeded`;
   - download `pending` rows;
   - download `failed` rows only when `retry_failed=true`;
   - respect `max_statements`.
7. Mark a selected artifact `downloading`, increment `attempts`, and set
   `last_attempt_at`.
8. Download XML with `/financial`.
9. Write XML to `statements/<business-id>/<financial-date>.xml`.
10. Compute SHA-256 and byte size.
11. Mark artifact `succeeded` with path/hash/size/downloaded timestamp, or `failed`
    with a safe error message.
12. Update discovery coverage counters and set `completed_at` only after discovery
    paging completed.
13. Finish the existing `data_source_action_runs` row with Temporal/action status.

## API And UI Visibility

For the first implementation, reuse the existing source action UI. The action can be
triggered from the source Actions tab once `finland_prh_xbrl` appears in
`data_sources`.

The UI should show:

- the source row;
- the `pull_source` action;
- `statements_manifest` file status;
- action runs and Temporal status through the existing source action components.

Dedicated UI for listing every XML artifact is deferred until after the first
download test. The Postgres ledger tables should be queryable manually during the
spike.

## Error Handling

Lower-level HTTP and file operations wrap errors with context and return them.
Temporal activity/workflow boundaries log once through existing worker logging.
External error messages stored in Postgres should be safe summaries, not full stack
traces or request bodies.

PRH `429` should be treated as retryable by Temporal activity retry policy or by a
bounded backoff in the downloader. Permanent `400` responses for a specific statement
should mark only that artifact failed and allow the run to continue.

## Testing

Unit tests should cover:

- source catalog spec for `finland/prh_xbrl`;
- date input validation;
- discovery response decoding;
- statement artifact upsert/skip behavior;
- manifest line generation;
- XML path generation and SHA-256 metadata;
- failed artifact retry behavior;
- workflow wiring for `pull_source` on `finland_prh_xbrl`.

The first manual test should use a 2-5 day `registeredDateStart` /
`registeredDateEnd` window and a small `max_statements` value. Success is measured by
having real `statements.ndjson` and XML files plus populated Postgres ledger rows.

## Later Phases

After we inspect downloaded XML samples:

1. Design XBRL parsing and raw fact storage.
2. Add ClickHouse tables for generic facts and derived metrics.
3. Map practical metrics such as recent revenue, employee count when available,
   assets, equity, liabilities, and net income.
4. Add recent revenue columns to the Finland explorer cache.
5. Decide whether Virre paid document coverage is worth implementing.

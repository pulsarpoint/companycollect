# Sweden Greenhouse source design

## Scope and source contract

This package collects public Greenhouse Job Board API snapshots for reviewed
Swedish companies. The initial board is Mentimeter (`mentimeter`), linked to
`corpscout.se_companies.company_id = '5568925506'`. The public endpoint is
`GET https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs?content=true`.
No applicant or private recruiting data is requested.

The source asset must receive a valid JSON object with a `jobs` list. Any HTTP,
JSON, or board-contract failure aborts the complete run before a manifest is
written. A valid HTTP 200 response containing an empty list is a successful
snapshot and may close previously active jobs.

## Storage and grains

Raw responses are content-addressed in `source-sweden-greenhouse`. One manifest
records every successfully fetched board in the run. The source owns one DuckDB
file and the eight `se_greenhouse_*` ClickHouse tables created by migration
`000359`.

- Board grain: one row per Greenhouse board token.
- Company-link grain: one reviewed board/company pair.
- Snapshot grain: one board response object per successful run.
- Current grain: one active source job ID per board.
- Version grain: one content hash per source job ID.
- Event grain: one opened, updated, closed, or reopened observation.
- Location and compensation grains: source facts attached to one version.

Only locations recognizable as Swedish are published. Company linkage comes
only from the reviewed board link, never from employer-name fuzzy matching.

## Lifecycle and publishing

DuckDB performs set-based JSON normalization. ClickHouse publishing appends new
content versions and `first_seen`, `content_changed`, `closed_by_absence`, or
`reopened` lifecycle events by stable SHA-256 identifiers, then
atomically exchanges only the Greenhouse board, link, and current tables. An
absent job is closed only after every configured board fetched successfully.
Closed timestamps are observation-based and marked estimated.

The daily schedule is deliberately `STOPPED` until a production canary confirms
the migration, company link, RustFS access, row counts, and repeat-run behavior.

## Explicit non-goals

This package does not union, deduplicate, rank, or reconcile jobs with
Platsbanken or another ATS. It does not write `company_job_current`,
`company_job_history`, or `company_hiring_monthly`.

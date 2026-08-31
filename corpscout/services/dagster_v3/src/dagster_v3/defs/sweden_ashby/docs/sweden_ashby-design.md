# Sweden Ashby source design

## Scope and source contract

This package collects public Ashby job-board snapshots for reviewed Swedish
companies. The initial board is Lovable (`lovable`), linked to Lovable Labs
Sweden AB, `corpscout.se_companies.company_id = '5595061739'`. It calls
`GET https://api.ashbyhq.com/posting-api/job-board/{name}?includeCompensation=true`.

The response must contain a `jobs` list. Listed postings whose address country
is Sweden, or whose source location is recognizably Swedish, are retained. Any
fetch or contract failure aborts the run before its manifest is written.

Ashby's public endpoint exposes only currently published postings. Historical
backfills therefore use Common Crawl's CDX indexes and WARC records as a separate
archive source. `sweden_ashby_historical_jobs_s3` has one annual partition starting
with 2025, the first year with reviewed Lovable job-page captures. It scans every
crawl collection overlapping the selected year and retains canonical job-detail
pages; application forms and board-shell pages are excluded.

## Storage and grains

Raw JSON is content-addressed in `source-sweden-ashby`. The source has its own
DuckDB file and eight `se_ashby_*` ClickHouse tables. Board, company-link,
snapshot, current, version, event, location, and compensation tables never share
rows with another provider. Compensation is stored only when Ashby exposes a
public posting summary, without currency inference.

Historical WARC response records are independently content-addressed under
`historical/warc/`. The annual manifest preserves every CDX observation with its
crawl id, capture timestamp, original and canonical URL, content digest, record
id when the CDX row supplies one, and original WARC filename/range.
Content-addressing may reuse identical raw record objects, but the manifest does
not merge vacancies or observations.

The approved board link supplies `company_id`; job text is never used for fuzzy
company matching. DuckDB performs set-based normalization. Stable source-owned
hashes make history appends idempotent.

## Lifecycle and operations

Publishing derives `first_seen`, `content_changed`, estimated
`closed_by_absence`, and `reopened` events by comparing the complete successful
snapshot with Ashby's prior current table.
The daily schedule is `STOPPED` until migration, RustFS, link integrity, live row
counts, and repeat-run behavior are validated.

The historical job is manual and uses one Dagster run per year. Archive data is
not published into the live Ashby DuckDB or ClickHouse tables; normalization is a
separate future boundary so archive provenance cannot be confused with live API
lifecycle observations.

## Explicit non-goals

No Ashby job is merged with Platsbanken or another ATS, even when title,
description, company, and location are identical. Existing company hiring views
are outside this package.

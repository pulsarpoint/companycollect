# Denmark DataCVR company captures

## Source boundary

- DataCVR at `https://datacvr.virk.dk` is accessed through CloakBrowser because
  the JSON gateway requires a browser session.
- The raw asset includes companies only (`enhedstype="virksomhed"`). Persons and
  production units remain outside this source slice.
- Company status is not fixed. Results contain both active and ceased companies
  whose `startDato` falls inside the requested date range.
- DuckDB normalization and CVR-level deduplication are deferred to a downstream
  asset.

## Partitions and filters

- `denmark_cvr_backfill_s3` contains 138 calendar-month partitions from `2015-01`
  through `2026-06`, in the `Europe/Copenhagen` timezone. The end boundary is
  `2026-07-01`.
- `denmark_cvr_active_s3` contains daily partitions from `2026-07-01` onward in
  the same timezone. Each partition queries one exact registration date by using
  the partition date for both `startdatoFra` and `startdatoTil`.
- The daily asset captures newly registered companies by `startDato`. DataCVR's
  current search contract does not expose a record-update timestamp, so this is
  not a feed of later changes to already registered companies.
- A materialization first sends one count request covering its complete date
  range: a calendar month for backfill or one day for the active asset.
- When the generic count is at most 3,000, the same generic filter is downloaded.
- When it exceeds 3,000, the resource downloads a fixed list of 105 valid
  region/municipality pairs. `filters.py` stores the six DataCVR regions and all
  municipality code/name values as plain lists.
- Fixed filters are not generated or recursively refined during materialization.
  Zero-result filters are retained in query statistics because they contribute to
  proving which fixed filters were checked.

## Pagination and completeness

- Download requests use a fixed page size of 1,000 and page indices starting at
  zero.
- A query with at most 3,000 advertised results downloads every advertised page.
  If a fixed filter itself exceeds 3,000, the resource retains the accessible
  first 3,000 and marks the partition incomplete.
- A partition is complete only when the generic advertised count, the sum of the
  filtered advertised counts, and the merged entity count are equal, and every
  individual query is complete.
- Count mismatches do not fail the materialization. They produce an incomplete
  object, a warning log, and `is_complete=false` materialization metadata so the
  evidence remains available for later analysis.

## Immutable object storage

Each partition writes at most one merged JSON object in bucket
`source-denmark-cvr`. Object keys are stable across Dagster runs and deliberately
exclude `run_id`:

- complete:
  `denmark_cvr/backfill/month=<YYYY-MM>/companies.json`
- incomplete:
  `denmark_cvr/backfill/month=<YYYY-MM>/companies_incomplete.json`
- daily complete:
  `denmark_cvr/active/date=<YYYY-MM-DD>/companies.json`
- daily incomplete:
  `denmark_cvr/active/date=<YYYY-MM-DD>/companies_incomplete.json`

Before launching the browser, each asset checks both possible result keys. If
either already exists, the partition performs no download and no write, logs the
existing key, and materializes with `is_skipped=true`. Incomplete files are also
immutable evidence and therefore skip later retries.

The object contains partition and run metadata, generic/filtered/downloaded counts,
one audit entry per query, and a flat `enheder` list. Entity dictionaries retain
the source field names and values and are not deduplicated.

Schema-invalid source responses are stored separately as `.invalid.json`, and the
materialization fails without writing a result object. Logs never contain
response bodies, company fields, cookies, or browser state.

The source model accepts a nullable `virksomhedsform`, matching the DataCVR value
observed in the March 2015 Middelfart response. The raw `null` value remains
unchanged in the merged object.

## Operations and verification

- `BackfillPolicy.multi_run(max_partitions_per_run=1)` and the
  `denmark_cvr_search` pool serialize browser-heavy work.
- Materialization metadata reports completeness, query/page/file counts, entity
  counts, source bytes, stored bytes, the exact result key, and whether the
  partition was skipped.
- No schedule, job, DuckDB asset, or ClickHouse asset is registered in this slice.
- Validate with `uv run pytest tests/test_denmark_cvr.py
  tests/test_denmark_cvr_partitions.py -v`, `uv run ruff check`, and
  `uv run dg check defs`.

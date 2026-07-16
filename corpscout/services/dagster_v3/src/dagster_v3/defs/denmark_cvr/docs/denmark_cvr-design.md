# Denmark DataCVR monthly company source

## Source boundary

- DataCVR at `https://datacvr.virk.dk` is accessed through CloakBrowser because
  the JSON gateway requires a browser session.
- The raw asset includes companies only (`enhedstype="virksomhed"`). Persons and
  production units remain outside this source slice.
- Company status is not fixed. A monthly result contains both active and ceased
  companies whose `startDato` falls inside that calendar month.
- DuckDB normalization and CVR-level deduplication are deferred to a downstream
  asset.

## Monthly partitions and filters

- `denmark_cvr_search_results_s3` uses completed calendar-month partitions from
  `2015-01`, in the `Europe/Copenhagen` timezone.
- A materialization first sends one count request covering the first through last
  day of the month.
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
  first 3,000 and marks the month incomplete.
- The month is complete only when the generic advertised count, the sum of the
  filtered advertised counts, and the merged entity count are equal, and every
  individual query is complete.
- Count mismatches do not fail the materialization. They produce an incomplete
  object, a warning log, and `is_complete=false` materialization metadata so the
  evidence remains available for later analysis.

## Object storage

Each successful materialization writes exactly one merged JSON object in bucket
`source-denmark-cvr`:

- complete:
  `denmark_cvr/search/month=<YYYY-MM>/run_id=<run-id>/companies.json`
- incomplete:
  `denmark_cvr/search/month=<YYYY-MM>/run_id=<run-id>/companies_incomplete.json`

The object contains month and run metadata, generic/filtered/downloaded counts,
one audit entry per query, and a flat `enheder` list. Entity dictionaries retain
the source field names and values and are not deduplicated.

Schema-invalid source responses are stored separately as `.invalid.json`, and the
materialization fails without writing a monthly result object. Logs never contain
response bodies, company fields, cookies, or browser state.

## Operations and verification

- `BackfillPolicy.multi_run(max_partitions_per_run=1)` and the
  `denmark_cvr_search` pool serialize browser-heavy work.
- Materialization metadata reports completeness, query/page/file counts, entity
  counts, source bytes, stored bytes, and the exact result key.
- No schedule, job, DuckDB asset, or ClickHouse asset is registered in this slice.
- Validate with `uv run pytest tests/test_denmark_cvr.py
  tests/test_denmark_cvr_partitions.py -v`, `uv run ruff check`, and
  `uv run dg check defs`.

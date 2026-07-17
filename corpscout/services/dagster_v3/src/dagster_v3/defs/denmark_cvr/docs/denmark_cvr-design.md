# Denmark DataCVR entity captures

## Source boundary

- DataCVR at `https://datacvr.virk.dk` is accessed through CloakBrowser because
  the JSON gateway requires a browser session.
- The raw layer downloads companies (`enhedstype="virksomhed"`), production units
  (`enhedstype="produktionsenhed"`), and persons (`enhedstype="person"`) as
  independent assets and objects. A response is rejected if its totals or rows
  include any entity type other than the one requested by that asset.
- Company status is not fixed. Results contain both active and ceased companies
  whose `startDato` falls inside the requested date range.
- `denmark_cvr_companies_duckdb` normalizes both raw asset families into one
  persistent DuckDB file. ClickHouse export remains deferred.

## Partitions and filters

- Each entity type has one backfill and one active asset. The company assets retain
  their original names; production units and persons use
  `denmark_cvr_production_units_*_s3` and `denmark_cvr_persons_*_s3`.
- Every backfill asset contains 138 calendar-month partitions from `2015-01`
  through `2026-06`, in the `Europe/Copenhagen` timezone. The end boundary is
  `2026-07-01`.
- Every active asset contains daily partitions from `2026-07-01` onward in the
  same timezone. Each partition queries one exact registration date by using the
  partition date for both `startdatoFra` and `startdatoTil`.
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

Each entity asset writes at most one merged JSON object per partition in bucket
`source-denmark-cvr`. Object keys are stable across Dagster runs and deliberately
exclude `run_id`. Companies use `companies.json`, production units use
`production_units.json`, and persons use `persons.json`; incomplete captures add
`_incomplete` before `.json`:

- complete:
  `denmark_cvr/backfill/month=<YYYY-MM>/companies.json`
- incomplete:
  `denmark_cvr/backfill/month=<YYYY-MM>/companies_incomplete.json`
- daily complete:
  `denmark_cvr/active/date=<YYYY-MM-DD>/companies.json`
- daily incomplete:
  `denmark_cvr/active/date=<YYYY-MM-DD>/companies_incomplete.json`

The production-unit and person objects use the same directory layout with their
own filenames, so the three assets can be retried and materialized independently.

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

The source model accepts nullable `virksomhedsform` and `senesteNavn` company
fields, matching values observed in DataCVR responses. Raw `null` values remain
unchanged in the merged object.

## Incremental DuckDB normalization

`denmark_cvr_companies_duckdb` continues to depend only on the two company raw S3
assets and ignores the production-unit and person filenames. Normalization for
those entity types is outside this download-only change. The company asset writes
one local database at `data/denmark_cvr_source.duckdb`. The database contains:

- `denmark_cvr.companies`: one normalized row per CVR number, including every
  company response field, source object/partition/run audit columns, a payload
  hash, and the source entity as JSON in `raw_record`.
- `denmark_cvr.ingested_objects`: one state row per processed S3 result object,
  including completeness, source counts, hashes, sizes, and ingestion timestamps.

Every materialization lists result objects below both `denmark_cvr/backfill/` and
`denmark_cvr/active/`. Invalid-response evidence and unrelated JSON files are
ignored. Object keys already present in `ingested_objects` are not downloaded or
parsed, so after the initial load a daily update processes only newly materialized
date objects.

New rows are loaded through an explicit Arrow schema and upserted by CVR. Backfill
objects are processed before active objects, and later rows replace earlier rows
for the same CVR. All pending objects in a materialization are committed in one
transaction; a malformed stored object rolls back company rows and ingestion
state for the complete run. Validation errors identify only the object key and
schema issue locations, never response values.

## Operations and verification

- All six raw assets use `BackfillPolicy.multi_run(max_partitions_per_run=1)` and
  the shared `denmark_cvr_search` pool to serialize browser-heavy work.
- Materialization metadata reports completeness, query/page/file counts, entity
  counts, source bytes, stored bytes, the exact result key, and whether the
  partition was skipped.
- No schedule, job, or ClickHouse asset is registered in this slice.
- Validate with `uv run pytest tests/test_denmark_cvr.py
  tests/test_denmark_cvr_partitions.py tests/test_denmark_cvr_duckdb.py -v`,
  `uv run ruff check`, and
  `uv run dg check defs`.

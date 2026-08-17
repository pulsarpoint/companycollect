# Denmark DataCVR captures

## Source boundary

- DataCVR at `https://datacvr.virk.dk` is accessed through CloakBrowser because
  its JSON gateways require a browser session.
- Search assets download companies (`enhedstype="virksomhed"`). Responses are
  rejected when totals or rows contain a different entity type.
- Person search is intentionally not an asset. DataCVR returns zero persons when
  `startdatoFra`/`startdatoTil` are present, while the same request without dates
  advertises more than 1.8 million results and cannot be enumerated through the
  3,000-result search ceiling. Person IDs instead come from complete company
  details.
- Production units are not downloaded through date-filtered search. DataCVR
  returns empty production-unit results when the search request includes the
  `startdatoFra` and `startdatoTil` filters used by the smart list partitions.
  An independent production-unit branch instead selects company CVRs from
  DuckDB and extracts production units from the per-company response.
- Company-detail and production-unit capture assets use the HTTPS-only endpoint
  `/gateway/virksomhed/hentVirksomhed?cvrnummer=<CVR>&locale=en` through one
  reusable CloakBrowser session per materialized partition.

## Company search partitions

- Company backfills contain calendar-month partitions from `2015-01` through
  `2026-06` in `Europe/Copenhagen`. Active assets contain daily partitions from
  `2026-07-01` onward.
- Each active partition uses its date for both `startdatoFra` and `startdatoTil`.
  It therefore captures companies registered on that date. The DataCVR search
  contract does not expose a record-update timestamp, so this is not independently
  a change feed for companies registered earlier.
- A materialization first sends a count request for the complete partition range.
  Counts at or below 3,000 use the generic filter. Larger counts are split over a
  fixed list of valid region/municipality pairs.
- Download requests use pages of 1,000 rows. A partition is complete only when
  generic, filtered, and merged counts agree and every query is complete.
- Count mismatches preserve an incomplete raw object and report
  `is_complete=false` instead of discarding the available evidence.

## Search object storage and DuckDB

Search captures are immutable objects in `source-denmark-cvr`, for example:

- `denmark_cvr/backfill/month=<YYYY-MM>/companies.json`
- `denmark_cvr/active/date=<YYYY-MM-DD>/companies.json`

Incomplete objects add `_incomplete` before `.json`. Existing complete or
incomplete result objects cause a retry to skip the browser request. Invalid
responses are stored separately as `.invalid.json`; logs do not include response
bodies, company data, cookies, or browser state.

One non-partitioned asset normalizes these search objects into the shared
`data/denmark_cvr_source.duckdb` database:

- `denmark_cvr_companies_duckdb` upserts one row per CVR.
It uses an explicit Arrow schema, preserves the original row in `raw_record`,
and records processed object keys in `denmark_cvr.ingested_objects`. All pending
objects in one materialization are committed in one transaction.

## Company detail backfill

`denmark_cvr_company_details_s3` reads CVR numbers from
`denmark_cvr.companies`. It uses 128 static partitions named `bucket_000` through
`bucket_127`, with companies assigned by `md5_number_lower(cvr) % 128`. The same
hash is validated in Python before an object is written.

For legacy buckets, the asset lists existing keys once and opens one browser
session only when an original response is missing. `bucket_000` is the v2
object-catalog pilot: its first run probes only the deterministic per-CVR keys,
writes an immutable Parquet catalog, and replaces the exact partition
`commit.json` only after catalog verification. Later runs read that commit and
catalog directly and never enumerate the legacy prefix. Every successful
response produces:

- `denmark_cvr/company_details/bucket_NNN/cvr=<CVR>/company.json`, containing the
  original source response with Danish object keys and unchanged values.
- `denmark_cvr/company_details/bucket_NNN/cvr=<CVR>/company_en.json`, containing
  the same structure and values with keys changed through the versioned static
  Danish-to-English mapping.

Writes are checkpointed per company. The original object is written first. A
retry can repair a missing `_en` object from the existing original without making
another DataCVR request. Unknown source keys fail with their structural path so
the mapping can be reviewed without logging source values.

HTTP 429 and transient 5xx responses are attempted three times for the affected
CVR in the same browser session. The default exponential delays before attempts
two and three are 30 and 60 seconds; a bounded `Retry-After` header takes
precedence. Exhausted 429 rate limits still fail normally because they are not
company-specific.

An exhausted 500/502/503/504 response never fails the partition. The asset
immediately:

- writes
  `denmark_cvr/company_details/bucket_NNN/cvr=<CVR>/company_error.json`;
- records the CVR, HTTP status, request-attempt count, and UTC timestamp in
  `data/denmark_cvr_company_detail_failures.sqlite3`;
- logs the exhausted failure and `ignore_company` decision in
  `corpscout.dk_cvr_company_detail_failures`;
- continues downloading the remaining companies in the browser session.

The marker contains only safe audit fields, never the response body, cookies, or
browser state. A later successful response clears that CVR's unresolved local
failure history. Existing markers are terminal inputs for the static snapshot,
so later materializations do not repeatedly call a company endpoint already
classified as unavailable. Materialization metadata reports
`skipped_company_count`, `already_skipped_company_count`, and
`skipped_request_attempt_count`.

### Company-detail object-catalog and compaction pilot

The `bucket_000` catalog is stored under:

```text
v2/source=denmark_cvr/dataset=company_details/partition/hash_bucket=bucket_000/
```

It records one row per active original, English-key, or terminal failure JSON
object with CVR, kind, exact key, byte size, and SHA-256. The catalog is the
steady-state inventory contract; unchanged runs reuse its existing commit and
do not rewrite it. Other company-detail buckets and date-versioned updates keep
their legacy listing behavior until the pilot is accepted.

`denmark_cvr_company_details_compacted_s3` shares the upstream asset's 128-bucket
partition contract so Dagster can execute both assets in one run, but rejects
every partition except the `bucket_000` pilot. It receives that partition's
typed catalog reference through Dagster's IO manager, verifies every cataloged
JSON body, groups source objects toward a 256 MiB target, and writes
content-addressed Parquet shards plus a separate catalog under:

```text
v2/source=denmark_cvr/dataset=company_details_compacted/partition/hash_bucket=bucket_000/
```

The pilot retains all original JSON objects. When the source catalog SHA-256 is
unchanged, compaction reuses the existing commit without reading the JSON
objects again.

### Local company-detail smoke test

Run a deterministic 50-company sample from the local DuckDB company table with:

```bash
uv run python scripts/denmark_cvr_company_details_smoke_test.py
```

The command uses one browser session for the whole sample, performs no S3 or
DuckDB writes, reports all unmapped key paths it encounters, verifies that key
translation leaves JSON values unchanged, and checks that the translated object
can be serialized. Use repeated `--cvr <CVR>` options to test an explicit sample
when the local DuckDB database is unavailable.

## Daily company-detail refresh

`denmark_cvr_company_detail_updates_s3` uses the same daily partition definition
as the active company-list asset. For date `X`, it selects distinct CVRs from
`denmark_cvr.companies` where:

```sql
source_capture_type = 'active' and source_partition_key = X
```

The selected company details are downloaded through one browser session and
stored in a date-versioned namespace:

- `denmark_cvr/company_details/updates/date=<X>/bucket_NNN/cvr=<CVR>/company.json`
- `denmark_cvr/company_details/updates/date=<X>/bucket_NNN/cvr=<CVR>/company_en.json`
- `denmark_cvr/company_details/updates/date=<X>/bucket_NNN/cvr=<CVR>/company_error.json`
  for a company-specific server error that remains after three attempts.

Date-versioned keys keep refresh inputs reproducible and prevent an initial
snapshot from hiding a later refresh. With the current upstream search policy,
the daily set represents companies registered on `X`; if the company-list asset
is extended to capture other updates into the same audit columns, those CVRs flow
through without changing the detail assets.

## Person-ID catalog and person-detail backfill

The retired `denmark_cvr_persons_backfill_s3` and
`denmark_cvr_persons_active_s3` search assets are not registered because date
filters always produce zero person rows. Person details instead start only after
the complete company-detail snapshot:

1. `denmark_cvr_company_detail_person_ids_duckdb` depends on both
   `denmark_cvr_companies_duckdb` and `denmark_cvr_company_details_s3`.
2. Before replacing its catalog, it verifies that every expected company CVR has
   either both `company.json` and `company_en.json`, or an explicit
   `company_error.json` marker, in the correct one of all 128 company-detail
   buckets. Marked companies are counted and excluded from person-ID extraction.
3. It extracts IDs from
   `personkreds.personkredser[].personRoller[]` and
   `personkreds.ophoerteFad[]`, keeps only records whose `enhedstype` is
   `person` case-insensitively, and stores the required `id` plus `personType` in
   `denmark_cvr.person_ids`.
4. `denmark_cvr_person_details_s3` reads that DuckDB catalog through 128 stable
   `md5_number_lower(person_id) % 128` partitions.

One person bucket opens one reusable browser session. Each request uses the
HTTPS endpoint
`/gateway/person/hentPerson?identifikator=<ID>&persontype=<TYPE>&locale=en`.
HTTP 429 and transient 5xx responses use bounded `Retry-After`/exponential
backoff. Successful responses are checkpointed as:

- `denmark_cvr/person_details/bucket_NNN/enhedsnummer=<ID>/person.json`
- `denmark_cvr/person_details/bucket_NNN/enhedsnummer=<ID>/person_en.json`

The original object preserves Danish keys and values. The companion object uses
a versioned static English-key mapping and leaves every value unchanged.

An HTTP 404 for one person is treated as a terminal entity-level result rather
than a partition failure. The asset writes
`denmark_cvr/person_details/bucket_NNN/enhedsnummer=<ID>/person_error.json`,
counts the person as resolved, and continues through the bucket. The marker
contains safe request metadata but never the response body, cookies, or browser
state. Later materializations reuse the marker instead of requesting the same
missing person again. Rate-limit and server failures remain materialization
errors after their configured retries.

## Independent production-unit capture and normalization

Production-unit capture assets are peers of the company-detail assets. They
depend directly on `denmark_cvr_companies_duckdb`; neither production-unit S3
asset depends on a company-detail S3 asset.

- `denmark_cvr_production_units_s3` reads one of the same 128 stable CVR hash
  buckets from `denmark_cvr.companies` and writes one raw capture per company.
- `denmark_cvr_production_unit_updates_s3` uses the daily partition key to select
  company rows whose `source_capture_type='active'` and
  `source_partition_key=<date>`.
- Both assets open one browser session for the selected companies and store only
  the source `produktionsenheder` section plus capture metadata. They belong to
  the separate `denmark_cvr_production_units` Dagster group.

Static and daily objects use independent namespaces:

- `denmark_cvr/production_units/bucket_NNN/cvr=<CVR>/production_units.json`
- `denmark_cvr/production_units/updates/date=<X>/bucket_NNN/cvr=<CVR>/production_units.json`

The downstream `denmark_cvr_production_units_duckdb` and
`denmark_cvr_production_unit_updates_duckdb` assets consume only their matching
production-unit S3 assets. They extract both `aktiveProduktionsenheder` and
`ophoerteProduktionsenheder`, preserve each complete production-unit object in
`raw_record`, and store an explicit `company_cvr` parent key.

Each partition is atomic. All input objects are read and validated before DuckDB
is changed. Inside one transaction, existing production units for every parent
CVR in the partition are deleted and the current active and ceased units are
inserted. This parent-level replacement also removes production units that have
disappeared from a later company response while leaving unrelated companies
unchanged.

If an older `denmark_cvr.production_units` table from the retired search-based
loader exists, the first capture-driven materialization recreates it with the new
parent-aware schema.

## Operations and verification

- Search assets use `BackfillPolicy.multi_run(max_partitions_per_run=1)` and the
  shared `denmark_cvr_search` pool.
- Company-detail assets use one partition per run and the dedicated
  `denmark_cvr_company_details` pool, giving one browser session per partition.
- Person-detail downloads use one ID bucket per run and the dedicated
  `denmark_cvr_person_details` pool. The global per-pool limit serializes those
  browser sessions.
- Production-unit capture assets use one partition per run and their own
  `denmark_cvr_production_units` browser pool.
- All DuckDB writers use the `denmark_cvr_duckdb` single-writer pool.
- Production-unit search assets are intentionally not registered.
- No schedule, job, or ClickHouse asset is registered in this source slice.
- Validate with `uv run pytest -q`, `uv run ruff check`, and
  `uv run dg check defs`.

# Sweden JobTech Links jobs design doc

## 1. Source overview

- **Country / source**: Sweden — JobTech Links
- **Module**: `defs/sweden_jobtech_links/`
- **Catalog**: `https://data.jobtechdev.se/annonser/jobtechlinks/index.html`
- **Format**: one dated `YYYY-MM-DD.tar.gz` archive containing newline-delimited
  JSON in `output.json`
- **Authentication**: none
- **Raw object bucket**: `source-sweden-jobtech-links`

JobTech Links aggregates job advertisements linked from Swedish job sites. All
published rows remain byte-for-byte in the S3 archive. The normalized JobTech
Links dataset contains external publishers only because rows whose canonical
provider is `arbetsformedlingen.se` belong to the separate Platsbanken pipeline.
No deduplication is performed between the remaining external publishers.

## 2. Ingest mode — and why

The raw source boundary uses three assets with fixed partition definitions:

- `sweden_jobtech_links_historical_snapshot_s3` has static `2021` through
  `2025` partitions for the historical backfill;
- `sweden_jobtech_links_2026_month_snapshot_s3` has fixed monthly `2026-01`
  through `2026-08` partitions for the recent catch-up; and
- `sweden_jobtech_links_daily_snapshot_s3` has daily `YYYY-MM-DD` partitions
  from 2026-09-01 onward.

JobTech publishes daily archives, but running almost 1,900 historical Dagster
partitions would add orchestration overhead without changing the raw evidence.
The coarse historical partitions therefore batch source files while every
archive still receives its own date-and-SHA-256 object key. Separate fixed
definitions avoid dynamic partition registration and keep the operational
daily range independent from one-time backfill ranges.

## 3. Loading

Each S3 asset resolves its exact fixed partition window against the live catalog
and processes matching archives sequentially. Each archive is streamed to a
temporary file with dlt HTTP retries plus a whole-file retry loop. The loader
validates `Content-Length`, computes SHA-256 while downloading, and verifies
that the tarball contains exactly one non-empty, safe `output.json` member
before upload. Only one archive occupies temporary disk at a time.

The archive is preserved byte-for-byte at:

`snapshots/snapshot_date=<date>/sha256=<digest>/<date>.tar.gz`

An immutable `metadata.json` beside it records the source URL, archive headers,
member path and size, hash, first retrieval time, and originating Dagster run.
Each materialization also writes a run-specific partition manifest containing
the exact archive catalog for downstream replay. A month is marked complete
only when materialized after its end boundary. Retrying a closed month with a
complete manifest and all referenced S3 objects returns that manifest without
an HTTP request or a new S3 write. Current-month retries still read the catalog,
reuse already stored archives, and download newly published dates. Other
partition retries reuse complete stored archives unless `refresh_existing` is
set.

## 4. Transform

Each partition family has a raw and normalized DuckDB asset after its S3 asset.
The partition key determines an isolated file at:

`data/sweden_jobtech_links/duckdb/partition_key=<key>/data.duckdb`

The raw asset resolves the newest valid partition manifest and handles one S3
archive at a time, so neither compressed nor extracted archives accumulate on
temporary disk. DuckDB's native newline-delimited JSON reader records total,
Platsbanken, external-row, and external-provider counts per dated snapshot. It
persists only external-provider payloads in `job_ads_raw_external`; the original
archive remains the complete replay boundary in S3.

The normalized asset uses set-based DuckDB SQL to create:

- `snapshots`, one audit row per dated source archive;
- `job_ad_versions`, one row per stable publisher identity and serving-content
  version;
- `job_ad_observations`, one compact daily presence row per advertisement;
- `job_ad_location_versions`, one row per versioned workplace location; and
- `job_ad_enrichment_versions`, containing only JobTech's accepted binary
  occupation, competency, trait, and geography enrichments.

Publisher plus publisher-owned identifier defines the stable advertisement
identity. Observation and ingestion timestamps are excluded from the content
version hash, so an unchanged ad observed on consecutive dates reuses the same
version. Active intervals and current-state resolution need observations across
partitions and therefore belong in the later ClickHouse stage. Company matching
remains a separate enrichment stage.

## 5. ClickHouse schema

Migration `000363_corpscout_se_jobtech_links_jobs` owns the snapshot, job
version, observation, location, enrichment, active-interval, current-job, and
exact company-match tables. The DuckDB assets implement the first five shapes
but do not write ClickHouse yet.

## 6. Translation

Not applicable at the raw boundary. Source text remains unchanged.

## 7. Currency

Not applicable at the raw boundary. Any later compensation normalization must
retain the source value and currency evidence.

## 8. Scheduling

`sweden_jobtech_links_daily_catalog_sensor` is registered stopped by default.
Each evaluation reads the source catalog and launches fixed daily partitions
available from 2026-09-01 onward. Stable run keys suppress duplicate
sensor-launched runs. Historical and monthly partitions remain manual
backfills.

The sensor deliberately still targets the S3-only daily job. The three explicit
`*_duckdb_job` definitions materialize S3, raw DuckDB, and normalized DuckDB for
one year, month, or day while downstream behavior is validated manually.

Materialize one explicit daily partition before enabling the sensor. After its
archive/member metadata has been reconciled, the sensor can launch newly
published daily partitions. Catalog-driven automation is used instead of a
midnight schedule because archive publication time can vary.

## 9. Issues found during processing

- The catalog also links DCAT metadata files; discovery therefore accepts only
  exact `YYYY-MM-DD.tar.gz` filenames.
- Archive dates are source snapshot dates, not job publication dates.
- A standard Dagster time-window definition cannot change cadence within one
  asset. Three fixed assets encode the controlled year/month/day transition.
- The same vacancy may occur in this source and Platsbanken. Cross-source
  deduplication is explicitly out of scope; only rows explicitly attributed to
  `arbetsformedlingen.se` are excluded from JobTech Links normalization.
- A sampled 2026 archive was dominated by `arbetsformedlingen.se`, so retaining
  all raw JSON again in DuckDB would make yearly files unnecessarily large. S3
  remains the full-fidelity raw boundary and the snapshot catalog retains the
  excluded-row counts.

## 10. Verification

- Source and asset-graph contract: `tests/test_sweden_jobtech_links_source.py`
- Normalization contract: `tests/test_sweden_jobtech_links_normalize.py`
- Definition validation: `uv run dg check defs`
- Manual gate: materialize one explicit daily DuckDB job while the sensor
  remains stopped. Reconcile S3 archive counts with `snapshots`, confirm the
  Platsbanken/external split, and inspect version/observation/location/enrichment
  counts before switching daily automation to the downstream job.

# Sweden JobTech Links jobs design doc

## 1. Source overview

- **Country / source**: Sweden — JobTech Links
- **Module**: `defs/sweden_jobtech_links/`
- **Catalog**: `https://data.jobtechdev.se/annonser/jobtechlinks/index.html`
- **Format**: one dated `YYYY-MM-DD.tar.gz` archive containing newline-delimited
  JSON in `output.json`
- **Authentication**: none
- **Raw object bucket**: `source-sweden-jobtech-links`

JobTech Links aggregates job advertisements linked from Swedish job sites. The
source is separate from Platsbanken: raw and normalized records must preserve
their JobTech Links provenance and must not be deduplicated against Platsbanken.

## 2. Ingest mode — and why

The raw source boundary uses one named dynamic partition set with deliberately
mixed batch sizes:

- `year:2021` through `year:2025` for the historical backfill;
- `month:2026-01` through `month:2026-08` for the recent catch-up; and
- one `day:YYYY-MM-DD` partition per available archive from 2026-09-01 onward.

JobTech publishes daily archives, but running almost 1,900 historical Dagster
partitions would add orchestration overhead without changing the raw evidence.
The coarse historical partitions therefore batch source files while every
archive still receives its own date-and-SHA-256 object key. Daily partitions
begin at the operational cutover so retries and freshness remain precise.

## 3. Loading

`sweden_jobtech_links_snapshot_s3` resolves its exact partition window against
the live catalog and processes matching archives sequentially. Each archive is
streamed to a temporary file with dlt HTTP retries plus a whole-file retry loop.
The loader validates `Content-Length`, computes SHA-256 while downloading, and
verifies that the tarball contains exactly one non-empty, safe `output.json`
member before upload. Only one archive occupies temporary disk at a time.

The archive is preserved byte-for-byte at:

`snapshots/snapshot_date=<date>/sha256=<digest>/<date>.tar.gz`

An immutable `metadata.json` beside it records the source URL, archive headers,
member path and size, hash, first retrieval time, and originating Dagster run.
Each materialization also writes a run-specific partition manifest containing
the exact archive catalog for downstream replay. By default, retries reuse
complete stored archives without downloading them again; `refresh_existing`
forces a source recheck while preserving any changed content under a new hash.

## 4. Transform

No transformation is part of the initial asset. A later raw DuckDB asset will
stream `output.json`, retain every source row and provenance field, and derive
the tables owned by migration `000363_corpscout_se_jobtech_links_jobs`.

## 5. ClickHouse schema

Migration `000363_corpscout_se_jobtech_links_jobs` owns the future snapshot,
job version, observation, location, enrichment, active-interval, current-job,
and exact company-match tables. This raw S3 asset does not write ClickHouse.

## 6. Translation

Not applicable at the raw boundary. Source text remains unchanged.

## 7. Currency

Not applicable at the raw boundary. Any later compensation normalization must
retain the source value and currency evidence.

## 8. Scheduling

`sweden_jobtech_links_catalog_sensor` is registered stopped by default. Each
evaluation reads the source catalog, adds any missing historical/monthly/daily
dynamic partition keys, and launches runs only for new daily keys from
2026-09-01 onward. Historical and monthly partitions are registered for manual
backfill but are never launched automatically.

Register one explicit partition key through Dagster and materialize it before
enabling the sensor. After its archive/member metadata has been reconciled, the
sensor's first live tick can register all remaining catalog-backed keys and
launch any missing daily partitions. Catalog-driven automation is used instead
of a midnight schedule because archive publication time can vary.

## 9. Issues found during processing

- The catalog also links DCAT metadata files; discovery therefore accepts only
  exact `YYYY-MM-DD.tar.gz` filenames.
- Archive dates are source snapshot dates, not job publication dates.
- A standard Dagster time-window definition cannot change cadence within one
  asset. Named dynamic keys encode the controlled year/month/day transition.
- The same vacancy may occur in this source and Platsbanken. Cross-source
  deduplication is explicitly out of scope.

## 10. Verification

- Unit contract: `tests/test_sweden_jobtech_links_source.py`
- Definition validation: `uv run dg check defs`
- Manual gate: register and materialize one explicit partition of
  `sweden_jobtech_links_snapshot_s3` while the sensor remains stopped. Inspect
  its archive, metadata, and partition-manifest keys and compare byte counts
  with Dagster metadata before enabling daily automation.

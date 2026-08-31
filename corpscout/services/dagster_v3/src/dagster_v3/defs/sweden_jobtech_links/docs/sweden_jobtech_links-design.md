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

The first source boundary is a non-partitioned latest-snapshot asset. It reads
the catalog and selects the greatest valid date rather than assuming that an
archive exists for the current date. Every distinct archive is stored under a
date-and-SHA-256 content address, so corrections published under an existing
date remain distinct and replayable.

Historical partition ingestion is intentionally deferred. The catalog contains
many daily snapshots, and a history design must distinguish repeated
observations from actual job content versions before backfilling it.

## 3. Loading

`sweden_jobtech_links_snapshot_s3` streams the complete archive to a temporary
file with dlt HTTP retries plus a whole-file retry loop. It validates
`Content-Length`, computes SHA-256 while downloading, and verifies that the
tarball contains exactly one non-empty, safe `output.json` member before upload.

The archive is preserved byte-for-byte at:

`snapshots/snapshot_date=<date>/sha256=<digest>/<date>.tar.gz`

An immutable `metadata.json` beside it records the source URL, archive headers,
member path and size, hash, first retrieval time, and originating Dagster run.
Repeated materializations may download to verify the current source artifact,
but never rewrite an already stored content-addressed archive or metadata row.

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

No schedule is enabled until the asset has been manually materialized and its
archive/member metadata has been reconciled with the source catalog.

## 9. Issues found during processing

- The catalog also links DCAT metadata files; discovery therefore accepts only
  exact `YYYY-MM-DD.tar.gz` filenames.
- Archive dates are source snapshot dates, not job publication dates.
- The same vacancy may occur in this source and Platsbanken. Cross-source
  deduplication is explicitly out of scope.

## 10. Verification

- Unit contract: `tests/test_sweden_jobtech_links_source.py`
- Definition validation: `uv run dg check defs`
- Manual gate: materialize `sweden_jobtech_links_snapshot_s3`, inspect S3 archive
  and metadata keys, and compare hash and byte counts with Dagster metadata.

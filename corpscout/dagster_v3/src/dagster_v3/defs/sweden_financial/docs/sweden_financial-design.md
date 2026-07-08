# Sweden Financial Raw Archive Pipeline Design

## Source

Sweden annual reports are published by Bolagsverket as bulk ZIP archives under the
public S3-style listing endpoint:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler?prefix=arsredovisningar/&delimiter=\
```

The listing returns object keys such as `arsredovisningar/2020/08_2.zip`. The
source denies year-specific listing prefixes, so the resource lists the allowed
root prefix `arsredovisningar/`, starts year scans with a marker such as
`arsredovisningar/2026/`, and filters returned keys client-side. The download URL
is the listing host plus the upstream key:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/arsredovisningar/2020/08_2.zip
```

One downloaded outer ZIP contains many nested ZIPs. Each nested ZIP name carries
the company id and report-period end date, while the XHTML filename inside the
nested ZIP is a UUID. Because the outer ZIP is the upstream audit artifact, the
pipeline stores the outer ZIP files first, then extracts each nested report XHTML
into deterministic object keys and catalogs them in DuckDB.

## Resource

`SwedenFinancialReportsResource` owns the source-specific behavior:

- list archives from the XML listing endpoint, including `NextMarker` pagination;
- scan by year using a marker and client-side key filtering;
- build download URLs from upstream keys;
- derive deterministic object keys from year, archive name, and upstream
  `LastModified`;
- skip existing archive objects before issuing the archive `GET`;
- stream missing ZIP downloads to a temporary file;
- upload missing ZIPs through the shared `ObjectStoreResource`;
- emit changed/unchanged counts and sample S3 keys in materialization metadata.

This is a concrete resource rather than a generic downloader because the source
uses a specific S3 XML listing endpoint, a specific download URL rewrite, and
source-specific object-key conventions.

## Asset

`sweden_financial_backfill_raw_archives_s3` materializes the raw backfill archive
layer. It uses static year partitions `2020` through `2026`.

`sweden_financial_current_raw_archives_s3` materializes the current refresh
archive layer. It uses 7-day date partitions from `2026-07-04` through the end of
2026 and scans upstream archive year `2026` on each run.

It writes to bucket:

```text
source-sweden-financial
```

Archive objects use deterministic keys:

```text
sweden_financial/raw_archives/
  year=2020/
  archive=08_2.zip/
  source_last_modified=2025-02-07T09-13-53.713Z/
  archive.zip
```

If the same upstream key and `LastModified` timestamp already exists in object
storage, materialization reuses it and does not download the ZIP again.

Each raw archive asset also writes an archive sync manifest to object storage:

```text
sweden_financial/raw_archive_sync_manifests/
  sync_kind=backfill/
  load_partition_key=2026/
  manifest.json
```

The manifest records every archive observed in that raw materialization,
including upstream key, `LastModified`, ETag, object-storage key, and whether the
ZIP was downloaded or reused. Raw archive assets do not use DuckDB and do not run
in the `sweden_financial_duckdb` pool.

`sweden_financial_backfill_report_xhtml_catalog_duckdb` materializes the
extracted XHTML catalog for a full backfill year partition. It reads raw archive
objects from:

```text
sweden_financial/raw_archives/year=<partition_year>/
```

For each nested report ZIP, the asset writes the report body to:

```text
sweden_financial/report_xhtml/
  year=2020/
  company_id=5561234567/
  report_period_end=2020-12-31/
  source_archive_hash=<sha256-prefix>/
  source_archive=08_2.zip/
  nested_zip=<nested-zip-name>/
  report.xhtml
```

`sweden_financial_current_report_xhtml_catalog_duckdb` materializes only changed
current ZIPs for the same Dagster run. It reads the raw asset's archive sync
manifest, writes `sweden_financial.archive_sync_catalog` in DuckDB, reads changed
archive keys where `downloaded = true`, parses those ZIPs, and replaces catalog
rows only for the affected archive names.

The DuckDB table `sweden_financial.report_xhtml_catalog` stores one row per
extracted report XHTML with the partition year, company id, report-period end,
source archive object key, nested ZIP name, report object key, content length,
content hash, and `source_run_id`.

The DuckDB table `sweden_financial.archive_sync_catalog` stores one row per
archive sync manifest consumed by the catalog assets, including the upstream
key, `LastModified`, ETag, source size, object-storage key, and whether the
archive was downloaded or reused.

Catalog DuckDB files are partitioned by archive year. The file path is:

```text
data/sweden_financial/sweden_financial_source_<year>.duckdb
```

Backfill partitions write their own year file. Current refresh partitions write
the active archive-year file, currently `sweden_financial_source_2026.duckdb`.

`sweden_financial_backfill_parsed_reports_duckdb` reads the XHTML catalog rows
for its backfill year after XHTML extraction, loads each XHTML body from object
storage, parses inline XBRL facts, and replaces parsed rows in the same year
DuckDB file.

`sweden_financial_current_parsed_reports_duckdb` reads only catalog rows written
by the current refresh run and replaces parsed rows for those changed archive
names in `sweden_financial_source_2026.duckdb`.

The parsed DuckDB tables are:

- `sweden_financial.reports` - one row per parsed XHTML report, aligned with the
  ClickHouse `corpscout.se_financial_reports` shape.
- `sweden_financial.facts` - one row per parsed inline XBRL fact, aligned with
  the ClickHouse `corpscout.se_financial_facts` shape.
- `sweden_financial.parse_errors` - one row per XHTML document that failed
  parsing, so a bad report does not block the rest of the partition.

`sweden_financial_reports_clickhouse` and
`sweden_financial_facts_clickhouse` publish those parsed DuckDB tables into
`corpscout.se_financial_reports` and `corpscout.se_financial_facts`. Each
ClickHouse asset represents one physical ClickHouse table. The assets build a
read-only union view across existing per-year DuckDB files and replace the full
ClickHouse table from that combined parsed dataset.

## Job And Schedule

`sweden_financial_backfill_job` selects both
`sweden_financial_backfill_raw_archives_s3` and
`sweden_financial_backfill_report_xhtml_catalog_duckdb`, then
`sweden_financial_backfill_parsed_reports_duckdb`. Backfill should
materialize the 2020-2026 partitions.

`sweden_financial_current_year_job` selects
`sweden_financial_current_raw_archives_s3` and
`sweden_financial_current_report_xhtml_catalog_duckdb`, then
`sweden_financial_current_parsed_reports_duckdb`.

`sweden_financial_current_year_weekly` runs at `45 6 * * 6` in
`Europe/Belgrade`, targets the matching 7-day partition date, and is enabled by
default. Each weekly run can discover upstream `LastModified` changes and add
new raw archive versions while reusing unchanged archive objects.

`sweden_financial_clickhouse_job` selects
`sweden_financial_reports_clickhouse` and `sweden_financial_facts_clickhouse`.
Run it after the relevant parsed DuckDB partitions are materialized.

## Out Of Scope

Financial metric mapping and USD conversion are downstream layers. The parsed
report assets preserve report and fact-level XBRL data in DuckDB so those
metric layers can be built separately.

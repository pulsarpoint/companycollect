# Sweden Financial Raw Archive Pipeline Design

## Source

Sweden annual reports are published by Bolagsverket as bulk ZIP archives under the
public S3-style listing endpoint:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler?prefix=arsredovisningar/&delimiter=\
```

The listing returns object keys such as `arsredovisningar/2020/08_2.zip`. The
year component is a real upstream boundary, so the Dagster assets are partitioned
by report archive year. The download URL is the listing host plus the upstream
key:

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
- filter listings by partition year, for example `arsredovisningar/2020/`;
- build download URLs from upstream keys;
- derive deterministic object keys from year, archive name, and upstream
  `LastModified`;
- skip existing archive objects before issuing the archive `GET`;
- stream missing ZIP downloads to a temporary file;
- upload missing ZIPs through the shared `ObjectStoreResource`;
- emit counts and sample S3 keys in materialization metadata.

This is a concrete resource rather than a generic downloader because the source
uses a specific S3 XML listing endpoint, a specific download URL rewrite, and
source-specific object-key conventions.

## Asset

`sweden_financial_raw_archives_s3` materializes the raw archive layer. It uses a
static year partition set that includes the 2020-2025 backfill years plus the
current runtime year when that year is outside the backfill range.

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

There is no manifest file for the raw archive layer. The raw archive objects are
self-describing enough for the next asset to list by partition prefix and parse
metadata from the object key.

`sweden_financial_report_xhtml_catalog_duckdb` materializes the extracted XHTML
catalog for the same year partition. It reads raw archive objects from:

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

The DuckDB table `sweden_financial.report_xhtml_catalog` stores one row per
extracted report XHTML with the partition year, company id, report-period end,
source archive object key, nested ZIP name, report object key, content length,
content hash, and `source_run_id`. Materializing one partition replaces only that
partition's catalog rows.

## Job And Schedule

`sweden_financial_backfill_job` selects both
`sweden_financial_raw_archives_s3` and
`sweden_financial_report_xhtml_catalog_duckdb`. Backfill should materialize the
2020-2025 partitions.

`sweden_financial_current_year_job` selects the same assets and is used by the
active refresh path.

`sweden_financial_current_year_weekly` runs at `45 6 * * 1` in
`Europe/Belgrade`, targets the current calendar year partition, and is enabled by
default. Each weekly run can discover upstream `LastModified` changes and add
new raw archive versions while reusing unchanged archive objects.

## Out Of Scope

Nested ZIP preservation, ClickHouse tables, XBRL fact parsing, and financial
metric mapping are intentionally out of scope for this slice. XHTML extraction is
represented by the report XHTML catalog asset; parsing those XHTML/XBRL facts
into normalized financial statement tables is the next pipeline layer.

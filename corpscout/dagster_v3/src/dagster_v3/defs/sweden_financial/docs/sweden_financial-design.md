# Sweden Financial Raw Archive Pipeline Design

## Source

Sweden annual reports are published by Bolagsverket as bulk ZIP archives under the
public S3-style listing endpoint:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler?prefix=arsredovisningar/&delimiter=\
```

The listing returns object keys such as `arsredovisningar/2020/08_2.zip`. The
download URL is the listing host plus the upstream key:

```text
https://vardefulla-datamangder.bolagsverket.se/arsredovisningar-bulkfiler/arsredovisningar/2020/08_2.zip
```

One downloaded outer ZIP contains many nested ZIPs. Each nested ZIP name carries
the company id and report-period end date, while the XHTML filename inside the
nested ZIP is a UUID. Because the outer ZIP is the upstream audit artifact, this
first slice stores only the outer ZIP files.

## Resource

`SwedenFinancialReportsResource` owns the source-specific behavior:

- list archives from the XML listing endpoint, including `NextMarker` pagination;
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

`sweden_financial_raw_archives_s3` materializes the raw archive layer.

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

There is no manifest file in this first slice. The raw archive objects are
self-describing enough for the next asset to list by prefix and parse metadata
from the object key. A durable catalog table can be added later if XHTML
extraction needs more queryable metadata than object keys provide.

## Job And Schedule

`sweden_financial_raw_archives_refresh_job` selects only
`sweden_financial_raw_archives_s3`.

`sweden_financial_raw_archives_weekly` runs at `45 6 * * 1` in
`Europe/Belgrade`. The schedule is stopped by default until the first live
materialization is validated.

## Out Of Scope

XHTML extraction, nested ZIP preservation, DuckDB tables, ClickHouse tables,
XBRL fact parsing, and financial metric mapping are intentionally out of scope
for this slice. XHTML extraction is the next logical asset after raw archive
download is validated.

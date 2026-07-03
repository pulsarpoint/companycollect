# Sweden Company Raw Snapshot And DuckDB Design

## Source Overview

Sweden company data is available from Bolagsverket's public high-value-dataset host as two full ZIP snapshots:

| dataset | url | format | cadence | auth |
|---|---|---|---|---|
| SCB/FDB company bulk file | `https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip` | ZIP containing tab-separated text | about every 7 days | no |
| Bolagsverket legal-register bulk file | `https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip` | ZIP containing semicolon-separated text | about every 7 days | no |

The first implementation captures each raw ZIP company file and rebuilds a raw DuckDB database from those ZIPs. No parsing into a normalized company model, source-field normalization, ClickHouse export, contact extraction, or financial annual-report processing is part of this slice.

## Resource

`SwedenCompanyBulkResource` in `resources.py` owns the source-specific download behavior:

- the two official company ZIP URLs;
- streaming HTTP download with whole-file retry for transient connection, timeout, and incomplete-stream failures;
- SHA256 and byte-count collection for newly downloaded files;
- deterministic object keys in the `source-sweden-company` bucket;
- HEAD-based source timestamp resolution from `Last-Modified`;
- skip behavior when the source-last-modified/source ZIP key already exists in object storage.

This is a concrete source resource rather than a generic downloader because the source has a fixed pair of files, a known weekly cadence, and source-specific object-key conventions. The shared `ObjectStoreResource` remains the S3/RustFS boundary.

## Assets

`sweden_company_raw_snapshot_s3` materializes the raw snapshot.

On each materialization it:

1. Computes `retrieved_date` from the materialization timestamp.
2. Ensures the `source-sweden-company` bucket exists.
3. For each company ZIP, reads HTTP metadata with `HEAD` and derives `source_last_modified` from `Last-Modified`.
4. Checks whether the source-last-modified/source object key already exists.
5. Skips downloading files that already exist.
6. Downloads missing files and uploads them to S3/RustFS.
7. Writes a date-level manifest.
8. Emits materialization metadata with bucket, manifest key, file keys, downloaded count, reused count, and downloaded bytes.

Object keys are date-based:

```text
sweden_company/raw/source_last_modified=YYYY-MM-DDTHH-MM-SSZ/source=scb_bulkfil/source.zip
sweden_company/raw/source_last_modified=YYYY-MM-DDTHH-MM-SSZ/source=bolagsverket_bulkfil/source.zip
sweden_company/raw/retrieved_date=YYYY-MM-DD/manifest.json
```

If the same upstream `Last-Modified` timestamp already exists, the asset reuses that raw ZIP and does not issue an HTTP GET download for that file. A rerun still issues lightweight HEAD requests to resolve the current source timestamp.

`sweden_company_raw_duckdb` materializes the raw DuckDB staging database.

On each materialization it:

1. Resolves the raw manifest for the current Dagster run, falling back to the latest manifest if the raw download asset was materialized separately.
2. Downloads each ZIP listed in that manifest from the `source-sweden-company` bucket.
3. Extracts the single text member from each ZIP into a temporary local directory.
4. Replaces the raw DuckDB tables with the exact source columns plus provenance columns.
5. Emits row-count metadata for the manifest table and the two source tables.

The DuckDB file is:

```text
data/sweden_company_source.duckdb
```

The DuckDB schema is `sweden_company`.

| table | source | purpose |
|---|---|---|
| `raw_files` | manifest | one row per source ZIP used for the load |
| `bolagsverket_raw` | `bolagsverket_bulkfil.zip` | raw legal-register rows from the semicolon-separated file |
| `scb_raw` | `scb_bulkfil.zip` | raw SCB/FDB rows from the tab-separated file |

Each source table is a full replacement table. The source columns are loaded as `varchar` to preserve upstream values before we decide normalization rules. The loader also adds:

| column | meaning |
|---|---|
| `source_run_id` | Dagster run id from the selected raw manifest |
| `source_line_number` | 1-based row number inside the extracted source data, excluding the header |
| `source_record_id` | source identifier field (`organisationsidentitet` for Bolagsverket, `PeOrgNr` for SCB) |
| `source_payload_hash` | SHA256 of the raw JSON representation of the source columns |
| `source_s3_key` | object-store key of the ZIP loaded into the table |
| `raw_record` | JSON representation of the source columns for audit/debug use |

## Job And Schedule

`sweden_company_raw_snapshot_job` selects `sweden_company_raw_duckdb` with its upstream dependency, so the job runs both `sweden_company_raw_snapshot_s3` and `sweden_company_raw_duckdb`.

`sweden_company_raw_snapshot_weekly` runs at `15 6 * * 1` in `Europe/Belgrade`, matching the observed roughly weekly source refresh and staggering it from other country jobs. The schedule is `STOPPED` by default until the first live materialization is validated.

## Out Of Scope

No normalized company model, ClickHouse export, translation, NACE mapping, contacts, domain extraction, or financial annual-report processing is included here. Those belong in later Sweden company and Sweden financial slices after the raw object-storage and raw DuckDB boundaries are validated.

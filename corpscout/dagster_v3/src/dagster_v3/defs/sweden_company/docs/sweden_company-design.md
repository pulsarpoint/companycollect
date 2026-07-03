# Sweden Company Registry Pipeline Design

## Source Overview

Sweden company data is available from Bolagsverket's public high-value-dataset host as two full ZIP snapshots:

| dataset | url | format | cadence | auth |
|---|---|---|---|---|
| SCB/FDB company bulk file | `https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip` | ZIP containing tab-separated text | about every 7 days | no |
| Bolagsverket legal-register bulk file | `https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip` | ZIP containing semicolon-separated text | about every 7 days | no |

The implementation captures each raw ZIP company file, rebuilds a raw DuckDB database from those ZIPs, materializes deterministic normalized DuckDB tables from the raw staging tables, and publishes those tables to migrated ClickHouse tables. Contact/domain candidates and financial annual-report processing are separate later slices.

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
7. Writes a run-scoped manifest and updates a date-level latest-manifest pointer.
8. Emits materialization metadata with bucket, manifest key, file keys, downloaded count, reused count, and downloaded bytes.

Object keys are date-based:

```text
sweden_company/raw/source_last_modified=YYYY-MM-DDTHH-MM-SSZ/source=scb_bulkfil/source.zip
sweden_company/raw/source_last_modified=YYYY-MM-DDTHH-MM-SSZ/source=bolagsverket_bulkfil/source.zip
sweden_company/raw/retrieved_date=YYYY-MM-DD/run_id=<dagster-run-id>/manifest.json
sweden_company/raw/retrieved_date=YYYY-MM-DD/manifest.json
```

If the same upstream `Last-Modified` timestamp already exists, the asset reuses that raw ZIP and does not issue an HTTP GET download for that file. A rerun still issues lightweight HEAD requests to resolve the current source timestamp.

The run-scoped manifest is the canonical manifest for downstream assets in the same Dagster run. The date-level manifest is only a latest pointer for manual downstream-only materializations and does not replace earlier run-scoped manifests.

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

`sweden_company_normalized_duckdb` materializes deterministic normalized tables from the raw DuckDB tables.

It creates:

| table | purpose |
|---|---|
| `companies` | one row per normalized organization identifier, with Bolagsverket preferred over SCB for legal identity |
| `company_addresses` | parsed Bolagsverket postal addresses and SCB fallback/enrichment addresses |
| `company_industry_codes` | one row per valid non-empty SCB `Ng1`..`Ng5` SNI code |

The industry-code table stores the raw 5-digit SNI code and derives `nace_rev2_class_code` from the first four digits. It does not label the 5-digit SNI value as NACE because the fifth digit is Sweden-specific detail.

Contact extraction is intentionally separate. Domains, emails, and phone numbers in these sources are unstructured text candidates, not canonical registry fields, and should be handled later by `sweden_company_contact_candidates_duckdb`.

`sweden_company_clickhouse` publishes the normalized DuckDB tables to ClickHouse.

It asserts that migrations have already created the target tables and then full-replaces each table through a staging-table swap:

| DuckDB table | ClickHouse table | purpose |
|---|---|---|
| `sweden_company.companies` | `corpscout.se_companies` | one row per normalized organization identifier |
| `sweden_company.company_addresses` | `corpscout.se_company_addresses` | source-specific postal/visiting address observations |
| `sweden_company.company_industry_codes` | `corpscout.se_industries` | SCB SNI activity codes with derived 4-digit NACE Rev. 2 class code |

The ClickHouse tables are created only by migrations. Dagster does not run DDL beyond temporary stage-table creation during export.

## Job And Schedule

`sweden_company_refresh_job` selects `sweden_company_clickhouse` with its upstream dependencies, so the job runs raw S3 download/reuse, raw DuckDB rebuild, normalized DuckDB rebuild, and ClickHouse publish.

`sweden_company_refresh_weekly` runs at `15 6 * * 1` in `Europe/Belgrade`, matching the observed roughly weekly source refresh and staggering it from other country jobs. The schedule is `STOPPED` by default until the first live materialization is validated.

## Out Of Scope

No contact/domain extraction, financial annual-report processing, translation, or external NACE label enrichment is included here. Those belong in later Sweden company and Sweden financial slices after the registry pipeline is validated.

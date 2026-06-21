# GLEIF CSV Bulk Load Design

Date: 2026-06-21

## Problem

The current GLEIF full bootstrap downloads raw Golden Copy files quickly, but the `gleif_reference_duckdb_state` step can run for hours. The slow path is Python row-by-row JSON normalization followed by DuckDB `executemany` inserts. That approach is memory-safe, but it is not appropriate for GLEIF full snapshot scale.

The ClickHouse publication path also uses Python row batches from DuckDB into ClickHouse. That can remain as a later optimization, but the immediate blocking issue is the DuckDB state build.

## Decision

Use GLEIF Golden Copy CSV ZIP files as the canonical raw files for this pipeline. Use dlt to load extracted CSV files into DuckDB raw/staging tables, then build the normalized DuckDB state with set-based DuckDB SQL.

The final ClickHouse schema remains unchanged. The change is upstream: raw files become `latest.csv.zip`, dlt owns CSV-to-DuckDB ingestion, and DuckDB SQL owns normalized relational transformations instead of Python JSON parsing.

## Asset Graph

Keep the existing Dagster graph:

```text
gleif_full_raw_reference_files
  -> gleif_reference_duckdb_state
  -> gleif_reference_clickhouse
  -> gleif_raw_retention
```

and for daily deltas:

```text
gleif_delta_raw_reference_files
  -> gleif_reference_duckdb_state
  -> gleif_reference_clickhouse
  -> gleif_raw_retention
```

The raw assets still persist source files to object storage first. The DuckDB state asset remains the normalized local state builder. The ClickHouse asset remains the final publisher to `corpscout.gleif_*`.

## Raw Files

`GleifRawDownloadConfig.file_format` should default to `csv`.

The raw asset should download:

- `lei2/latest.csv`
- `rr/latest.csv`
- `repex/latest.csv`

Full bootstrap uses no `delta` parameter. Daily delta uses `delta=LastDay`.

Object keys should look like:

```text
gleif/raw/load_mode=full/publish_date=<publish_date>/run_id=<run_id>/file_kind=lei_records/source.csv.zip
gleif/raw/load_mode=full/publish_date=<publish_date>/run_id=<run_id>/file_kind=relationships/source.csv.zip
gleif/raw/load_mode=full/publish_date=<publish_date>/run_id=<run_id>/file_kind=reporting_exceptions/source.csv.zip
```

The manifest should include `file_format: "csv"` per downloaded file so the processing asset can reject unsupported legacy JSON manifests with a clear message.

## dlt And DuckDB Processing

For each file in the manifest:

1. Copy the raw ZIP from object storage to a temporary directory.
2. Validate that the ZIP contains exactly one CSV member.
3. Extract the CSV member to a temporary file.
4. Create one dlt resource for the extracted CSV.
5. Run a dlt pipeline with the DuckDB destination into raw/staging tables.
6. Build normalized staging tables with DuckDB `CREATE TABLE AS SELECT`.
7. Replace or upsert the current `gleif.*` tables only after all staging work succeeds.

dlt should own the mechanical loading boundary:

```text
extracted CSV files
-> dlt resources
-> DuckDB raw/staging tables
```

DuckDB SQL should own the relational normalization boundary:

```text
dlt raw/staging tables
-> normalized gleif_staging tables
-> current gleif tables
```

This keeps dlt focused on durable source loading and schema handling while avoiding row-by-row Python transformations for the wide GLEIF CSVs.

For full mode:

```text
dlt raw/staging tables
-> normalized gleif_staging tables
-> transactionally replace gleif tables
```

For delta mode:

```text
dlt raw/staging tables
-> normalized gleif_staging tables
-> transactionally upsert into current gleif tables
```

The current DuckDB database path remains `data/gleif.duckdb`.

## Normalized Table Mapping

The final normalized tables stay the same:

- `gleif_lei_records`
- `gleif_lei_names`
- `gleif_lei_addresses`
- `gleif_lei_identifiers`
- `gleif_lei_relationships`
- `gleif_lei_relationship_periods`
- `gleif_lei_reporting_exceptions`
- `gleif_lei_issuers`
- `gleif_code_list_entries`

`gleif_lei_records` maps from the wide `lei2` CSV columns such as:

- `LEI`
- `Entity.LegalName`
- `Entity.LegalName.xmllang`
- `Entity.EntityStatus`
- `Entity.LegalJurisdiction`
- `Entity.EntityCategory`
- `Entity.EntitySubCategory`
- `Entity.LegalForm.EntityLegalFormCode`
- `Entity.LegalForm.OtherLegalForm`
- `Entity.RegistrationAuthority.RegistrationAuthorityID`
- `Entity.RegistrationAuthority.OtherRegistrationAuthorityID`
- `Entity.RegistrationAuthority.RegistrationAuthorityEntityID`
- `Entity.EntityCreationDate`
- `Entity.EntityExpirationDate`
- `Entity.EntityExpirationReason`
- `Registration.InitialRegistrationDate`
- `Registration.LastUpdateDate`
- `Registration.RegistrationStatus`
- `Registration.NextRenewalDate`
- `Registration.ManagingLOU`
- `Registration.ValidationSources`
- `Registration.ValidationAuthority.ValidationAuthorityID`
- `Registration.ValidationAuthority.OtherValidationAuthorityID`
- `Registration.ValidationAuthority.ValidationAuthorityEntityID`
- `ConformityFlag`

`gleif_lei_names` should include the legal name plus repeated CSV name columns:

- `Entity.OtherEntityNames.OtherEntityName.1..5`
- `Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1..5`

The SQL should generate these rows with explicit `UNION ALL` branches and filter empty names.

`gleif_lei_addresses` should include legal and headquarters addresses first:

- `Entity.LegalAddress.*`
- `Entity.HeadquartersAddress.*`

Other and transliterated addresses are out of scope for this optimization unless we decide to extend address roles later.

`gleif_lei_identifiers` should include identifiers available in the CSV that map to the existing table contract. The initial SQL should preserve fields already present in the JSON parser where CSV columns exist. If a mapping is not available in the three Golden Copy CSV files, it should not be invented.

`gleif_lei_relationships` maps from the `rr` CSV:

- `Relationship.StartNode.NodeID`
- `Relationship.StartNode.NodeIDType`
- `Relationship.EndNode.NodeID`
- `Relationship.EndNode.NodeIDType`
- `Relationship.RelationshipType`
- `Relationship.RelationshipStatus`
- `Registration.InitialRegistrationDate`
- `Registration.LastUpdateDate`
- `Registration.RegistrationStatus`
- `Registration.NextRenewalDate`
- `Registration.ManagingLOU`
- `Registration.ValidationSources`
- `Registration.ValidationDocuments`
- `Registration.ValidationReference`
- `DeletedAt`

`relationship_record_id` can remain the stable synthetic ID based on start LEI, relationship type, and end LEI, matching the current parser behavior.

`gleif_lei_relationship_periods` should use explicit `UNION ALL` over:

- `Relationship.Period.1.startDate`
- `Relationship.Period.1.endDate`
- `Relationship.Period.1.periodType`
- through `Relationship.Period.5.*`

`gleif_lei_reporting_exceptions` maps from the `repex` CSV:

- `LEI`
- `Exception.Category`
- `Exception.Reason.1..5`
- `Exception.Reference.1..5`
- `DeletedAt`

The existing final table has one `exception_reason` and one `exception_reference`. The initial CSV SQL should use the first non-empty reason/reference, and `exception_record_id` should be a stable hash based on LEI and category.

`gleif_lei_issuers` and `gleif_code_list_entries` should remain empty in this change. They require separate GLEIF mapping/reference files and should be added as a later source extension.

## ClickHouse Publication

The initial fix keeps `gleif_reference_clickhouse` unchanged. It will continue publishing from DuckDB to ClickHouse using the existing `replace_duckdb_tables_in_clickhouse` helper.

This means the first performance fix targets the proven bottleneck: Python JSON normalization into DuckDB. If ClickHouse publication later becomes too slow, introduce a second design for direct ClickHouse loading from DuckDB-exported CSV or Parquet.

## Failure Handling

Raw download remains all-or-nothing per Dagster run. If any of the three Golden Copy files fails, the manifest should not represent a complete run.

DuckDB processing must fail before touching current tables when:

- a manifest file is not `file_format = "csv"`
- a ZIP contains zero CSV members
- a ZIP contains more than one CSV member
- required CSV columns are missing
- a dlt CSV load fails
- expected dlt raw/staging tables are missing

Full mode should replace current DuckDB tables only after every normalized staging table has been built successfully.

Delta mode should upsert current DuckDB tables only after the complete delta staging set has been built successfully.

## Observability

Add progress logs around each expensive boundary:

```text
copying_gleif_raw_file_from_s3 file_kind=<kind> size_bytes=<bytes>
extracted_gleif_csv file_kind=<kind> csv_path=<path> size_bytes=<bytes>
loaded_gleif_source_csv_with_dlt file_kind=<kind> table=<table> row_count=<count>
built_gleif_table table=<table> row_count=<count>
published_gleif_clickhouse_table table=<table> row_count=<count>
```

These logs should make server runs diagnosable without checking `/tmp` or process internals.

## Testing

Add or update tests for:

- `GleifRawDownloadConfig.file_format` defaults to `csv`.
- raw object keys use `source.csv.zip`.
- manifest file entries include `file_format`.
- CSV manifests are accepted by DuckDB state processing.
- non-CSV manifests fail with a clear error.
- ZIPs with zero or multiple CSV members fail before current DuckDB tables are replaced.
- extracted CSV files are loaded through dlt into DuckDB raw/staging tables.
- a dlt load failure prevents normalized/current table replacement.
- small `lei2` CSV ZIP fixture builds `gleif_lei_records`.
- repeated name columns become `gleif_lei_names` rows.
- legal and headquarters address columns become `gleif_lei_addresses` rows.
- small `rr` CSV ZIP fixture builds relationships and relationship periods.
- small `repex` CSV ZIP fixture builds reporting exceptions.
- the CSV processing path does not call the JSON parser row iterator.

Existing tests for table constants, asset registration, jobs, schedules, and ClickHouse migrations should continue to pass.

## Migration And Deployment Notes

Existing JSON raw files in S3 can remain as historical artifacts. New materializations should write CSV raw files under new `source.csv.zip` keys.

If the server has an in-flight `gleif_reference_bootstrap_job` stuck in the old Python JSON path, stop that run before deploying this change. After deployment, launch the bootstrap job again so it downloads CSV files and builds DuckDB through the new bulk path.

No ClickHouse DDL migration is required for this optimization because final table names and columns remain unchanged.

## Out Of Scope

- Direct ClickHouse ingestion from GLEIF CSV.
- Parquet chunk generation on S3.
- GLEIF issuer and code-list mapping files.
- Additional address roles for other and transliterated addresses.
- Removing the old JSON parser code.

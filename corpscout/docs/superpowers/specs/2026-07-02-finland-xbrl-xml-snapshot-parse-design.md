# Finland XBRL XML Snapshot Parse Asset Design

## Summary

Add a monthly-partitioned Dagster asset that runs after `data_snapshot_xml` for the same partition. The asset reads the XML snapshot manifest from S3, parses each XML document with the existing `parse_statement_xml` path, writes per-document temporary parquet files while processing, merges those files into one partition-scoped DuckDB database, and removes the temporary parquet files when the DuckDB file is complete.

This keeps XML downloading and XML parsing separate:

- `data_snapshot_xml`: downloads or reuses raw XML files from PRH and writes `manifest.jsonl` plus `_SUCCESS.json`.
- New parse asset: reads that completed XML snapshot folder and produces structured parsed tables.

## Asset

Name:

```text
data_snapshot_xml_duckdb
```

Group:

```text
finland_xbrl
```

Partitions:

```text
XML_SNAPSHOT_PARTITIONS
```

Dependency:

```text
data_snapshot_xml
```

Description:

```text
Parses monthly historical Finland XBRL XML snapshot files from S3 into a partition-scoped DuckDB database.
```

The asset should be included in the same historical XML snapshot job after `data_snapshot_xml`, or in a dedicated job if we want to run parsing independently after XML download verification.

Recommended job update:

```text
finland_xbrl_xml_snapshot_job:
  - data_snapshot_xml
  - data_snapshot_xml_duckdb
```

## Partition Window

Use the same partition window as `data_snapshot_xml`.

For partition key `2023-07-01`, the asset reads:

```text
source-finland-prh-xbrl/
  financial_data/xml_snapshot/
    registeredDateStart=2023-07-01/
      registeredDateEnd=2023-07-31/
        manifest.jsonl
        _SUCCESS.json
        companies/<business_id>/<financial_date>.xml
```

The parse asset should require `_SUCCESS.json`. If the marker is missing, fail hard. Parsing an unfinished XML snapshot folder would hide source incompleteness.

## Output Layout

DuckDB output should be partition-scoped and deterministic:

```text
data/finland_xbrl/duckdb/
  xml_snapshot_parse/
    partition_key=2023-07-01/
      data.duckdb
```

Temporary parquet output should be local and partition-scoped:

```text
data/finland_xbrl/tmp/
  xml_snapshot_parse/
    partition_key=2023-07-01/
      statement_documents/
        part-000001.parquet
      facts/
        part-000001.parquet
```

After the DuckDB database is successfully built, remove the temporary parquet directory for that partition.

## DuckDB Tables

Create two tables in the partition DuckDB:

```text
statement_documents
facts
```

Schemas should match the existing table contracts:

- `dagster_v3.defs.finland_xbrl.tables.STATEMENT_DOCUMENTS_COLUMNS`
- `dagster_v3.defs.finland_xbrl.tables.STATEMENT_DOCUMENTS_POLARS_SCHEMA`
- `dagster_v3.defs.finland_xbrl.tables.FACTS_COLUMNS`
- `dagster_v3.defs.finland_xbrl.tables.FACTS_POLARS_SCHEMA`

The asset should not recreate removed raw ClickHouse tables like `fi_xbrl_contexts`, `fi_xbrl_units`, `fi_xbrl_facts_raw`, or `fi_xbrl_taxonomy_codes`. This parsed DuckDB is an intermediate partition artifact for downstream metric extraction.

## Data Flow

1. Compute `registeredDateStart` and `registeredDateEnd` from `context.partition_time_window`.
2. Build the XML snapshot prefix with existing helpers:
   - `xml_snapshot_partition_prefix`
   - `xml_snapshot_manifest_key`
   - `xml_snapshot_success_key`
3. Check `_SUCCESS.json` exists in `XBRL_BUCKET`.
4. Read `manifest.jsonl` from S3.
5. For each manifest row:
   - Read `xml_object_key` from S3.
   - Call existing `parse_statement_xml(...)`.
   - Write one temporary statement parquet file if the parser returns statement rows.
   - Write one temporary facts parquet file if the parser returns fact rows.
6. Open/create the partition DuckDB file.
7. Create or replace `statement_documents` from all temporary statement parquet files.
8. Create or replace `facts` from all temporary facts parquet files.
9. Remove temporary parquet files.
10. Return metadata with parsed counts and output paths.

## Parser Reuse

Use the existing parser entry point:

```python
parse_statement_xml(
    business_id=...,
    financial_date=...,
    registration_date=...,
    source_url=...,
    xml_object_key=...,
    source_run_id=context.run.run_id,
    body=xml_bytes,
    parsed_at=parsed_at,
)
```

Do not introduce a second XML parser. The parser currently returns:

```text
ParsedStatement.rows_by_table[fi_prh_xbrl_statement_documents]
ParsedStatement.rows_by_table[fi_prh_xbrl_facts_raw]
```

The wrapper should normalize rows using the same column selection already used in `assets/parse.py`.

## Failure Behavior

The asset should fail hard when:

- `_SUCCESS.json` is missing.
- `manifest.jsonl` is missing.
- A manifest row is missing required fields.
- An XML file listed in the manifest is missing from S3.
- DuckDB creation fails.

Parser failures for individual XML documents should be recorded in an in-memory failed list and logged. The current parser asset skips bad documents and continues; for this snapshot asset we should preserve that behavior initially so one malformed XML does not block a full historical month. The materialization metadata must include `documents_failed_this_run`, and warnings should name the failed object keys.

If we later decide that malformed XML should fail the whole partition, that can be changed behind a config flag.

## Empty Partition Behavior

If the XML snapshot manifest is empty:

- Create a DuckDB database anyway.
- Create empty `statement_documents` and `facts` tables with the expected schemas.
- Return `documents_in_manifest=0`, `statement_documents_row_count=0`, and `facts_row_count=0`.

This makes a valid empty partition materialize successfully and keeps downstream jobs deterministic.

## Logging

Log these points:

- Partition parse start with partition key, S3 manifest key, and DuckDB path.
- Manifest row count.
- Progress at first document, every 25 documents, and final document.
- Per-document parse failures with business ID, financial date, and XML key.
- DuckDB table creation start and completion.
- Temporary parquet cleanup completion.
- Final counts.

## Metadata

Return materialization metadata:

```text
partition
registered_date_start
registered_date_end
s3_bucket
s3_prefix
manifest_key
success_key
duckdb_path
documents_in_manifest
documents_parsed_this_run
documents_failed_this_run
statement_documents_row_count
facts_row_count
temporary_statement_parquet_count
temporary_facts_parquet_count
temporary_directory_removed
```

## Code Organization

Create a separate asset file:

```text
dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/data_snapshot_xml_duckdb.py
```

Recommended public helpers:

```python
FINLAND_XBRL_XML_SNAPSHOT_PARSE_DUCKDB_PATH
xml_snapshot_parse_duckdb_path(partition_key: str) -> Path
parse_xml_snapshot_manifest_rows(...)
materialize_data_snapshot_xml_duckdb(...)
data_snapshot_xml_duckdb(...)
```

Register exports in:

```text
dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/__init__.py
```

Update job selection in:

```text
dagster_v3/src/dagster_v3/defs/finland_xbrl/assets/jobs.py
```

## Tests

Add tests to `tests/test_finland_xbrl_assets.py`:

1. `data_snapshot_xml_duckdb` uses the same monthly partitions as `data_snapshot_xml`.
2. DuckDB path helper returns:
   ```text
   data/finland_xbrl/duckdb/xml_snapshot_parse/partition_key=2023-07-01/data.duckdb
   ```
3. Missing `_SUCCESS.json` raises and does not create DuckDB.
4. Missing `manifest.jsonl` raises and does not create DuckDB.
5. Empty manifest creates DuckDB with empty `statement_documents` and `facts` tables.
6. One valid XML manifest row creates both tables with expected row counts.
7. Existing parser is used by injecting a fake parser and asserting the input fields.
8. Parser failure is logged/recorded and does not block other documents.
9. Temporary parquet files are removed after a successful DuckDB merge.
10. `finland_xbrl_xml_snapshot_job` selects both `data_snapshot_xml` and `data_snapshot_xml_duckdb`.

## Verification

Run:

```bash
cd companycollect/corpscout/dagster_v3
uv run pytest tests/test_finland_xbrl_assets.py -q
uv run ruff check src/dagster_v3/defs/finland_xbrl tests/test_finland_xbrl_assets.py
uv run dg check defs
cd ..
git diff --check
```

## Open Decisions

1. Parser failure policy: current recommendation is continue and record failures, matching existing parse assets. If we want strict historical correctness, make parser failure fail the whole partition instead.
2. Output location: current recommendation is local DuckDB under `data/finland_xbrl/duckdb`. If this needs to be shared across Dagster workers, the DuckDB file should be uploaded to S3 after creation.
3. Downstream asset contract: this design only creates parsed DuckDB tables. A later asset should read these partition DuckDB files and build financial metrics.

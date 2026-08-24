# ESEF parsing canonicalization

## Outcome

ESEF parsing has one artifact-first, processed-week-partitioned path. Report
packages are parsed once by `esef_document_artifacts_s3`; four independent
DuckDB assets then project the result and one multi-asset operation publishes
the four ClickHouse tables one partition at a time.

The canonical datasets are:

1. XBRL facts;
2. contact candidates;
3. taxonomy labels; and
4. disclosures.

There are no compatibility aliases or parallel Dagster asset keys. Historical
ClickHouse migrations retain their original filenames because applied migration
history is immutable. Migration `000313_corpscout_esef_parsing_canonical`
removes the early-development tables and promotes the weekly tables to the
stable table names.

## Runtime graph

```text
esef_filings_index_duckdb
  -> esef_document_extraction_manifest_s3
  -> esef_document_artifacts_s3
       -> esef_filing_facts_duckdb
       -> esef_document_contact_candidates_duckdb
       -> esef_document_concept_labels_duckdb
       -> esef_disclosures_duckdb

all four DuckDB assets
  -> esef_parsing_clickhouse (one operation, four ClickHouse asset outputs)

esef_disclosures_clickhouse + esef_document_concept_labels_clickhouse
  -> esef_document_company_information_clickhouse
```

Each DuckDB output is an isolated file for one `processed_week`. This permits
parallel parsing without concurrent writers sharing a DuckDB file. Each
ClickHouse publisher stages rows, verifies the staged count, replaces exactly
one partition, and verifies the published partition count.

## Validation contract

- The artifact producer records row counts for contacts and labels; each
  projection independently checks its written count.
- Fact parsing fails an incomplete partition when an artifact is missing or a
  document fails to parse.
- Every DuckDB file contains a completed `_partition_status` row with expected
  and actual counts.
- ClickHouse publication refuses a mismatched stage or target partition count.
- Re-materializing a processed week replaces only that week and is idempotent.

## Deployment order

1. Stop ESEF schedules and confirm no active ESEF runs.
2. Apply ClickHouse migrations through `000316`.
3. Deploy Dagster code.
4. Materialize one closed processed week through all four DuckDB assets and the
   four-output ClickHouse publication operation.
5. Verify counts, uniqueness, partition isolation, downstream availability,
   and a successful idempotent rerun.
6. Start larger backfills only after the bounded run passes.

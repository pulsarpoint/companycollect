# ESEF parsing canonicalization

## Outcome

ESEF parsing has one artifact-first, processed-week-partitioned path. Report
packages are parsed once by `esef_document_artifacts_s3`; five independent
DuckDB assets then project the result and five matching assets publish one
ClickHouse partition at a time.

The canonical datasets are:

1. source documents;
2. XBRL facts;
3. contact candidates;
4. taxonomy labels; and
5. disclosures.

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
       -> esef_source_documents_duckdb
       -> esef_filing_facts_duckdb
       -> esef_document_contact_candidates_duckdb
       -> esef_document_concept_labels_duckdb
       -> esef_fact_disclosures_duckdb

each DuckDB asset
  -> matching ClickHouse asset
```

Each DuckDB output is an isolated file for one `processed_week`. This permits
parallel parsing without concurrent writers sharing a DuckDB file. Each
ClickHouse publisher stages rows, verifies the staged count, replaces exactly
one partition, and verifies the published partition count.

## Validation contract

- The artifact producer records row counts for source documents, contacts, and
  labels; each projection independently checks its written count.
- Fact parsing fails an incomplete partition when an artifact is missing or a
  document fails to parse.
- Every DuckDB file contains a completed `_partition_status` row with expected
  and actual counts.
- ClickHouse publication refuses a mismatched stage or target partition count.
- Re-materializing a processed week replaces only that week and is idempotent.

## Deployment order

1. Stop ESEF schedules and confirm no active ESEF runs.
2. Apply ClickHouse migration `000313`.
3. Deploy Dagster code.
4. Materialize one closed processed week through all five DuckDB and five
   ClickHouse assets.
5. Verify counts, uniqueness, partition isolation, downstream availability,
   and a successful idempotent rerun.
6. Start larger backfills only after the bounded run passes.

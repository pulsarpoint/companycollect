# ESEF filings design

## Purpose

The ESEF source discovers filings from filings.xbrl.org, archives each report
package by content hash, parses the package once with Arelle, and publishes
five reusable datasets to ClickHouse. Downstream features consume the
ClickHouse tables; they do not reopen parser DuckDB files or parse report
packages again.

## Partition clock

All routine assets use the source `processed_at` clock and weekly partitions
starting on Sunday at 00:00 UTC. Fiscal `period_end` is data, not the orchestration
clock. A filing discovered this week is processed this week even when its
financial period ended years ago.

The unpartitioned reconciliation asset occasionally performs a full index
sweep to identify upstream additions and removals. It is not part of the
weekly parsing path.

## Artifact boundary

`esef_document_extraction_manifest_s3` snapshots the filings in a processed
week and attaches resolved company identifiers. `esef_document_artifacts_s3`
then:

- downloads each unique report package;
- verifies and archives it under its SHA-256 digest;
- reuses an existing supported artifact when possible;
- parses missing artifacts with Arelle in recycled worker processes;
- extracts XBRL facts, source-document metadata, contact candidates, taxonomy
  labels, visible sections, and parser quality information; and
- writes one versioned result object for the processed week.

The report package and the versioned artifact are the replayable source. There
is no second raw-fact or rendered-report archive path.

## Independent DuckDB assets

The weekly result object fans out to five independently materializable assets:

| Asset | DuckDB dataset | Purpose |
|---|---|---|
| `esef_source_documents_duckdb` | source documents | Package identity, parser status, company link, counts, provenance |
| `esef_filing_facts_duckdb` | facts | Normalized numeric and text XBRL facts |
| `esef_document_contact_candidates_duckdb` | contact candidates | Auditable email, phone, and website observations |
| `esef_document_concept_labels_duckdb` | taxonomy labels | Extension and standard concept labels by language and role |
| `esef_fact_disclosures_duckdb` | disclosures | Structured blocks and plain text derived directly from narrative facts |

Each asset writes an atomic DuckDB file dedicated to one processed week. No
two assets write the same file, so their Dagster pools allow parallel work.
Every file records its completion contract in
`esef_filings._partition_status`.

## ClickHouse publication

Each DuckDB asset has one matching, weekly partitioned ClickHouse publisher:

- `esef_source_documents_clickhouse`;
- `esef_facts_clickhouse`;
- `esef_document_contact_candidates_clickhouse`;
- `esef_document_concept_labels_clickhouse`; and
- `esef_fact_disclosures_clickhouse`.

A publisher validates the DuckDB completion row, exports to a temporary table,
checks the staged row count, replaces exactly the requested
`processed_week` partition, checks the published row count, and drops the
temporary table. A rerun is therefore idempotent and cannot delete another
week.

Aggregate and enrichment assets depend on the ClickHouse outputs. In
particular, company source records, Swedish company information, and LLM
enrichment consume `esef_source_documents_clickhouse`; official taxonomy
translation consumes `esef_document_concept_labels_clickhouse`; financial
metrics consume `esef_facts_clickhouse`.

## Data quality invariants

- Artifact schema versions are explicitly supported and validated.
- Package SHA-256 is the immutable file identity.
- `source_record_uid` is derived consistently from that file identity.
- Producer row counts must equal DuckDB projection counts.
- A fact partition fails when a required artifact is absent or unparseable.
- DuckDB expected and actual counts must match before publication.
- ClickHouse staged and published counts must match the DuckDB contract.
- The five serving tables are partitioned by `processed_week`.
- Downstream consumers read ClickHouse, never parser-local DuckDB files.

## Operational sequence

Apply ClickHouse migrations before deploying code that targets the resulting
schema. Keep ESEF schedules stopped during a destructive development cutover.
After deployment, materialize one closed week, compare all five DuckDB and
ClickHouse counts, check key uniqueness and source-document coverage, rerun
the same week to prove idempotency, and only then launch a larger backfill.

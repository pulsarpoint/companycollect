# ESEF filings design

## Purpose

The ESEF source discovers filings from filings.xbrl.org, archives each report
package by content hash, parses the package once with Arelle, and publishes
source-owned datasets to ClickHouse. Downstream features consume the ClickHouse
tables; they do not reopen parser DuckDB files or parse report packages again.

## Source ownership boundary

Every asset that acquires, parses, enriches, projects, or publishes information
obtained from ESEF belongs to the Dagster `esef` group. This includes semantic
outputs such as financial metrics and company-information projections even when
they run separately from the weekly parser job. Dagster jobs and dependencies
define execution boundaries; the group defines source ownership.

S3 artifacts and weekly DuckDB files are internal processing contracts. The
source's public boundary is its independently queryable ClickHouse tables.
External reference inputs such as GLEIF and exchange rates remain owned by their
respective source groups and are declared as upstream dependencies. Systems that
consume ESEF ClickHouse tables remain outside the ESEF source boundary.

## Partition clock

All routine assets use the source `processed_at` clock and weekly partitions
starting on Sunday at 00:00 UTC. Fiscal `period_end` is data, not the orchestration
clock. A filing discovered this week is processed this week even when its
financial period ended years ago.

## Artifact boundary

`esef_document_extraction_manifest_s3` snapshots the filings in a processed
week and attaches resolved company identifiers. `esef_document_artifacts_s3`
then:

- downloads each unique report package;
- verifies and archives it under its SHA-256 digest;
- reuses an existing supported artifact when possible;
- parses missing artifacts with Arelle in recycled worker processes;
- extracts XBRL facts, internal document metadata, contact candidates, taxonomy
  labels, visible sections, and parser quality information; and
- writes one versioned result object for the processed week.

The report package and the versioned artifact are the replayable source. There
is no second raw-fact or rendered-report archive path.

## Independent DuckDB assets

The weekly result object fans out to four independently materializable assets:

| Asset | DuckDB dataset | Purpose |
|---|---|---|
| `esef_filing_facts_duckdb` | facts | Normalized numeric and text XBRL facts |
| `esef_document_contact_candidates_duckdb` | contact candidates | Auditable email, phone, and website observations |
| `esef_document_concept_labels_duckdb` | taxonomy labels | Extension and standard concept labels by language and role |
| `esef_disclosures_duckdb` | disclosures | Tagged narrative facts plus visible XHTML sections, with segment, concept, period, page, and anchor provenance |

Each asset writes an atomic DuckDB file dedicated to one processed week. No
two assets write the same file, so their Dagster pools allow parallel work.
Every file records its completion contract in
`esef_filings._partition_status`.

## ClickHouse publication

One non-subsettable multi-asset operation publishes the four weekly ClickHouse
outputs together:

- `esef_facts_clickhouse`;
- `esef_document_contact_candidates_clickhouse`;
- `esef_document_concept_labels_clickhouse`; and
- `esef_disclosures_clickhouse`.

The operation validates every DuckDB completion row, exports to temporary tables,
checks the staged row count, replaces exactly the requested
`processed_week` partition, checks the published row count, and drops the
temporary table. A rerun is therefore idempotent and cannot delete another
week.

Aggregate and enrichment assets depend on the four ClickHouse outputs. Filing
identity and package URLs come from `esef_filings`; official taxonomy translation
consumes `esef_document_concept_labels_clickhouse`; financial metrics consume
`esef_facts_clickhouse`; and `esef_document_company_information_clickhouse`
reconstructs bounded model evidence directly from `esef_disclosures` and concept
labels. The exact model request and response remain content-addressed in S3, but
there is no enrichment DuckDB or second ClickHouse publisher.

## The people pass

The per-filing people pass is a second, independent paid extraction alongside
`esef_document_company_information_clickhouse`. Where that asset enriches only
the latest final document per linked company, the people pass extracts every
eligible filing of every admitted LEI, newest first
(`selection_method: every_filing_per_lei`), one row per `(source_document_id,
model_provider, model_name, prompt_version)`.

- **Asset and job.** `esef_document_people_extraction_clickhouse` depends on
  `esef_disclosures_clickhouse`, `esef_document_concept_labels_clickhouse`, and
  `esef_filings_clickhouse`. It is unpartitioned and launched explicitly through
  `esef_document_people_job`, which chains the extraction with its projection;
  it never runs on the weekly schedule or inside
  `esef_document_company_information_job`.
- **Config.** `EsefPeopleExtractionConfig` requires `provider` and `model` with
  no defaults — a bare "Materialize" fails run-config validation rather than
  silently spending on a default provider, mirroring the company-information
  enrichment's config — plus `prompt_version`, `link_statuses` (default
  `register_verified`), `country_iso2s`, `company_ids`, `source_document_ids`,
  `max_documents`, `refresh_existing`, `max_evidence_chars`,
  `timeout_seconds`, and `concurrency`.
- **Prompt.** `esef-people-v1` is the people sentences of the company-information
  enrichment prompt only, over the `people_and_audit` tagged facts and the six
  people visible sections of one filing, with the same evidence budgets and
  citation rules. Output is the existing `PersonCandidate` list, at most 100.
- **Prefixes.** Artifacts are written under `esef_filings/llm_people_extraction`
  and requests under `esef_filings/llm_people_extraction_requests` (the provider
  segment is always present in both keys) — siblings of the company-information
  enrichment's `esef_filings/llm_company_enrichment` and
  `esef_filings/llm_company_enrichment_requests`.
- **Table.** `esef_document_people_extraction` is a MergeTree keyed
  `(source_document_id, model_provider, model_name, prompt_version)`, one row
  per document per pass, with `source_record_uid` a DEFAULT and `extracted_at`
  a `DateTime64(3, 'UTC')`.
- **Statuses.** `extracted` (a fresh model call), `reused` (the exact request's
  output artifact is already present in the object store), and `no_evidence`.
  An unchanged content-addressed request hash instead skips the document
  entirely before either status is decided — no row is written, and it is
  counted in `unchanged_document_count`, not `reused`. A failed call is
  likewise logged and counted but writes no row.
- **Reuse rule.** Same as the company-information enrichment: an unchanged
  content-addressed request hash (matching the document's currently stored
  `existing_request_sha256`) skips the document entirely — no row is written,
  counted as `unchanged_document_count`. When the hash has changed, the pass
  still avoids the paid call if that exact request's output artifact is
  already in the object store, writing a row with status `reused` instead.
  Only the pass's own config (provider, model, prompt version, evidence
  bounds) changes what counts as unchanged.
- **Projection and identity.** `esef_document_people_clickhouse` REPLACES its
  table (stage table plus `EXCHANGE TABLES`) from
  `esef_document_people_extraction` alone — it never appends, because a rerun
  of the same document under the same prompt and model must not accumulate
  duplicate rows. Row identity is `(lei, fiscal_year, source_record_uid,
  candidate_uid)`, where `candidate_uid` is the company-source-record
  observation hash over `source_record_uid`, `esef_person`, the prompt version,
  the normalised person name, and the role category; status preference is
  current, then historical, then unclear.
- **`people_json` on the enrichment.** The `people_json` column produced by
  `esef_document_company_information_clickhouse` is retained only as an
  artifact of that pass; no projection reads it any longer.
  `esef_document_people` is sourced exclusively from
  `esef_document_people_extraction`. The business-items and
  group-relationships projections are unaffected and still append from the
  company-information enrichment.

## Data quality invariants

- Artifact schema versions are explicitly supported and validated.
- Package SHA-256 is the immutable file identity.
- `source_record_uid` is derived consistently from that file identity.
- Producer row counts must equal DuckDB projection counts.
- A fact partition fails when a required artifact is absent or unparseable.
- DuckDB expected and actual counts must match before publication.
- ClickHouse staged and published counts must match the DuckDB contract.
- The four parsing serving tables are partitioned by `processed_week`.
- Downstream consumers read ClickHouse, never parser-local DuckDB files.

## Country-agnostic products (2026-09-09)

ESEF products are country-agnostic and keyed by LEI and document only. Tables
do not carry `country_iso2`, `country_code`, or `company_id`; those belong in
the identity registry layer.

The `esef_entity_registry_map` carries `link_status` (`register_verified`,
`unverified`, or `gleif`) reflecting verification against the registers of
`COUNTRY_IDENTITY_RULES`. Resolved `company_id`, `country_iso2`, and
`link_status` live in that map; products read them on-demand per LEI.

For Sweden, eight country-specific views (`se_esef_filings`, `se_esef_facts`,
`se_esef_disclosures`, `se_esef_document_contact_candidates`,
`se_esef_document_company_information`, `se_esef_document_people`,
`se_esef_document_business_items`, `se_esef_document_group_relationships`)
expose `company_id` by joining the map. These views are the surface for Swedish
consumers; the country-agnostic products remain the ClickHouse tables of record.

A consumer never writes `FINAL` after a view name: the view already reads its
underlying ReplacingMergeTree product `FINAL` (`se_esef_filings`, `se_esef_facts`,
`se_esef_document_people`, `se_esef_document_business_items`, and
`se_esef_document_group_relationships` are FINAL reads inside the view;
`se_esef_disclosures`, `se_esef_document_contact_candidates`, and
`se_esef_document_company_information` are plain MergeTree products and need
none). Writing `FINAL` a second time after the view name is redundant at best
and a ClickHouse error at worst.

## Operational sequence

Apply ClickHouse migrations before deploying code that targets the resulting
schema. Keep ESEF schedules stopped during a destructive development cutover.
After deployment, materialize one closed week, compare all four DuckDB and
ClickHouse counts, check key uniqueness and source-document coverage, rerun
the same week to prove idempotency, and only then launch a larger backfill.

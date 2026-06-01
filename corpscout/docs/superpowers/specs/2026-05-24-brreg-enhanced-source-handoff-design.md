# BRREG Enhanced Source Handoff Design

Date: 2026-05-24

Status: Proposed and approved for planning. This supersedes the narrower BRREG Dagster domain-enrichment design from 2026-05-24.

Companion schema document: `docs/superpowers/specs/2026-05-24-brreg-normalized-source-tables-design.md`.

## Summary

Split BRREG enrichment into two clear responsibilities:

```text
brreg_company_raw_inputs
  -> Dagster BRREG enrichment graph
  -> brreg_enhanced_raw_inputs
  -> Corpscout BRREG source unpacker
  -> normalized BRREG source tables
  -> Corpscout suggestions and review
  -> canonical companies
```

Dagster owns the messy source-specific enrichment process. It receives BRREG raw input records and writes a durable enhanced JSON document to `brreg_enhanced_raw_inputs`.

Corpscout owns the database schema, the normalized BRREG source tables, suggestion creation, review, approval, and canonical companies. Corpscout unpacks a versioned enhanced BRREG document into source tables, then creates suggestions from those normalized source facts.

## Why This Split

The previous design treated domain enrichment as a standalone Dagster slice. That is useful for a narrow MVP, but it creates the same coordination problem again when translation, financial extraction, source-specific registry enrichment, and optional company research are added.

The better boundary is the enhanced source document:

- Dagster can orchestrate translation, domain discovery, financial fetching, retries, dependencies, and partial failures in one graph.
- Corpscout gets a durable, replayable artifact instead of needing to track every enrichment step directly.
- Normalized source tables can be rebuilt from enhanced JSON when the Corpscout schema changes.
- Suggestion generation stays in Corpscout, where review and approval already live.
- Each source can have different enrichment logic while still handing Corpscout the same kind of artifact: a versioned enhanced source document.

## Ownership

Dagster owns:

- selecting BRREG raw inputs to enrich;
- running BRREG-specific enrichment steps;
- deciding step order and retry behavior;
- calling Temporal workflows when durable bounded work is useful;
- producing the enhanced BRREG JSON document;
- writing enhancement status, run metadata, and safe error summaries to `brreg_enhanced_raw_inputs`.

Corpscout owns:

- importing BRREG raw inputs;
- storing raw inputs and enhanced artifacts in Corpscout Postgres;
- defining normalized BRREG source tables;
- unpacking enhanced BRREG JSON into those source tables;
- creating company suggestions from source tables;
- review, approval, rejection, and canonical company updates;
- showing source facts, evidence, domains, financial data, and suggestion status in the UI.

Temporal may still own bounded execution work:

- domain discovery batches;
- external calls that need worker-level retry and timeout behavior;
- existing Go workflow reuse where it is simpler than rewriting immediately.

Temporal does not own source-level orchestration or product state. Dagster owns that layer.

## Storage Contract

The concrete table definitions live in `docs/superpowers/specs/2026-05-24-brreg-normalized-source-tables-design.md`.

That document defines:

- `brreg_enhanced_raw_inputs`, the Dagster write target;
- `brreg_source_companies`, the root normalized BRREG source table;
- `brreg_source_addresses`;
- `brreg_source_industries`;
- `brreg_source_capital`;
- `brreg_source_domains`;
- `brreg_source_financials`.

The storage contract is:

- Dagster writes only enhanced artifacts.
- Corpscout writes normalized source tables from enhanced artifacts.
- Suggestions read normalized source tables.
- Canonical companies are mutated only by Corpscout approval flows.

## Enhanced JSON Shape

The enhanced payload should be explicit about section outcomes:

```json
{
  "schema_version": "brreg.enhanced.v1",
  "source": {
    "status": "succeeded",
    "organization_number": "810202572",
    "payload_hash": "..."
  },
  "translation": {
    "status": "succeeded",
    "payload_en": {}
  },
  "domains": {
    "status": "succeeded",
    "items": []
  },
  "financials": {
    "status": "not_available",
    "items": []
  },
  "metadata": {
    "dagster_run_id": "...",
    "started_at": "...",
    "finished_at": "..."
  }
}
```

Valid section statuses:

- `not_done`
- `running`
- `succeeded`
- `failed`
- `not_available`
- `skipped`

This avoids overloading one row state with many independent facts. A company can be translated and domain-enhanced while financial extraction is still missing or unavailable.

## Corpscout Unpacker

The unpacker is a Corpscout-owned job or service that reads successful or partially successful enhanced artifacts and writes normalized BRREG source tables.

Rules:

- Process one enhanced artifact in a database transaction where practical.
- Make writes idempotent by `raw_input_id`, `organization_number`, `payload_hash`, and `enhancement_version`.
- Preserve enough metadata to know which enhanced artifact produced each normalized row.
- Mark older source rows as superseded or rebuild tables from enhanced artifacts during MVP.
- Do not create suggestions inside Dagster.
- Do not approve or mutate canonical companies from the unpacker.

For MVP, rebuildable tables are acceptable because raw inputs and enhanced artifacts remain durable.

## Suggestion Flow

Corpscout suggestion creation should read from normalized BRREG source tables, not directly from raw payloads or Dagster internals.

Initial flow:

```text
brreg_source_companies and related tables
  -> suggestion builder
  -> company suggestions
  -> review
  -> approved canonical companies and company-source links
```

This keeps suggestion logic close to Corpscout review semantics and avoids making Dagster responsible for product decisions.

## UI Implications

BRREG source views should eventually show:

- raw input lifecycle and diagnostics;
- latest enhanced artifact status;
- translation outcome;
- domain candidates and manual source-domain additions;
- financial facts;
- normalized BRREG facts;
- suggestion status and review links.

The raw input table can keep showing useful operational state, but product-facing source facts should move toward the normalized BRREG source tables.

## First Task

The next implementation plan should start with the schema document:

1. Create migration `000051_brreg_enhanced_source_tables`.
2. Add the `brreg_enhanced_raw_inputs` Dagster handoff table.
3. Add the normalized BRREG source tables listed in the companion schema document.
4. Add shape tests for constraints and indexes.
5. Add sqlc read queries for source-company detail and list views.

Workflow implementation should wait until this table layer exists.

## Non-Goals

- Do not build a generic cross-source enhanced schema yet.
- Do not force CVR, Ariregister, or GLEIF into this design until BRREG proves it.
- Do not make Dagster write canonical companies.
- Do not remove existing BRREG raw input action UI in the same change.
- Do not require every enrichment section to succeed before Corpscout can use the record.

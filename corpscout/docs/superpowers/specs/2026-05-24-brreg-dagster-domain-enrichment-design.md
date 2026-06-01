# BRREG Dagster Domain Enrichment MVP Design

Status: Superseded by `docs/superpowers/specs/2026-05-24-brreg-enhanced-source-handoff-design.md`.

This document describes an earlier domain-only Dagster slice. The current direction is to have Dagster produce a versioned enhanced BRREG source document, then let Corpscout unpack that document into normalized BRREG source tables.

Date: 2026-05-24

## Summary

Move the first slice of BRREG company enrichment into a Dagster-owned asset graph while keeping Corpscout as the raw-ingestion, review, suggestion, and approval product.

The MVP proves this path only for BRREG normalized source records and domain enrichment:

```text
brreg_company_raw_inputs
  -> brreg_source_companies
  -> Dagster domain enrichment asset
  -> Temporal domain discovery batch
  -> brreg_source_domain_observations
  -> brreg_source_company_domains
  -> Corpscout BRREG source detail/list view
```

Dagster becomes the high-level orchestrator for BRREG enrichment state. Temporal remains useful as a durable execution engine for bounded external work. Corpscout should not keep accumulating source-specific task state machinery for every enrichment step.

## Goals

- Preserve `brreg_company_raw_inputs` as the rebuildable source of truth for imported BRREG data.
- Add normalized BRREG source tables that can be truncated and rebuilt during MVP.
- Let Dagster decide which BRREG source companies need domain enrichment.
- Reuse the existing Temporal domain-discovery workflow for bounded batches if it remains useful.
- Store enrichment outputs, evidence, run status, and normalized domains in Corpscout Postgres.
- Let Corpscout read enriched source tables and later create suggestions from them.

## Non-Goals

- Do not migrate translation, financial extraction, or suggestion creation in this MVP slice.
- Do not make Dagster create or approve Corpscout suggestions.
- Do not preserve old BRREG per-action history as a product contract.
- Do not make Dagster a data store. Dagster orchestrates; Corpscout Postgres stores source and enrichment data.

## Ownership Boundaries

Corpscout owns:

- BRREG raw input ingestion.
- Normalized/enriched source table schema in Corpscout Postgres.
- Source record list/detail UI.
- Suggestion creation, review, approval, and canonical `companies`.
- Canonical `company_source_links`.

Dagster owns:

- BRREG enrichment asset graph.
- Selecting source records for enrichment.
- Deciding step readiness and retry scope.
- Starting and waiting for Temporal workflows.
- Writing enrichment run status and materialization metadata.
- Materializing BRREG normalized domain outputs from observations.

Temporal owns:

- Durable execution of one bounded external-work batch.
- Internal retries for domain discovery.
- Writing raw domain observations if workflow result payloads would be too large.

## Data Model

Add BRREG-specific normalized and enrichment tables. The exact column set can be refined during implementation, but the contract is:

### `brreg_source_companies`

One current normalized BRREG source record per organization number and payload hash, rebuildable from `brreg_company_raw_inputs`.

Key fields:

- `id`
- `raw_input_id`
- `organization_number`
- `name`
- `registration_status`
- `country_iso2`
- `payload_hash`
- `source_updated_at`
- `normalized_payload`
- `created_at`
- `updated_at`

### `brreg_enrichment_runs`

Tracks Dagster-owned enrichment attempts.

Key fields:

- `id`
- `dagster_run_id`
- `asset_key`
- `step`
- `scope`
- `status`
- `started_at`
- `finished_at`
- `error`
- `metadata`

Valid MVP statuses:

- `queued`
- `running`
- `succeeded`
- `failed`
- `cancelled`

### `brreg_source_domain_observations`

Raw domain findings from Temporal/domain discovery. These are observations, not approved company domains.

Key fields:

- `id`
- `source_company_id`
- `raw_input_id`
- `enrichment_run_id`
- `domain`
- `signal`
- `confidence`
- `evidence`
- `metadata`
- `created_at`

### `brreg_source_company_domains`

Normalized current BRREG source-domain connections materialized from observations.

Key fields:

- `id`
- `source_company_id`
- `domain`
- `best_signal`
- `confidence`
- `status`
- `evidence`
- `first_seen_at`
- `last_seen_at`

Valid MVP statuses:

- `active`
- `superseded`
- `rejected`

## Dagster Asset Flow

### 1. BRREG Source Company Normalization

Asset reads `brreg_company_raw_inputs` and materializes `brreg_source_companies`.

For MVP, this asset can be rebuildable:

```text
truncate brreg_source_company_domains
truncate brreg_source_domain_observations
truncate brreg_source_companies
rebuild from brreg_company_raw_inputs
```

The rebuild behavior is acceptable because this is an MVP and raw inputs remain the source of truth.

### 2. BRREG Domain Enrichment Selection

Dagster selects BRREG source companies needing domain enrichment. Initial criteria:

- company has no active `brreg_source_company_domains`, or
- last domain enrichment run for that source company failed, or
- user requested a retry/rebuild.

Dagster should batch selected companies, with an initial batch cap of 500 records.

### 3. Temporal Domain Discovery Execution

For each batch, Dagster starts a Temporal workflow equivalent to the current `EnrichCompanyDomains` behavior.

Input should include:

- source: `brreg`
- country: `NO`
- source company ids
- raw input ids
- organization numbers
- names
- enrichment run id
- force/retry flag

Temporal performs external discovery and writes raw observations to `brreg_source_domain_observations` if results are too large to return safely. If result payloads are small enough, Dagster may receive results directly and write observations itself.

### 4. Domain Materialization

After Temporal completes, Dagster materializes `brreg_source_company_domains` from observations.

Rules for MVP:

- normalize domains to lowercase host/domain form;
- keep one current source-domain row per `source_company_id + domain`;
- choose best confidence and signal when multiple observations support the same domain;
- preserve evidence in JSONB.

### 5. Run Status

Dagster writes `brreg_enrichment_runs` status transitions:

```text
queued -> running -> succeeded
queued -> running -> failed
queued -> cancelled
```

Failure should include a safe error summary and structured metadata. Detailed worker errors stay in Dagster/Temporal logs.

## Corpscout UI/API Flow

Corpscout should initially add read-only visibility:

- BRREG normalized source company list/detail.
- Domain enrichment status summary from `brreg_enrichment_runs`.
- Connected source domains from `brreg_source_company_domains`.
- Raw observations/evidence on detail pages when useful.

Existing raw input pages can remain during the MVP, but product UI should start shifting away from raw action state as the primary lifecycle model.

Later, Corpscout will add:

- `Enrich selected` action that triggers Dagster, not per-step River/Temporal buttons.
- `Create suggestions` action that reads ready BRREG source records and writes Corpscout-owned suggestion tables.

## Suggestion Boundary

Dagster must not create suggestions directly.

The future suggestion path is:

```text
brreg_source_companies
brreg_source_company_domains
brreg enrichment summary
  -> Corpscout suggestion builder
  -> company_suggestions and child suggestions
  -> human approval
  -> companies and company_source_links
```

The first bridge should be a Corpscout-owned view:

```text
v_brreg_company_suggestion_candidates
```

This view is outside the domain-enrichment MVP unless needed for a smoke test.

## Error Handling

- Dagster records step-level failures in `brreg_enrichment_runs`.
- Temporal handles retries inside one bounded batch.
- Dagster decides whether a failed source record should be retried.
- Corpscout displays failure status and a safe error summary.
- Detailed logs and stack traces stay in worker logs, not API responses.

## Testing

Database:

- migration tests for the new BRREG source/enrichment tables;
- rebuild test proving raw inputs can repopulate normalized BRREG companies.

Dagster:

- unit test for BRREG raw-to-source normalization;
- test that domain enrichment selection excludes already enriched source companies;
- test that failed companies can be selected for retry;
- test that Temporal results or observations materialize source domains.

Scheduler/Temporal integration:

- test Dagster-triggered Temporal input shape if the existing workflow is reused;
- test domain observations are connected to the correct BRREG source company and raw input.

Corpscout API/UI:

- test BRREG normalized list/detail endpoints;
- smoke test detail page shows normalized source fields, enrichment status, and domains.

## Rollout

1. Add BRREG source/enrichment tables.
2. Add Dagster project or module for Corpscout enrichment.
3. Implement BRREG normalization asset.
4. Add read-only Corpscout API/UI for normalized BRREG records.
5. Implement Dagster domain enrichment asset using Temporal for batches.
6. Materialize BRREG source domains.
7. Hide or de-emphasize the old BRREG domain action UI once Dagster path is usable.

## Initial Implementation Choices

- Temporal should return domain discovery results to Dagster for the first implementation. If payload size becomes unsafe, switch that single boundary so Temporal writes observations directly.
- The first normalized BRREG child tables are only companies and domains. Addresses, industries, translation, and financials come later.
- The first Dagster trigger is manual. Add a completed-pull-run sensor or Corpscout `Enrich selected` trigger after the asset graph is proven.

For the MVP, prefer the simplest implementation that proves the architecture: manual Dagster run, Temporal returns small results when possible, and direct observation writes only if payload size requires it.

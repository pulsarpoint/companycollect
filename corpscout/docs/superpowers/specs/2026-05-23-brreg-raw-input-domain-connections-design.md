# BRREG Raw Input Domain Connections Design

## Summary

Add an explicit BRREG raw-input-to-domain connection model so domain enhancement can attach discovered domains to the exact raw input that produced the evidence. The connection is not an approved company-domain relationship yet. It is raw evidence that can later be turned into `suggestion_company_domains` when the BRREG raw input is submitted for review.

## Goals

- Store domains discovered for a specific `brreg_company_raw_inputs` row.
- Show a DB-backed connected domain count in the BRREG raw-input table.
- Show connected domains in the BRREG raw-input detail sheet.
- Let operators manually add a domain connection from the raw-input detail sheet.
- Let operators remove a connection without losing audit/history.
- Include active discovered and manual domain links when BRREG suggestions are submitted.

## Non-Goals

- Do not create approved `company_domains` directly from raw input enrichment.
- Do not generalize the bridge table to all raw input sources in this change.
- Do not require an approved company to exist before linking raw input evidence to domains.
- Do not expose hard-delete behavior for operator removals.

## Data Model

Create a BRREG-specific bridge table:

```sql
brreg_raw_input_domains (
  id uuid primary key default gen_random_uuid(),
  raw_input_id uuid not null references brreg_company_raw_inputs(id) on delete cascade,
  domain_id uuid not null references domains(id),
  action_id uuid references brreg_raw_input_actions(id) on delete set null,
  signal text not null,
  confidence smallint not null check (confidence between 1 and 100),
  status text not null default 'active' check (status in ('active', 'removed')),
  metadata jsonb not null default '{}'::jsonb check (jsonb_typeof(metadata) = 'object'),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  removed_at timestamptz,
  removed_by text,
  unique (raw_input_id, domain_id, signal)
)
```

Indexes:

- `(raw_input_id, status)` for detail lookups and counts.
- `(domain_id, status)` for reverse lookup.
- `(action_id)` where action linkage is present.

Signal values should match current source semantics where possible:

- Enhancement signals: `wikidata`, `certsh`, `search`, `heuristic`.
- Operator signal: `manual`.

`metadata` stores source-specific details such as discovery provider, raw evidence, operator notes, workflow/action IDs, and timestamps. Manual links should include enough metadata to identify that an operator created the connection.

## Domain Persistence Flow

Domain enhancement should continue to upsert canonical rows into `domains`. After the domain row exists, the writer inserts or updates `brreg_raw_input_domains`.

For each discovery:

1. Resolve the BRREG raw input by `organization_number` / native ID.
2. Upsert the normalized domain into `domains`.
3. Insert an active bridge row with action ID, signal, confidence, and discovery metadata.
4. On conflict, update confidence, metadata, `action_id`, `updated_at`, and keep the row active unless the existing row was manually removed and the enhancement run is not forced.

Soft-removed rows should not be silently reactivated by ordinary enhancement. A force enhancement may reactivate them, but the metadata must preserve that this was a reactivation.

## Manual Operator Flow

The BRREG raw-input detail sheet gets a connected domains section.

Operators can:

- Add a domain manually.
- See active connected domains.
- Remove a connection.

Manual add behavior:

1. Normalize and validate the domain.
2. Upsert into `domains` with `import_source='manual_upload'`.
3. Insert or reactivate a `brreg_raw_input_domains` row with `signal='manual'`.
4. Use confidence `100`.
5. Store operator metadata in `metadata`.

Removal behavior:

- Set `status='removed'`, `removed_at=now()`, `removed_by=<operator>`, `updated_at=now()`.
- Do not delete the domain row.
- Do not delete the connection row.

## Submission Flow

When `BrregProcessor` submits suggestions for a raw input, it should include active `brreg_raw_input_domains` rows as `suggestion_company_domains`.

Mapping:

- `domain` comes from `domains.domain`.
- `signal` maps from raw-input-domain signal into the suggestion domain signal contract.
- `signal_confidence` comes from the bridge confidence.
- `evidence` includes bridge metadata and raw input/action IDs.
- `relationship_type` defaults to `candidate`.
- `domain_status` defaults to `needs_review`.

This makes both enhancement-discovered and manually added domains part of the normal suggestion review process.

Signal mapping for `suggestion_company_domains`:

- `manual` -> `manual_import`
- `heuristic` -> `search`
- `wikidata`, `certsh`, `whois`, and `search` keep their values.

## API

Add API support for BRREG raw input domain connections:

- `GET /api/v1/raw-inputs/brreg/{id}` includes `connected_domains`.
- `POST /api/v1/raw-inputs/brreg/{id}/domains` adds or reactivates a connection.
- `POST /api/v1/raw-inputs/brreg/{id}/domains/{connection_id}/remove` soft-removes a connection.

The raw input list API should expose `connected_domain_count` for BRREG rows. It should be computed in SQL from active `brreg_raw_input_domains` rows, not derived in the UI.

## UI

BRREG raw input table:

- Add a compact `Domains` column with the active connected domain count.
- Keep the value sortable/filterable later, but initial scope only needs display.

BRREG raw input detail sheet:

- Show connected active domains with domain, signal, confidence, status, and metadata preview.
- Add a manual domain input/action.
- Add a remove action per connection.
- Keep raw payload and action-status sections visible as they are today.

Removed connections do not need to show by default. They can be added later as an audit/history expansion if needed.

## Data Pipeline Changes

`WriteDiscoveredDomains` currently writes to the domain relationship layer. It should be changed for BRREG enhancement to write raw-input domain evidence instead:

- Accept enough information to connect discovery results to raw input rows.
- Upsert canonical `domains`.
- Insert/update `brreg_raw_input_domains`.
- Preserve BRREG action event behavior from the existing enhancement workflow.

The existing action status (`enhance`) remains the lifecycle signal for whether a raw input has been enhanced. The connected domain count is evidence output, not the action status itself.

## Tests

Scheduler/API:

- Migration test for table constraints and indexes.
- Raw input list test that `connected_domain_count` uses active bridge rows.
- Raw input detail test that connected domains are returned.
- Manual add test for upsert/reactivation.
- Remove test for soft removal.
- Processor test that active raw-input domains become `suggestion_company_domains`.

Data pipelines:

- `WriteDiscoveredDomains` test for BRREG raw-input-domain inserts.
- Test that ordinary enhancement does not reactivate a removed connection.
- Test that forced enhancement can reactivate with metadata.
- Workflow test preserving enhancement action status updates.

UI:

- Typecheck and build.
- Browser smoke test on `/sources/brreg/raw_input`: domain count visible, detail shows connected domains, manual add/remove controls render.

## Fixed Defaults

- Operator identity uses the existing frontend convention, `ops`, until authenticated user identity is available.
- Force reactivation metadata includes `reactivated: true`, `reactivated_at`, and `reactivated_by`.

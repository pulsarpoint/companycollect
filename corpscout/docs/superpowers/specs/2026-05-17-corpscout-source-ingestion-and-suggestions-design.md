# Corpscout Source Ingestion And Suggestions Design

## Goal

Design the Corpscout pipeline for pulling external data, storing raw source-specific input, processing that input, and creating reviewable suggestions for resolved entities.

The pipeline must support many independent sources with different schedules, source markers, payload shapes, confidence rules, and processors. CPE and CVE are only two sources. They must not become the universal ingestion model.

The intended flow is:

```text
source configuration and sync state
-> source pull run
-> source-specific raw input table or source-compatible input schema
-> source-specific processor
-> entity/root suggestions and section suggestions
-> review approval or rejection
-> resolved company, organization, or open-source project tables
```

## Related Design

This document extends:

- `docs/superpowers/specs/2026-05-16-corpscout-resolved-entity-tables-design.md`

That resolved-entity design established these decisions:

- Corpscout has three resolved root entity types: `companies`, `organizations`, and `open_source_projects`.
- There is no generic `entries` table.
- There is no generic `identity_observations` table.
- Source-specific candidate and suggestion tables should be used instead.
- CPE/CVE approved links are compact operational mapping tables, while evidence and review state live in suggestion tables.

## Scope

This design covers:

- source configuration and pull state
- source pull run metadata
- source-specific raw input tables
- raw input processing lifecycle
- new entity suggestions
- existing entity field or section suggestions
- linking suggestions back to source inputs
- approval and rejection workflow
- deduplication and idempotency rules
- scheduling and processing boundaries
- UI review grouping rules
- testing requirements

## Non-Goals

- Do not design a generic raw input table for all sources.
- Do not design a generic `identity_observations` table.
- Do not write source-derived data directly into resolved entity/profile tables from pullers or processors.
- Do not force all sources to use the same payload columns.
- Do not force companies, organizations, and open-source projects to have identical suggestion sections.
- Do not implement external consumer integrations in this phase.
- Do not design the phase-two CPE lookup API contract here.

## Core Decisions

### Source Inputs Are Source-Specific

Every external source that stores raw data should own a specific input table.

Examples:

- NVD/CVE compatibility tables such as `nvds`, `nvd_descriptions`, `nvd_references`, `nvd_config_nodes`, and `nvd_config_match_criteria`
- CPE compatibility tables such as `cpe_dictionary`, `cpe_match_criteria`, and `cpe_match_criteria_names`
- `gleif_company_raw_inputs`
- `company_registry_raw_inputs`
- `github_owner_raw_inputs`
- `domain_discovery_raw_inputs`
- `website_crawl_raw_inputs`
- `ai_company_profile_raw_inputs`
- `manual_research_raw_inputs`

Each table can keep source-native fields that matter for that source. A GLEIF input table should keep LEI and registry metadata. A GitHub input table should keep owner, repository, organization, and API response metadata. An AI profile input table should keep the prompt version, model, normalized website, and response payload.

For sources that already have a mature importer in backoffice-v2, prefer schema-compatible source tables and the same loader contract over inventing a new Corpscout-only raw shape. For CPE and CVE specifically, do not introduce simplified `cpe_raw_inputs` or `cve_raw_inputs` tables in the first implementation. Mirror the loader-owned NVD/CPE schema used by backoffice-v2 and let Corpscout processors read from those tables to create entity-link suggestions.

### Pull State Is Shared Operational Metadata

Scheduling and last-pulled source markers are operational concerns. They can be stored in shared source state tables because they describe the puller, not the identity data.

Use shared tables for:

- `data_sources`, which stores source definitions and source sync state
- `source_pull_runs`, which stores pull audit rows
- `source_processor_states`, added by this design only for processors that need independent progress markers

River owns task claiming and execution. Corpscout should not duplicate River's job locking with source-level lease columns. If a source must not have two active pull jobs, enqueue it with a River uniqueness rule based on the source name and task type.

Do not use shared tables for raw source payloads.

### Processing Reads From The Source Input Table

Each source has one or more processors. A processor reads only its source-specific input table, interprets raw payloads, and creates suggestions.

Processors must not directly mutate resolved entity tables. The only allowed processor output is reviewable suggestions plus source links. Review approval services are the only application layer that may write to resolved company, organization, or open-source project tables.

This applies to existing sources as well as new sources. Existing registry workers such as `SourceCrawlWorker` must be refactored away from direct upserts into `companies`, `company_locations`, `company_phones`, `company_emails`, `company_aliases`, `company_sources`, `company_domains`, and similar resolved tables. The new flow is:

```text
River pull task
-> source-specific raw input table or compatibility schema
-> processor
-> root and section suggestions
-> suggestion_source_links
-> reviewer approval
-> resolved entity writes
```

There is no mixed mode in the MVP target. Old source workers, new source workers, crawlers, and processors must all route source-derived company/profile changes through suggestions before the MVP is considered complete.

Approval services may still write to resolved tables after review. That is not considered source ingestion; it is the explicit reviewed mutation boundary.

### SourceCrawlWorker MVP Refactor Scope

The current `SourceCrawlWorker` direct-write body should be replaced, not preserved. MVP uses two River responsibilities:

- `SourcePullWorker` calls the external crawler or source adapter, creates a `source_pull_runs` row, writes only source-specific raw input rows or compatibility-schema rows, updates successful source markers, and enqueues the configured `processor_task_type`.
- `SourceProcessWorker` reads pending raw input rows for one source or scans compatibility tables from `source_processor_states`, creates root and section suggestions, writes `suggestion_source_links`, and marks raw input rows or processor state as processed, ignored, or failed.

Pulling and processing may share packages and normalization helpers, but they should be separate River task types in the MVP. This gives retries, monitoring, and failure handling a clear boundary. The one-job pull-and-process option is not used for the first implementation.

Existing helper paths that upsert `companies`, `company_locations`, `company_phones`, `company_emails`, `company_aliases`, `company_sources`, or `company_domains` should be removed from source ingestion. Equivalent data becomes `company_suggestions` or section suggestions.

### Suggestions Are Section-Specific

Suggestion tables should be specific to the entity type and profile section they update.

Examples:

- `company_suggestions`
- `company_domain_suggestions`
- `company_contact_suggestions`
- `company_financial_suggestions`
- `company_relationship_suggestions`
- `company_status_suggestions`
- `organization_suggestions`
- `organization_contact_suggestions`
- `open_source_project_suggestions`
- `open_source_project_repository_suggestions`

A new entity suggestion is represented by a root suggestion row plus optional child section suggestion rows. Existing entity updates are represented by section suggestion rows that point directly to the existing entity.

## Source Operational Tables

### `data_sources`

MVP uses a clean source registry. The table name can remain `data_sources`, but the implementation does not need to preserve the existing table shape, rows, direct-write worker behavior, or legacy scheduler compatibility. The migration should drop and recreate the old source-ingestion tables when that is simpler than transforming them safely.

```sql
CREATE TABLE data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE NOT NULL,
    display_name TEXT,
    description TEXT,
    source_group TEXT NOT NULL,
    input_table_name TEXT NOT NULL,
    pull_task_type TEXT NOT NULL,
    processor_task_type TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    schedule_kind TEXT NOT NULL DEFAULT 'manual',
    schedule_expression TEXT,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_started_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ,
    last_source_marker_type TEXT,
    last_source_marker TEXT,
    last_source_modified_at TIMESTAMPTZ,
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_data_sources_source_group CHECK (
        source_group IN (
            'security_identifier',
            'registry',
            'domain',
            'website',
            'github',
            'ai_research',
            'manual',
            'other'
        )
    ),
    CONSTRAINT chk_data_sources_schedule_kind CHECK (
        schedule_kind IN ('manual', 'interval', 'cron', 'event')
    ),
    CONSTRAINT chk_data_sources_marker_pair CHECK (
        (last_source_marker_type IS NULL AND last_source_marker IS NULL)
        OR (last_source_marker_type IS NOT NULL AND last_source_marker IS NOT NULL)
    ),
    CONSTRAINT chk_data_sources_failures CHECK (consecutive_failures >= 0),
    CONSTRAINT chk_data_sources_config_object CHECK (
        jsonb_typeof(config) = 'object'
    )
);
```

Rules:

- `data_sources.name` stays the stable source name used in logs, runs, and UI.
- `input_table_name` points to the source-specific raw input table or the primary table in a source-compatible schema.
- `pull_task_type` is the River task type used to pull the source.
- `processor_task_type` is the River task type used to process pulled input. MVP active sources should use a separate processor task when this field is set.
- `config` remains the place for safe source-specific configuration and documentation metadata. Do not store secrets.
- Pullers should write `last_source_marker_type`, `last_source_marker`, and `last_source_modified_at` only after a successful pull.
- The marker can be an ETag, checksum, feed version, last modified timestamp string, or another source-native version token.
- Full-refresh sources can leave marker fields empty.
- River task uniqueness prevents duplicate active pull jobs for the same source.
- `last_error` must be safe for operators and must not include secrets.

Seed the MVP source registry explicitly. Existing source rows do not need to be preserved.

```sql
INSERT INTO data_sources (
    name,
    display_name,
    source_group,
    input_table_name,
    pull_task_type,
    processor_task_type,
    enabled,
    schedule_kind,
    schedule_expression,
    config
)
VALUES
    ('gleif', 'GLEIF', 'registry', 'gleif_company_raw_inputs', 'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('companies_house', 'UK Companies House', 'registry', 'companies_house_company_raw_inputs', 'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('brreg', 'Brreg', 'registry', 'brreg_company_raw_inputs', 'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('ai_company_profile', 'AI Company Profile', 'ai_research', 'ai_company_profile_raw_inputs', 'ai_company_profile_pull', 'source_process', true, 'manual', NULL, '{}'::jsonb),
    ('nvd_cpe', 'NVD CPE', 'security_identifier', 'cpe_dictionary', 'nvd_cpe_sync', 'nvd_cpe_process', false, 'interval', '24h', '{}'::jsonb),
    ('nvd_cve', 'NVD CVE', 'security_identifier', 'nvds', 'nvd_cve_sync', 'nvd_cve_process', false, 'interval', '6h', '{}'::jsonb)
ON CONFLICT (name) DO UPDATE SET
    display_name = EXCLUDED.display_name,
    source_group = EXCLUDED.source_group,
    input_table_name = EXCLUDED.input_table_name,
    pull_task_type = EXCLUDED.pull_task_type,
    processor_task_type = EXCLUDED.processor_task_type,
    enabled = EXCLUDED.enabled,
    schedule_kind = EXCLUDED.schedule_kind,
    schedule_expression = EXCLUDED.schedule_expression,
    config = EXCLUDED.config,
    updated_at = now();
```

### `source_pull_runs`

MVP uses a clean pull-run audit table. It does not need to preserve the existing `source_pull_runs` column names or statuses.

```sql
CREATE TABLE source_pull_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    river_job_id BIGINT,
    task_type TEXT NOT NULL,
    trigger_type TEXT NOT NULL DEFAULT 'scheduled',
    status TEXT NOT NULL DEFAULT 'running',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    rows_seen INTEGER NOT NULL DEFAULT 0,
    raw_rows_inserted INTEGER NOT NULL DEFAULT 0,
    raw_rows_updated INTEGER NOT NULL DEFAULT 0,
    raw_rows_unchanged INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_source_pull_runs_trigger_type CHECK (
        trigger_type IN ('scheduled', 'manual', 'retry', 'backfill', 'event')
    ),
    CONSTRAINT chk_source_pull_runs_status CHECK (
        status IN ('running', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT chk_source_pull_runs_counts CHECK (
        rows_seen >= 0
        AND raw_rows_inserted >= 0
        AND raw_rows_updated >= 0
        AND raw_rows_unchanged >= 0
    ),
    CONSTRAINT chk_source_pull_runs_metadata_object CHECK (
        jsonb_typeof(metadata) = 'object'
    )
);

CREATE INDEX idx_source_pull_runs_source_started
    ON source_pull_runs(source_id, started_at DESC);
```

Rules:

- Every pull attempt creates a run row.
- `source_id` continues to reference `data_sources(id)`.
- `river_job_id` links the audit row to the River job when available.
- `task_type` records the River task type that performed the pull.
- Corpscout-owned raw input rows inserted by the pull should reference `source_pull_runs.id`.
- Compatibility schemas that cannot add a pull-run foreign key should record counts and source artifact details on `source_pull_runs.metadata`.
- A failed run does not delete raw rows already inserted unless the puller explicitly rolls back the whole transaction.
- Pullers update `data_sources` marker fields only after a successful run.

### Legacy Provenance Tables

The current schema includes `company_sources` and `source_snapshots` from the old direct-write ingestion model. They are not part of the MVP target schema.

`company_sources` currently stores resolved-company provenance:

```text
company_id
source_id
external_id
pull_run_id
raw_data
fetched_at
```

`source_snapshots` currently stores bulk payload snapshots:

```text
source_id
pull_run_id
payload_hash
payload
fetched_at
```

MVP ingestion must not write either table.

Rules:

- `suggestion_source_links` replaces `company_sources` as the provenance mechanism for source-derived suggestions.
- Source-specific raw input tables replace `source_snapshots` as the durable raw payload store.
- The MVP migration should drop `company_sources` and `source_snapshots`.
- Readers that depend on `company_sources` or `source_snapshots` must be removed or rewritten to use suggestions and `suggestion_source_links`.
- Approval services should not recreate the old `company_sources` write path. Provenance for approved data should be traceable from approved suggestions through `suggestion_source_links`.

### `source_processor_states`

`source_processor_states` stores mutable processor state when a processor cannot mark individual raw input rows. This is needed for compatibility schemas such as NVD/CPE, where Corpscout should not add queue columns to tables that are loaded by the shared backoffice-compatible importer.

```sql
CREATE TABLE source_processor_states (
    source_id UUID NOT NULL REFERENCES data_sources(id) ON DELETE CASCADE,
    processor_task_type TEXT NOT NULL,
    last_started_at TIMESTAMPTZ,
    last_success_at TIMESTAMPTZ,
    last_failed_at TIMESTAMPTZ,
    last_processed_marker_type TEXT,
    last_processed_marker TEXT,
    last_processed_at TIMESTAMPTZ,
    last_source_pull_run_id UUID REFERENCES source_pull_runs(id),
    last_error TEXT,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_id, processor_task_type),
    CONSTRAINT chk_source_processor_states_marker_pair CHECK (
        (last_processed_marker_type IS NULL AND last_processed_marker IS NULL)
        OR (last_processed_marker_type IS NOT NULL AND last_processed_marker IS NOT NULL)
    ),
    CONSTRAINT chk_source_processor_states_failures CHECK (
        consecutive_failures >= 0
    )
);
```

Rules:

- Row-queue processors should use the `processing_status` fields on their source-specific input table.
- Compatibility-schema processors should use `source_processor_states` for retry state and their last processed marker.
- River task uniqueness prevents duplicate active compatibility processors for the same source and processor task type.
- CPE processors can store a marker such as `cpe_dictionary.updated_at:id`.
- CVE processors can store a marker such as `nvds.last_modified_date:id` or the last processed NVD pull run ID.
- Processor markers are independent from puller markers. A pull can succeed while the downstream processor still has pending work.

## Source-Specific Raw Input Tables

Every source-specific raw input table should include common operational columns, plus source-specific columns.

This pattern applies to sources where Corpscout owns the raw input table shape. It does not apply to compatibility schemas that must stay aligned with an existing loader contract, such as the NVD/CPE tables copied from backoffice-v2.

Common operational columns:

```text
id
source_pull_run_id
source_native_id
source_updated_at
raw_payload
payload_hash
first_seen_at
last_seen_at
processing_status
processing_attempts
processing_error
processing_lease_by
processing_lease_until
processed_at
created_at
updated_at
```

Recommended status values:

```text
pending
processing
processed
failed
ignored
superseded
```

Rules:

- Use `source_native_id` when the source provides a stable ID.
- Use `payload_hash` to detect changed payloads.
- Preserve raw payload history by inserting a new row when the same native ID has a new payload hash.
- Update `last_seen_at` when the same native ID and payload hash is seen again.
- Processors claim rows with `processing_status = 'pending'` using a lease.
- Failed rows remain available for retry based on attempts and lease expiry.

### CPE/CVE Raw Inputs: Backoffice-V2 Compatibility

CPE and CVE input data should use the same loader-owned schema shape as backoffice-v2. This keeps the NVD import path proven, avoids duplicate parsing logic, and lets future fixes to the feed loader be shared between products.

Do not create simplified `cpe_raw_inputs` or `cve_raw_inputs` tables for the first Corpscout implementation.

Mirror these final tables from the backoffice-v2 NVD/CPE schema:

- `nvds`
- `nvd_descriptions`
- `nvd_references`
- `cwes`
- `nvd_cwes`
- `nvd_cvss3`
- `nvd_cvss40`
- `nvd_cvss2_extras`
- `cpe_dictionary`
- `cpe_match_criteria`
- `cpe_match_criteria_names`
- `nvd_config_nodes`
- `nvd_config_match_criteria`
- `intel_cve_kev`, if KEV is loaded in Corpscout

Use the backoffice-v2 NVD/CPE migrations and loader code as the implementation source of truth, starting from `database/migrations/000002_nvd_intel_schema.up.sql` and including later loader-owned NVD/CPE additions such as vulnerability-state indexes. If the shared loader requires a column, constraint, or index, Corpscout should carry the same definition.

The NVD/CPE schema is large enough that the implementation plan must include a dedicated estimation and migration-splitting pass before writing SQL. Do not put the whole compatibility schema into one migration. Use separate migrations by logical group:

- CVE/NVD core: `nvds`, descriptions, references, CWE, CVSS, config nodes, and config match criteria.
- CPE core: `cpe_dictionary`, `cpe_match_criteria`, and `cpe_match_criteria_names`.
- NVD/CPE lookup and state indexes, including vulnerability-state additions.
- Optional enrichment feeds such as `intel_cve_kev`.

Temporary staging tables should be documented in the loader code or migration comments, but they should not be created as permanent database tables.

Mirror the loader staging contract for NVD bulk CVE imports:

- `stg_nvds`
- `stg_nvd_descriptions`
- `stg_nvd_references`
- `stg_nvd_cwes`
- `stg_nvd_cvss3`
- `stg_nvd_cvss40`
- `stg_nvd_cvss2_extras`
- `stg_cpe_match_criteria`
- `stg_nvd_config_nodes`
- `stg_nvd_config_match_criteria`

The staging tables are temporary tables created inside the loader transaction. They are listed here because Corpscout should preserve the same copy-from and merge contract, not because they need permanent migrations.

Backoffice-v2 loading behavior to preserve:

- NVD CVE feed files are flattened into staging rows, copied with `pgx.CopyFrom`, then merged into `nvds` and child NVD tables.
- Changed CVEs replace their child rows in one transaction.
- CPE dictionary pages upsert into `cpe_dictionary` by `cpe_fs`.
- CPE match pages upsert into `cpe_match_criteria` by `match_criteria_id`.
- CPE match names link `cpe_match_criteria` to `cpe_dictionary` through `cpe_match_criteria_names`.
- CPE/CVE feed metadata and pull attempts are still tracked in Corpscout `source_pull_runs`.

Backoffice-v2 product/catalog tables and views should not be copied into Corpscout as part of this input schema. Examples that remain outside this design include `cpe_catalog_connections`, `product_cpe_bindings`, `product_cve_bindings`, `product_cpe_cve_matches`, product exposure views, and catalog candidate workflows. Corpscout only needs the CPE/CVE source data and entity-link suggestion output.

Processor rules for this compatibility schema:

- CPE processors read from `cpe_dictionary`, not from a Corpscout-only CPE queue table.
- CVE processors read from `nvds`, NVD child tables, `cpe_match_criteria`, `cpe_match_criteria_names`, and `nvd_config_match_criteria`.
- Processors track retries and last processed markers in `source_processor_states`.
- Suggestions link back to these tables through `suggestion_source_links.source_input_key`, using the source table primary key serialized as text.

### Example: GLEIF Company Raw Inputs

```sql
CREATE TABLE gleif_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    lei TEXT NOT NULL,
    legal_name TEXT,
    registration_status TEXT,
    headquarters_country_code TEXT,
    parent_lei TEXT,
    ultimate_parent_lei TEXT,
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_gleif_company_raw_inputs_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_gleif_company_raw_inputs_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_gleif_company_raw_inputs_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_gleif_company_raw_inputs_payload UNIQUE (lei, payload_hash)
);

CREATE INDEX idx_gleif_company_raw_inputs_processing
    ON gleif_company_raw_inputs(processing_status, processing_lease_until, created_at);
```

### Companies House Company Raw Inputs

Companies House has a stable native identifier: company number. Store it as both `source_native_id` and `company_number`.

```sql
CREATE TABLE companies_house_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    company_number TEXT NOT NULL,
    company_name TEXT,
    company_status TEXT,
    company_type TEXT,
    country_iso2 TEXT DEFAULT 'GB',
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_companies_house_raw_source_native CHECK (
        source_native_id = company_number
    ),
    CONSTRAINT chk_companies_house_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_companies_house_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_companies_house_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_companies_house_raw_payload UNIQUE (company_number, payload_hash)
);

CREATE INDEX idx_companies_house_raw_processing
    ON companies_house_company_raw_inputs(processing_status, processing_lease_until, created_at);

CREATE INDEX idx_companies_house_raw_company_number
    ON companies_house_company_raw_inputs(company_number);
```

### Brreg Company Raw Inputs

Brreg has a stable native identifier: `organisasjonsnummer`. Store it as both `source_native_id` and `organization_number`.

```sql
CREATE TABLE brreg_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
    organization_number TEXT NOT NULL,
    organization_name TEXT,
    registration_status TEXT,
    website TEXT,
    country_iso2 TEXT DEFAULT 'NO',
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_brreg_raw_source_native CHECK (
        source_native_id = organization_number
    ),
    CONSTRAINT chk_brreg_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_brreg_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_brreg_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_brreg_raw_payload UNIQUE (organization_number, payload_hash)
);

CREATE INDEX idx_brreg_raw_processing
    ON brreg_company_raw_inputs(processing_status, processing_lease_until, created_at);

CREATE INDEX idx_brreg_raw_organization_number
    ON brreg_company_raw_inputs(organization_number);
```

### Example: AI Company Profile Raw Inputs

```sql
CREATE TABLE ai_company_profile_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    normalized_website TEXT,
    normalized_domain TEXT,
    requested_company_name TEXT,
    model_name TEXT,
    prompt_version TEXT,
    source_updated_at TIMESTAMPTZ,
    raw_payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_status TEXT NOT NULL DEFAULT 'pending',
    processing_attempts INTEGER NOT NULL DEFAULT 0,
    processing_error TEXT,
    processing_lease_by TEXT,
    processing_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_ai_company_profile_raw_inputs_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_ai_company_profile_raw_inputs_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_ai_company_profile_raw_inputs_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_ai_company_profile_raw_inputs_payload UNIQUE (normalized_domain, prompt_version, payload_hash)
);

CREATE INDEX idx_ai_company_profile_raw_inputs_processing
    ON ai_company_profile_raw_inputs(processing_status, processing_lease_until, created_at);
```

## Processor Contract

Each source processor has the same responsibilities:

1. Claim pending raw input rows from its source table.
2. Parse and validate the raw payload.
3. Normalize identity hints such as names, domains, websites, LEIs, GitHub owners, CPE tokens, or CVE IDs.
4. Try to match the input to existing resolved entities.
5. If the input describes an existing entity, create section suggestions for fields that differ.
6. If the input describes a missing entity, create a root entity suggestion and child section suggestions for the data the source provides.
7. Link created suggestions back to the raw input row.
8. Mark the raw input row as processed, ignored, or failed.

Processors must be idempotent. Reprocessing the same raw input row must not create duplicate pending suggestions.

Processor output must be suggestions, not direct mutations.

Forbidden processor writes:

- `companies`
- `company_locations`
- `company_phones`
- `company_emails`
- `company_aliases`
- `company_domains`
- `company_sources`
- organization resolved tables
- open-source project resolved tables

Processors may read resolved tables to compare current values and build `current_payload`, but they may not update those tables. The approval service owns all writes to resolved tables after a reviewer accepts a suggestion.

## Root Entity Suggestions

Root entity suggestions represent proposed new entities.

Use separate root suggestion tables for each resolved entity type:

- `company_suggestions`
- `organization_suggestions`
- `open_source_project_suggestions`

### `company_suggestions`

```sql
CREATE TABLE company_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_legal_name TEXT,
    proposed_website TEXT,
    proposed_canonical_slug TEXT,
    proposed_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_company_suggestions_created_company_when_approved CHECK (
        status <> 'approved' OR created_company_id IS NOT NULL
    )
);

CREATE INDEX idx_company_suggestions_review
    ON company_suggestions(status, proposed_display_name);
```

Rules:

- `company_suggestions` is for new company proposals.
- Existing company updates should use section-specific suggestion tables that point to `companies.id`.
- `created_company_id` is set when the suggestion is approved and the company row is created.
- Child suggestions can point to `company_suggestions.id` before the company exists.
- `proposed_canonical_slug` is only a review hint. The approval service must generate the final `companies.canonical_slug` at approval time using the existing slug generation rules.
- If the generated slug collides with an existing company, approval must retry with a deterministic suffix such as the first 12 UUID characters. Pending suggestions do not reserve slugs.

### Organization And Open-Source Project Root Suggestions

`organization_suggestions` is for proposed new organizations.

```sql
CREATE TABLE organization_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_organization_type TEXT NOT NULL,
    proposed_website TEXT,
    proposed_canonical_slug TEXT,
    proposed_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_organization_id UUID REFERENCES organizations(id) ON DELETE SET NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_organization_suggestions_type CHECK (
        proposed_organization_type IN (
            'foundation',
            'standards_body',
            'nonprofit',
            'government',
            'university',
            'community',
            'other'
        )
    ),
    CONSTRAINT chk_organization_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_organization_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_organization_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_organization_suggestions_created_org_when_approved CHECK (
        status <> 'approved' OR created_organization_id IS NOT NULL
    )
);

CREATE INDEX idx_organization_suggestions_review
    ON organization_suggestions(status, proposed_display_name);
```

`open_source_project_suggestions` is for proposed new open-source projects.

```sql
CREATE TABLE open_source_project_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_repository_url TEXT,
    proposed_website TEXT,
    proposed_license TEXT,
    proposed_lifecycle_status TEXT,
    proposed_canonical_slug TEXT,
    proposed_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_open_source_project_id UUID REFERENCES open_source_projects(id) ON DELETE SET NULL,
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_open_source_project_suggestions_lifecycle CHECK (
        proposed_lifecycle_status IS NULL
        OR proposed_lifecycle_status IN ('active', 'maintenance', 'deprecated', 'unknown')
    ),
    CONSTRAINT chk_open_source_project_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_open_source_project_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_open_source_project_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_open_source_project_suggestions_created_project_when_approved CHECK (
        status <> 'approved' OR created_open_source_project_id IS NOT NULL
    )
);

CREATE INDEX idx_open_source_project_suggestions_review
    ON open_source_project_suggestions(status, proposed_display_name);
```

Do not force organization or open-source project suggestions to share company-only fields such as legal name, LEI, revenue, or headquarters.

Rules:

- `proposed_canonical_slug` is only a review hint for both tables.
- Approval services must generate final slugs at approval time using the same slug collision strategy as company suggestions.
- `created_organization_id` and `created_open_source_project_id` are set when approval creates the resolved row.

## Section Suggestion Tables

Section suggestion tables represent proposed changes to one profile section.

A section suggestion has exactly one target:

- an existing resolved entity, or
- a root suggestion for a new entity that does not exist yet

For company suggestions, that means exactly one of:

- `company_id`
- `company_suggestion_id`

For organization suggestions, exactly one of:

- `organization_id`
- `organization_suggestion_id`

For open-source project suggestions, exactly one of:

- `open_source_project_id`
- `open_source_project_suggestion_id`

### Common Section Suggestion Columns

Every section suggestion table should include this logical shape:

```text
id
resolved entity target, nullable
root suggestion target, nullable
operation
current_payload
proposed_payload
confidence
status
reviewed_by
reviewed_at
review_note
created_at
updated_at
```

Recommended operations:

```text
add
update
remove
replace
```

Recommended statuses:

```text
pending
approved
rejected
superseded
```

`current_payload` should store the current value snapshot the processor compared against. Reviewers need this to understand what changed. It also helps detect stale suggestions when resolved data changes before approval.

`proposed_payload` should store the new section value or values.

### `company_domain_suggestions`

```sql
CREATE TABLE company_domain_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    domain TEXT NOT NULL,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_domain_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_domain_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_domain_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_domain_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_domain_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_domain_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_domain_suggestions_existing_target
    ON company_domain_suggestions(company_id, status)
    WHERE company_id IS NOT NULL;

CREATE INDEX idx_company_domain_suggestions_new_target
    ON company_domain_suggestions(company_suggestion_id, status)
    WHERE company_suggestion_id IS NOT NULL;
```

### `company_contact_suggestions`

`company_contact_suggestions` covers email, phone, website, social link, and other company contact changes.

```sql
CREATE TABLE company_contact_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    contact_kind TEXT NOT NULL,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_contact_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_contact_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_contact_suggestions_contact_kind CHECK (
        contact_kind IN ('email', 'phone', 'website', 'social', 'other')
    ),
    CONSTRAINT chk_company_contact_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_contact_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_contact_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_contact_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);
```

### `company_location_suggestions`

`company_location_suggestions` covers headquarters, registered office, branch, and other address changes.

```sql
CREATE TABLE company_location_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    location_kind TEXT NOT NULL,
    country_code TEXT,
    city TEXT,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_location_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_location_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_location_suggestions_location_kind CHECK (
        location_kind IN ('headquarters', 'registered', 'office', 'branch', 'other')
    ),
    CONSTRAINT chk_company_location_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_location_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_location_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_location_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_location_suggestions_existing_target
    ON company_location_suggestions(company_id, status)
    WHERE company_id IS NOT NULL;

CREATE INDEX idx_company_location_suggestions_new_target
    ON company_location_suggestions(company_suggestion_id, status)
    WHERE company_suggestion_id IS NOT NULL;
```

### `company_status_suggestions`

`company_status_suggestions` covers lifecycle/status fields and registry identity fields that are scalar values rather than section lists.

```sql
CREATE TABLE company_status_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    status_field TEXT NOT NULL,
    current_value TEXT,
    proposed_value TEXT,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_status_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_status_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_status_suggestions_status_field CHECK (
        status_field IN (
            'lifecycle_status',
            'registration_status',
            'legal_name',
            'registration_number',
            'lei',
            'other'
        )
    ),
    CONSTRAINT chk_company_status_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_status_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_status_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_status_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_status_suggestions_existing_target
    ON company_status_suggestions(company_id, status, status_field)
    WHERE company_id IS NOT NULL;

CREATE INDEX idx_company_status_suggestions_new_target
    ON company_status_suggestions(company_suggestion_id, status, status_field)
    WHERE company_suggestion_id IS NOT NULL;
```

### `company_relationship_suggestions`

`company_relationship_suggestions` covers parent, subsidiary, ownership, acquisition, and merger relationships.

```sql
CREATE TABLE company_relationship_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    company_id UUID REFERENCES companies(id) ON DELETE CASCADE,
    company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE CASCADE,
    operation TEXT NOT NULL,
    relationship_type TEXT NOT NULL,
    related_company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    related_company_suggestion_id UUID REFERENCES company_suggestions(id) ON DELETE SET NULL,
    related_company_name TEXT,
    related_lei TEXT,
    current_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    proposed_payload JSONB NOT NULL,
    confidence REAL,
    status TEXT NOT NULL DEFAULT 'pending',
    reviewed_by TEXT,
    reviewed_at TIMESTAMPTZ,
    review_note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_company_relationship_suggestions_target CHECK (
        (company_id IS NOT NULL AND company_suggestion_id IS NULL)
        OR (company_id IS NULL AND company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_relationship_suggestions_related_target CHECK (
        NOT (
            related_company_id IS NOT NULL
            AND related_company_suggestion_id IS NOT NULL
        )
    ),
    CONSTRAINT chk_company_relationship_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_relationship_suggestions_relationship_type CHECK (
        relationship_type IN (
            'direct_parent',
            'ultimate_parent',
            'subsidiary_of',
            'owned_by',
            'acquired_by',
            'merged_into',
            'other'
        )
    ),
    CONSTRAINT chk_company_relationship_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_company_relationship_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_company_relationship_suggestions_current_object CHECK (
        jsonb_typeof(current_payload) = 'object'
    ),
    CONSTRAINT chk_company_relationship_suggestions_proposed_object CHECK (
        jsonb_typeof(proposed_payload) = 'object'
    )
);

CREATE INDEX idx_company_relationship_suggestions_existing_target
    ON company_relationship_suggestions(company_id, status, relationship_type)
    WHERE company_id IS NOT NULL;

CREATE INDEX idx_company_relationship_suggestions_new_target
    ON company_relationship_suggestions(company_suggestion_id, status, relationship_type)
    WHERE company_suggestion_id IS NOT NULL;
```

### Additional Company Section Tables

Use the same section-suggestion pattern for:

- `company_market_suggestions`
- `company_service_suggestions`
- `company_industry_suggestions`
- `company_financial_suggestions`

Each table should use typed columns for fields that are frequently filtered in the UI. Less frequently used structured data can remain in `proposed_payload`.

Examples:

- `company_financial_suggestions` should include `financial_kind`, `currency`, `period_year`, and `amount` when available.

### Organization Section Suggestions

Organizations should have their own section suggestion tables. Do not reuse company section tables.

Recommended initial organization tables:

- `organization_website_suggestions`
- `organization_contact_suggestions`
- `organization_location_suggestions`
- `organization_governance_suggestions`
- `organization_project_suggestions`
- `organization_social_link_suggestions`

### Open-Source Project Section Suggestions

Open-source projects should have project-specific section suggestion tables.

Recommended initial project tables:

- `open_source_project_repository_suggestions`
- `open_source_project_package_suggestions`
- `open_source_project_maintainer_suggestions`
- `open_source_project_security_contact_suggestions`
- `open_source_project_license_suggestions`
- `open_source_project_forum_suggestions`
- `open_source_project_release_suggestions`

## Suggestion Source Links

Suggestions should link back to the source input rows that produced them.

Use a small operational provenance table:

```sql
CREATE TABLE suggestion_source_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    suggestion_table TEXT NOT NULL,
    suggestion_id UUID NOT NULL,
    source_id UUID NOT NULL REFERENCES data_sources(id),
    source_input_table TEXT NOT NULL,
    source_input_key TEXT NOT NULL,
    source_pull_run_id UUID REFERENCES source_pull_runs(id),
    confidence REAL,
    evidence_excerpt TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT chk_suggestion_source_links_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    )
);

CREATE INDEX idx_suggestion_source_links_suggestion
    ON suggestion_source_links(suggestion_table, suggestion_id);

CREATE INDEX idx_suggestion_source_links_source
    ON suggestion_source_links(source_id, source_input_table, source_input_key);
```

This table is generic by design because it is not raw input data and it is not resolved profile data. It is only provenance glue between source-specific input tables and source-specific suggestion tables.

Rules:

- A suggestion can have multiple source links.
- Multiple sources can support the same suggestion.
- `source_id` references the matching `data_sources` row.
- The service layer validates that `source_input_table` and `source_input_key` point to an existing source-specific input row.
- `source_input_key` stores the source input primary key in text form. For UUID raw input tables this is the UUID string; for NVD/CPE compatibility tables this is the numeric primary key string; for composite source keys the processor must use a stable canonical key.
- Do not copy full raw payloads into `evidence_excerpt`.
- Use `evidence_excerpt` only for short UI summaries.

## Deduplication Rules

### Raw Input Deduplication

Raw source input tables dedupe by source-native identity and payload hash.

Rules:

- Same native ID and same payload hash: update `last_seen_at`.
- Same native ID and different payload hash: insert a new raw input row.
- No native ID: dedupe by a stable normalized key and payload hash.
- Never merge raw input rows across different sources.
- Compatibility schemas use their own natural constraints instead. For NVD/CPE this means keys such as `nvds.cve_id`, `cpe_dictionary.cpe_fs`, `cpe_match_criteria.match_criteria_id`, and `cpe_match_criteria_names` link uniqueness.

### Suggestion Deduplication

Processors should avoid duplicate pending suggestions.

Recommended dedupe key:

```text
entity type
section table
existing target or root suggestion target
operation
normalized proposed payload hash
status = pending
```

If a matching pending suggestion exists, add a new `suggestion_source_links` row instead of creating another suggestion.

If an approved or rejected suggestion exists, create a new suggestion only when the new evidence or proposed payload materially differs.

## Approval Workflow

### Approving A New Entity Suggestion

For a new company suggestion:

1. Lock the `company_suggestions` row.
2. Verify status is `pending`.
3. Create the `companies` row.
4. Set `company_suggestions.status = 'approved'`.
5. Set `company_suggestions.created_company_id` to the new company ID.
6. Optionally approve selected child section suggestions in the same transaction.
7. Leave rejected or unreviewed child suggestions unchanged.

The same pattern applies to organization and open-source project root suggestions.

### Approving A Section Suggestion For An Existing Entity

1. Lock the section suggestion row.
2. Verify status is `pending`.
3. Verify the target resolved entity still exists.
4. Compare `current_payload` with the current resolved data.
5. If the resolved data changed materially, mark the suggestion `superseded` and create a fresh suggestion if needed.
6. Apply the `proposed_payload` to the resolved table or child table.
7. Mark the suggestion `approved`.
8. Store reviewer metadata.

### Approving A Section Suggestion For A New Entity

1. Lock the section suggestion row.
2. Verify status is `pending`.
3. Load the parent root suggestion.
4. Verify the parent root suggestion is approved and has a created entity ID.
5. Apply the section suggestion to that created entity.
6. Mark the section suggestion `approved`.

A child section suggestion cannot be applied before its root entity suggestion creates the resolved entity.

### Rejecting Suggestions

Rejecting a suggestion:

- sets `status = 'rejected'`
- stores `reviewed_by`, `reviewed_at`, and `review_note`
- does not delete source links
- does not delete raw input rows

Rejected suggestions become durable negative evidence. Processors should check rejected suggestions before recreating similar pending suggestions.

## HTTP API Boundary

This document defines the database and service behavior for suggestions. The implementation plan must include a separate API design task before building the UI review workflow.

Minimum route surface to design:

- list pending review requests, grouped as new entity suggestions or existing entity section suggestions
- fetch one review request with child suggestions and source evidence
- approve or reject a root entity suggestion
- approve or reject one section suggestion
- approve selected child sections while approving a new root entity

The existing domain review endpoints are not enough for the new suggestion model. They should be removed or replaced by suggestion APIs that call service-layer approval methods rather than writing table-specific status updates directly in handlers.

### Domain Review Replacement

The old domain review surface is part of the direct-write ingestion model and is not preserved in MVP.

Rules:

- Drop `company_domain_reviews` from the active MVP schema.
- Replace domain review endpoints with the generic suggestion review endpoints.
- Remove `DomainResolveWorker` direct writes to `company_domains`.
- Domain discovery should write `domain_discovery_raw_inputs` and then create `company_domain_suggestions`.
- Approved domain suggestions are applied by the approval service into `company_domains`.

### Source API Replacement

The existing source API handlers must be rewritten for the clean MVP `data_sources` schema. The old API shape references columns from the direct-write scheduler model and should not be preserved.

Handler rules:

- `GET /api/v1/sources` returns the new source registry fields: `name`, `display_name`, `description`, `source_group`, `input_table_name`, `pull_task_type`, `processor_task_type`, `enabled`, `schedule_kind`, `schedule_expression`, `last_started_at`, `last_success_at`, `last_failed_at`, `last_source_marker_type`, `last_source_marker`, `last_source_modified_at`, `last_error`, and `consecutive_failures`.
- `PATCH /api/v1/sources/:name` updates MVP-editable fields only: `enabled`, `schedule_kind`, `schedule_expression`, safe `config`, `display_name`, and `description`.
- `POST /api/v1/sources/:name/trigger` validates that the source exists and is enabled, then enqueues `data_sources.pull_task_type` with River uniqueness on source name and task type.
- `source_pull_runs` rows are created by the pull worker, not by the HTTP handler, so manual trigger failure before River enqueue does not create a run row.
- Remove handler and query references to old columns such as `adapter_type`, `crawl_interval_hours`, `last_crawled_at`, and `last_cursor`.

## UI Review Model

### New Entity Review

The UI should show a new company suggestion as one request:

```text
company_suggestions row
-> company_domain_suggestions rows
-> company_contact_suggestions rows
-> company_location_suggestions rows
-> company_status_suggestions rows
-> company_relationship_suggestions rows
-> other company section suggestions
```

Reviewers can:

- approve the root entity
- approve selected child sections
- reject selected child sections
- reject the whole suggestion

If the root entity is rejected, child suggestions should be marked `rejected` or `superseded` in the same review action.

Child suggestions attached to a root suggestion should appear under that new-entity review request. They should not appear in existing-entity update queues because they do not target a resolved entity yet.

### Existing Entity Review

Existing entity updates should be grouped by:

- entity type
- entity ID
- section
- source

Examples:

- contact updates for one company
- domain updates for one company
- financial updates for one company
- repository updates for one open-source project

The UI should show source evidence through `suggestion_source_links`.

## Source Pull And Processing Scheduling

Sources can run on different schedules.

Examples:

- CPE source: scheduled daily or weekly
- CVE source: scheduled frequently
- GLEIF source: scheduled daily or weekly depending on feed cadence
- website crawl source: scheduled with rate limits
- AI profile source: manual or queue-driven
- manual research source: event-driven

Scheduling rules:

- Scheduler reads `data_sources`.
- Scheduler enqueues a River task using `data_sources.pull_task_type`.
- The River pull task creates a `source_pull_runs` row when it starts.
- The pull task writes source-specific raw input rows or updates a source-compatible input schema only.
- The pull task updates `source_pull_runs` and, on success, `data_sources` marker fields.
- The pull task enqueues `data_sources.processor_task_type` when processing is configured separately.
- Processor workers read pending rows from source-specific raw input tables, or scan compatibility tables from their processor marker.
- Processor workers create suggestions and source links.

MVP active sources use separate pull and process River tasks. The source work boundary is explicit: pull tasks stop at raw input, process tasks stop at suggestions, and reviewed approval applies resolved data changes.

## Concurrency And Retry Rules

Pullers:

- use River task claiming and River uniqueness for concurrency control
- update `data_sources` marker fields only after successful pull completion
- record failed runs without losing the previous successful source marker
- never write resolved company, organization, or open-source project tables

Processors:

- for row-queue tables, claim raw rows with `processing_status = 'pending'`
- for row-queue tables, set `processing_status = 'processing'`
- for row-queue tables, set `processing_lease_by` and `processing_lease_until`
- for row-queue tables, increment `processing_attempts`
- for row-queue tables, mark row `processed`, `ignored`, or `failed`
- for compatibility schemas, use River task claiming and `source_processor_states` markers
- retry failed processor work only when attempts and backoff policy allow
- never write resolved company, organization, or open-source project tables

Approval:

- locks suggestion rows before applying changes
- checks that the suggestion is still pending
- performs resolved table writes and suggestion status updates in one transaction
- is the only path from source-derived data into resolved entity/profile tables

## Source Examples

### AI Company Profile Source

Input:

- website
- optional company name
- AI response payload

Processor behavior:

- normalize website and domain
- search for existing company by domain and website
- if company exists, create section suggestions for changed contact, domain, market, service, financial, or relationship data
- if company does not exist, create `company_suggestions` plus child company section suggestions for available data
- link every suggestion to `ai_company_profile_raw_inputs`

### GLEIF Source

Input:

- LEI
- legal name
- registration status
- headquarters data
- direct parent LEI
- ultimate parent LEI

Processor behavior:

- match existing company by LEI
- create company status, legal name, location, and relationship suggestions when values differ
- create `company_suggestions` only when a reviewed import policy allows new companies from GLEIF
- link suggestions to `gleif_company_raw_inputs`

### Domain Discovery Source

Input:

- discovered domain
- source page or signal
- confidence score

Processor behavior:

- match domain to existing company, organization, or open-source project
- create domain suggestions for existing entities
- create root entity suggestions only when there is enough identity evidence
- keep weak discoveries in the domain-specific raw input table as ignored or failed review candidates

### CPE Source

Input:

- backoffice-compatible `cpe_dictionary` rows
- backoffice-compatible `cpe_match_criteria` rows
- `cpe_match_criteria_names` links from match criteria to dictionary names

Processor behavior:

- read CPE rows from the mirrored CPE schema using `source_processor_states` markers
- create `cpe_entity_link_suggestions`
- link suggestions back to `cpe_dictionary` or `cpe_match_criteria` with `suggestion_source_links`
- do not create product records in Corpscout
- approve into `cpe_entity_links` only after review or trusted approval path

### CVE Source

Input:

- backoffice-compatible `nvds` rows
- NVD child rows for descriptions, references, CWEs, CVSS, and configurations
- CPE match criteria connected through `nvd_config_match_criteria` and `cpe_match_criteria_names`

Processor behavior:

- read CVE rows from the mirrored NVD schema using `source_processor_states` markers
- create `cve_entity_link_suggestions` only for entity-level relevance
- link suggestions back to `nvds`, `nvd_config_match_criteria`, or related CPE tables with `suggestion_source_links`
- approve into `cve_entity_links` after review
- keep product-level applicability outside Corpscout

## Migration Strategy

1. Replace the old source-ingestion implementation with the MVP target schema; do not preserve direct-write compatibility.
2. Create clean `data_sources` and `source_pull_runs` tables and seed MVP sources.
3. Add `source_processor_states`.
4. Add the first Corpscout-owned source-specific raw input tables for active non-CPE/CVE sources: `gleif_company_raw_inputs`, `companies_house_company_raw_inputs`, `brreg_company_raw_inputs`, `ai_company_profile_raw_inputs`, and any active domain discovery input table.
5. Add `suggestion_source_links`.
6. Add root suggestion tables: `company_suggestions`, `organization_suggestions`, and `open_source_project_suggestions`.
7. Add initial company section suggestion tables for domains, contacts, locations, status, and relationships.
8. Drop old direct-ingestion tables such as `company_sources`, `source_snapshots`, and `company_domain_reviews`.
9. Remove old direct-write source worker behavior and replace `SourceCrawlWorker` with `SourcePullWorker` plus `SourceProcessWorker` responsibilities.
10. Remove `DomainResolveWorker` direct writes and replace domain review flows with domain raw inputs plus `company_domain_suggestions`.
11. Update existing `/api/v1/sources` handlers and queries to the clean `data_sources` schema.
12. Add initial organization and open-source project section suggestion tables only when the first active processor emits those suggestions.
13. Add service-layer approval and rejection methods, including slug collision handling.
14. Add an HTTP API design task for the suggestion review workflow before UI work starts.
15. Add UI review screens grouped by root entity suggestions and section suggestions.
16. Run a dedicated NVD/CPE schema estimation pass, then add the backoffice-compatible schema in multiple migrations by logical group.
17. Wire CPE/CVE processors only after the mirrored NVD/CPE schema and loader compatibility tests are in place.
18. Keep external consumer API work deferred to the phase-two CPE lookup design.

MVP implementation scope:

- clean source registry and pull-run schema
- `source_processor_states`
- `suggestion_source_links`
- GLEIF, Companies House, Brreg, AI company profile, and active domain discovery raw input tables
- company root suggestions
- company domain, contact, location, status, and relationship suggestions
- all active non-CPE source paths emit suggestions only
- old direct-write source worker paths removed and replaced
- `company_sources`, `source_snapshots`, and `company_domain_reviews` removed from active writes and active reads
- existing source API handlers updated to the clean `data_sources` schema
- service-layer approval/rejection for company root and initial company section suggestions

Out of first implementation scope:

- full NVD/CPE mirrored schema implementation
- CPE/CVE processors
- organization and open-source project section tables not emitted by the first processor
- phase-two external lookup API
- preserving old source-ingestion behavior for compatibility
- preserving the old `/api/v1/sources` response shape or old scheduler column names

## Testing Plan

Database tests:

- `data_sources` seed contains the MVP source registry and River task types
- `data_sources` stores last successful source marker metadata
- processor state stores independent last processed marker metadata
- pull runs track River job ID, task type, status, row counts, and errors
- old `company_sources`, `source_snapshots`, and `company_domain_reviews` are not part of the active MVP schema
- source-specific raw input tables dedupe by native ID and payload hash
- source-specific raw input tables allow multiple versions of the same source-native entity
- Companies House raw inputs require `source_native_id = company_number`
- Brreg raw inputs require `source_native_id = organization_number`
- company root suggestion approval handles canonical slug collisions
- mirrored NVD/CPE tables stay schema-compatible with the backoffice-v2 loader-owned schema
- `suggestion_source_links.source_input_key` supports UUID, numeric, and canonical composite input keys
- section suggestions require exactly one target: existing entity or root suggestion
- approved root suggestions require a created resolved entity ID
- suggestion source links can attach multiple sources to one suggestion
- rejected suggestions retain source links

Worker and processor tests:

- pullers update source run metadata correctly
- `SourcePullWorker` writes raw input rows only and enqueues the configured processor task
- `SourceProcessWorker` reads raw input rows and writes suggestions plus source links only
- row-queue processors claim rows with processing leases when using Corpscout-owned raw input tables
- CPE/CVE processors use `source_processor_states` instead of mutating mirrored NVD/CPE input tables
- source processors do not write `companies`, company profile tables, `company_sources`, `source_snapshots`, or `company_domain_reviews`
- domain discovery does not write `company_domains` directly and emits `company_domain_suggestions`
- processors are idempotent on repeated raw input rows or repeated compatibility-table scans
- processors create root suggestions when no resolved entity exists
- processors create section suggestions when resolved entity values differ
- processors add source links to existing pending suggestions instead of duplicating them
- failed processing rows are retryable
- ignored rows do not create suggestions

Approval tests:

- approving a company root suggestion creates one company row
- approving child suggestions for a new company writes to the created company
- approving section suggestions for existing companies updates only the intended section
- stale suggestions are superseded when current resolved data no longer matches the captured snapshot
- rejecting suggestions does not mutate resolved data
- approving duplicate pending suggestions is prevented by service-layer checks
- approval service is the only path that mutates resolved company/profile tables from source-derived suggestions

API tests:

- `GET /api/v1/sources` returns clean `data_sources` fields and no old scheduler fields
- `PATCH /api/v1/sources/:name` updates only MVP-editable source fields
- `POST /api/v1/sources/:name/trigger` enqueues the configured pull task with source uniqueness
- review list endpoint returns grouped root and section suggestions
- review detail endpoint includes child suggestions and source links
- approve endpoint calls the service layer and records reviewer metadata
- reject endpoint does not mutate resolved data

UI tests:

- new company suggestion shows root and child section suggestions as one review request
- existing company suggestions group by company and section
- source evidence is visible through source links
- reviewers can approve or reject child sections independently
- rejecting a root suggestion rejects or supersedes its child suggestions

## Implementation Defaults

- Prefer source-specific raw input tables over a generic catch-all table.
- For CPE/CVE, prefer the backoffice-compatible NVD/CPE schema and loader contract over Corpscout-only raw input tables.
- Preserve raw source payloads for traceability.
- Never store secrets in source metadata, raw payloads, logs, or errors.
- Treat pullers and processors as separate responsibilities, even when implemented in the same worker.
- Keep suggestion tables reviewable and durable; do not delete rejected suggestions.
- Use `suggestion_source_links` for provenance instead of copying full source payloads into suggestion tables.
- Use section-specific suggestion tables so approvals can be reviewed and applied independently.
- Use root entity suggestions only for creating new companies, organizations, or open-source projects.
- Existing entity updates should be represented by section suggestion rows.
- CPE/CVE suggestions are source-specific mapping suggestions, not the universal source model.
- Source pullers and processors must never write resolved entity/profile tables directly.
- Legacy `company_sources` and `source_snapshots` are not part of the active MVP schema; drop them.

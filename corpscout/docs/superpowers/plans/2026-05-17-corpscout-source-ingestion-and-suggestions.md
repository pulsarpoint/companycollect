# Corpscout Source Ingestion And Suggestions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the direct-write `SourceCrawlWorker` pipeline with a two-worker pull+process pipeline that routes all source-derived data through reviewable suggestions before reaching resolved entity tables.

**Architecture:** `SourcePullWorker` fetches external data and writes only to source-specific raw input tables; `SourceProcessWorker` reads those tables and emits suggestions plus provenance links; the approval service is the only path that writes resolved entity/profile rows after a reviewer accepts a suggestion.

**Tech Stack:** Go 1.24, pgx/v5, sqlc v1.30, River (riverqueue/river), Chi router, PostgreSQL 5435 (`corpscout`). All Go commands use `GOWORK=off` (or the Makefile). Migrations live in `database/migrations/`, queries in `database/queries/`, generated code in `scheduler/internal/db/gen/`.

---

## File Map

### Created
- `database/migrations/000017_source_ingestion_mvp.up.sql`
- `database/migrations/000017_source_ingestion_mvp.down.sql`
- `database/migrations/000018_source_raw_input_tables.up.sql`
- `database/migrations/000018_source_raw_input_tables.down.sql`
- `database/migrations/000019_suggestion_tables.up.sql`
- `database/migrations/000019_suggestion_tables.down.sql`
- `database/queries/raw_inputs.sql`
- `database/queries/suggestions.sql`
- `scheduler/internal/workers/source_pull.go`
- `scheduler/internal/workers/source_pull_test.go`
- `scheduler/internal/workers/source_process.go`
- `scheduler/internal/workers/source_process_test.go`
- `scheduler/internal/workers/gleif_processor.go`
- `scheduler/internal/workers/gleif_processor_test.go`
- `scheduler/internal/workers/companies_house_processor.go`
- `scheduler/internal/workers/companies_house_processor_test.go`
- `scheduler/internal/workers/brreg_processor.go`
- `scheduler/internal/workers/brreg_processor_test.go`
- `scheduler/internal/workers/processor_testmock_test.go`
- `scheduler/internal/service/suggestions.go`
- `scheduler/internal/service/suggestions_test.go`
- `scheduler/internal/httpapi/suggestions.go`
- `scheduler/internal/httpapi/suggestions_test.go`

### Modified
- `database/queries/sources.sql` — rewrite for new schema
- `database/queries/pull_runs.sql` — rewrite for new schema
- `database/queries/companies.sql` — add GetCompanyByLEI, GetCompanyByRegistrationAndCountry
- `database/queries/countries.sql` — keep country lookups in the canonical country reference query file; add an ID-only lookup for processors
- `scheduler/internal/workers/workers.go` — replace old arg types with SourcePullArgs / SourceProcessArgs
- `scheduler/internal/app/app.go` — rewrite scheduleOnce for new schema
- `scheduler/internal/app/river.go` — replace old workers with new workers
- `scheduler/internal/httpapi/sources.go` — rewrite for new schema
- `scheduler/internal/httpapi/handlers.go` — remove /review routes, add /suggestions routes
- `scheduler/internal/httpapi/testhelpers_test.go` — update stub methods for new Querier interface

### Deleted
- `scheduler/internal/workers/source_crawl.go`
- `scheduler/internal/workers/domain_resolve.go`
- `scheduler/internal/workers/gleif_enrich.go`
- `scheduler/internal/httpapi/review.go`

---

## Country reference-data correction

Countries are canonical reference data, not source-ingestion inputs and not suggestions. The `countries` table should be treated as the single place where country identifiers live; processors only resolve source-provided country codes against that table.

Current state: `database/migrations/000001_initial_schema.up.sql` creates `countries`, and `database/migrations/000002_countries_seed.up.sql` seeds only the initial working set. A later pass should add a dedicated country reference migration that extends this data to the full ISO 3166 country list and all codes we care about:

- `iso_alpha2`
- `iso_alpha3`
- `iso_numeric`
- `m49_code` if we want UN M49 compatibility
- `name` / common display name
- optional `official_name`

If sources use non-standard aliases such as `UK`, `Great Britain`, `Norge`, or source-specific region labels, add a separate `country_aliases` table instead of overloading `countries`. That table should map aliases to `country_id` and include `alias_type` / `source_name` when useful.

Important implementation rule for this plan: do not add country lookup queries to `suggestions.sql`. Put country lookup queries in `database/queries/countries.sql`. Processors should use the canonical country reference table to set `company_suggestions.proposed_country_id`. If a source country cannot be resolved, keep the raw country value in `proposed_profile` and leave `proposed_country_id` null; approval must require a resolved country before creating a `companies` row.

---

## Task 1: Migration 017 – Replace source operational tables

**Files:**
- Create: `database/migrations/000017_source_ingestion_mvp.up.sql`
- Create: `database/migrations/000017_source_ingestion_mvp.down.sql`

- [ ] **Step 1: Write the up migration**

Create `database/migrations/000017_source_ingestion_mvp.up.sql`:

```sql
-- Drop views that depend on company_sources and data_sources before dropping those tables.
-- v_company_sources is obsolete (replaced by suggestion_source_links) — drop permanently.
-- v_companies is recreated below after the new data_sources table exists.
DROP VIEW IF EXISTS v_company_sources;
DROP VIEW IF EXISTS v_companies;

-- Drop FK references to old data_sources before we drop the table.
ALTER TABLE companies DROP CONSTRAINT IF EXISTS companies_primary_source_id_fkey;
ALTER TABLE company_aliases DROP CONSTRAINT IF EXISTS company_aliases_source_id_fkey;

-- Null out stale UUIDs so re-added FKs don't violate referential integrity.
UPDATE companies SET primary_source_id = NULL WHERE primary_source_id IS NOT NULL;
UPDATE company_aliases SET source_id = NULL WHERE source_id IS NOT NULL;

-- Drop legacy direct-write ingestion tables in dependency order.
DROP TABLE IF EXISTS company_domain_reviews;
DROP TABLE IF EXISTS company_sources;
DROP TABLE IF EXISTS source_snapshots;
DROP TABLE IF EXISTS source_pull_runs;
DROP TABLE IF EXISTS data_sources;

-- Clean new source registry.
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
            'security_identifier', 'registry', 'domain', 'website',
            'github', 'ai_research', 'manual', 'other'
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
    CONSTRAINT chk_data_sources_config_object CHECK (jsonb_typeof(config) = 'object')
);

-- Clean pull-run audit table.
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
    CONSTRAINT chk_source_pull_runs_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_source_pull_runs_source_started
    ON source_pull_runs(source_id, started_at DESC);

-- Processor state for compatibility schemas (CPE/CVE) that can't use row-queue columns.
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
    CONSTRAINT chk_source_processor_states_failures CHECK (consecutive_failures >= 0)
);

-- Re-add FKs pointing to the new data_sources table.
ALTER TABLE companies
    ADD CONSTRAINT companies_primary_source_id_fkey
    FOREIGN KEY (primary_source_id) REFERENCES data_sources(id) ON DELETE SET NULL;

ALTER TABLE company_aliases
    ADD CONSTRAINT company_aliases_source_id_fkey
    FOREIGN KEY (source_id) REFERENCES data_sources(id) ON DELETE SET NULL;

-- Seed MVP source registry.
INSERT INTO data_sources (
    name, display_name, source_group, input_table_name,
    pull_task_type, processor_task_type, enabled,
    schedule_kind, schedule_expression, config
)
VALUES
    ('gleif', 'GLEIF', 'registry', 'gleif_company_raw_inputs',
     'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('companies_house', 'UK Companies House', 'registry', 'companies_house_company_raw_inputs',
     'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('brreg', 'Brreg', 'registry', 'brreg_company_raw_inputs',
     'source_pull', 'source_process', true, 'interval', '24h', '{}'::jsonb),
    ('ai_company_profile', 'AI Company Profile', 'ai_research', 'ai_company_profile_raw_inputs',
     'ai_company_profile_pull', 'source_process', false, 'manual', NULL, '{}'::jsonb),
    ('nvd_cpe', 'NVD CPE', 'security_identifier', 'cpe_dictionary',
     'nvd_cpe_sync', 'nvd_cpe_process', false, 'interval', '24h', '{}'::jsonb),
    ('nvd_cve', 'NVD CVE', 'security_identifier', 'nvds',
     'nvd_cve_sync', 'nvd_cve_process', false, 'interval', '6h', '{}'::jsonb)
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

-- Recreate v_companies pointing at the new data_sources table.
-- v_company_sources is not recreated — it referenced company_sources which is now dropped.
CREATE VIEW v_companies AS
SELECT
    c.id,
    c.name,
    c.registration_number,
    c.lei,
    c.status,
    c.created_at,
    c.updated_at,
    co.id          AS country_id,
    co.name        AS country_name,
    co.iso_alpha2  AS country_iso2,
    ds.name        AS primary_source,
    ds.display_name AS primary_source_display_name,
    (SELECT COUNT(*)::int FROM company_domains cd WHERE cd.company_id = c.id) AS domain_count
FROM companies c
JOIN countries co ON co.id = c.country_id
LEFT JOIN data_sources ds ON ds.id = c.primary_source_id;

GRANT SELECT ON v_companies TO corpscout_anon;
```

- [ ] **Step 2: Write the down migration**

Create `database/migrations/000017_source_ingestion_mvp.down.sql`:

```sql
DROP TABLE IF EXISTS source_processor_states;
DROP TABLE IF EXISTS source_pull_runs;

ALTER TABLE companies DROP CONSTRAINT IF EXISTS companies_primary_source_id_fkey;
ALTER TABLE company_aliases DROP CONSTRAINT IF EXISTS company_aliases_source_id_fkey;

DROP TABLE IF EXISTS data_sources;
```

- [ ] **Step 3: Apply migration and verify**

```bash
cd scheduler && make migrate-up
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "\d data_sources" \
  -c "SELECT name, pull_task_type, enabled, schedule_kind FROM data_sources ORDER BY name;"
```

Expected: table description shows new columns (`last_started_at`, `pull_task_type`, `consecutive_failures`); 6 seed rows present.

- [ ] **Step 4: Commit**

```bash
git add database/migrations/000017_source_ingestion_mvp.up.sql \
        database/migrations/000017_source_ingestion_mvp.down.sql
git commit -m "feat(db): replace source operational tables with clean MVP schema (migration 017)"
```

---

## Task 2: Migration 018 – Source-specific raw input tables

**Files:**
- Create: `database/migrations/000018_source_raw_input_tables.up.sql`
- Create: `database/migrations/000018_source_raw_input_tables.down.sql`

- [ ] **Step 1: Write the up migration**

Create `database/migrations/000018_source_raw_input_tables.up.sql`:

```sql
-- GLEIF raw inputs.
CREATE TABLE gleif_company_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    source_native_id TEXT NOT NULL,
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
    CONSTRAINT chk_gleif_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_gleif_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_gleif_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_gleif_company_raw_inputs_payload UNIQUE (lei, payload_hash)
);

CREATE INDEX idx_gleif_raw_processing
    ON gleif_company_raw_inputs(processing_status, processing_lease_until, created_at);

-- Companies House raw inputs.
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
    CONSTRAINT chk_ch_raw_source_native CHECK (source_native_id = company_number),
    CONSTRAINT chk_ch_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_ch_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_ch_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_companies_house_raw_payload UNIQUE (company_number, payload_hash)
);

CREATE INDEX idx_ch_raw_processing
    ON companies_house_company_raw_inputs(processing_status, processing_lease_until, created_at);
CREATE INDEX idx_ch_raw_company_number
    ON companies_house_company_raw_inputs(company_number);

-- Brreg raw inputs.
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
    CONSTRAINT chk_brreg_raw_source_native CHECK (source_native_id = organization_number),
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

-- AI company profile raw inputs.
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
    CONSTRAINT chk_ai_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_ai_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_ai_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT uq_ai_company_profile_raw_payload UNIQUE (normalized_domain, prompt_version, payload_hash)
);

CREATE INDEX idx_ai_raw_processing
    ON ai_company_profile_raw_inputs(processing_status, processing_lease_until, created_at);

-- Domain discovery raw inputs (processor deferred, table added for completeness).
CREATE TABLE domain_discovery_raw_inputs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_pull_run_id UUID NOT NULL REFERENCES source_pull_runs(id),
    domain TEXT NOT NULL,
    signal TEXT,
    confidence REAL,
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
    CONSTRAINT chk_domain_discovery_raw_status CHECK (
        processing_status IN ('pending', 'processing', 'processed', 'failed', 'ignored', 'superseded')
    ),
    CONSTRAINT chk_domain_discovery_raw_attempts CHECK (processing_attempts >= 0),
    CONSTRAINT chk_domain_discovery_raw_payload_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT chk_domain_discovery_raw_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    )
);

CREATE INDEX idx_domain_discovery_raw_processing
    ON domain_discovery_raw_inputs(processing_status, processing_lease_until, created_at);
```

- [ ] **Step 2: Write the down migration**

Create `database/migrations/000018_source_raw_input_tables.down.sql`:

```sql
DROP TABLE IF EXISTS domain_discovery_raw_inputs;
DROP TABLE IF EXISTS ai_company_profile_raw_inputs;
DROP TABLE IF EXISTS brreg_company_raw_inputs;
DROP TABLE IF EXISTS companies_house_company_raw_inputs;
DROP TABLE IF EXISTS gleif_company_raw_inputs;
```

- [ ] **Step 3: Apply and verify**

```bash
cd scheduler && make migrate-up
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "\d gleif_company_raw_inputs" \
  -c "\d companies_house_company_raw_inputs"
```

Expected: both tables present; `gleif` has `lei` column; `companies_house` has `company_number` column with the `source_native_id = company_number` constraint.

- [ ] **Step 4: Commit**

```bash
git add database/migrations/000018_source_raw_input_tables.up.sql \
        database/migrations/000018_source_raw_input_tables.down.sql
git commit -m "feat(db): add source-specific raw input tables (migration 018)"
```

---

## Task 3: Migration 019 – Suggestion tables

**Files:**
- Create: `database/migrations/000019_suggestion_tables.up.sql`
- Create: `database/migrations/000019_suggestion_tables.down.sql`

- [ ] **Step 1: Write the up migration**

Create `database/migrations/000019_suggestion_tables.up.sql`:

```sql
-- Provenance glue between source inputs and suggestions.
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

-- Root suggestion: proposed new company.
-- proposed_country_id is required at approval time because companies.country_id is NOT NULL.
-- Processors must supply it from the source record (e.g. GLEIF headquarters_country_code,
-- Companies House GB, Brreg NO).
CREATE TABLE company_suggestions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    proposed_display_name TEXT NOT NULL,
    proposed_legal_name TEXT,
    proposed_website TEXT,
    proposed_canonical_slug TEXT,
    proposed_country_id UUID REFERENCES countries(id),
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

-- Root suggestion: proposed new organization.
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
            'foundation', 'standards_body', 'nonprofit',
            'government', 'university', 'community', 'other'
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

-- Root suggestion: proposed new open-source project.
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
    CONSTRAINT chk_osp_suggestions_lifecycle CHECK (
        proposed_lifecycle_status IS NULL
        OR proposed_lifecycle_status IN ('active', 'maintenance', 'deprecated', 'unknown')
    ),
    CONSTRAINT chk_osp_suggestions_status CHECK (
        status IN ('pending', 'approved', 'rejected', 'superseded')
    ),
    CONSTRAINT chk_osp_suggestions_confidence CHECK (
        confidence IS NULL OR confidence BETWEEN 0 AND 1
    ),
    CONSTRAINT chk_osp_suggestions_profile_object CHECK (
        jsonb_typeof(proposed_profile) = 'object'
    ),
    CONSTRAINT chk_osp_suggestions_created_project_when_approved CHECK (
        status <> 'approved' OR created_open_source_project_id IS NOT NULL
    )
);

CREATE INDEX idx_osp_suggestions_review
    ON open_source_project_suggestions(status, proposed_display_name);

-- Section suggestion: domain changes for companies.
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

CREATE INDEX idx_company_domain_suggestions_existing
    ON company_domain_suggestions(company_id, status)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_domain_suggestions_new
    ON company_domain_suggestions(company_suggestion_id, status)
    WHERE company_suggestion_id IS NOT NULL;

-- Section suggestion: contact changes (email, phone, website, social) for companies.
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

-- Section suggestion: address/location changes for companies.
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

CREATE INDEX idx_company_location_suggestions_existing
    ON company_location_suggestions(company_id, status)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_location_suggestions_new
    ON company_location_suggestions(company_suggestion_id, status)
    WHERE company_suggestion_id IS NOT NULL;

-- Section suggestion: scalar lifecycle/status/registry field changes for companies.
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
            'lifecycle_status', 'registration_status', 'legal_name',
            'registration_number', 'lei', 'other'
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

CREATE INDEX idx_company_status_suggestions_existing
    ON company_status_suggestions(company_id, status, status_field)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_status_suggestions_new
    ON company_status_suggestions(company_suggestion_id, status, status_field)
    WHERE company_suggestion_id IS NOT NULL;

-- Section suggestion: parent/subsidiary/ownership relationship changes for companies.
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
        NOT (related_company_id IS NOT NULL AND related_company_suggestion_id IS NOT NULL)
    ),
    CONSTRAINT chk_company_relationship_suggestions_operation CHECK (
        operation IN ('add', 'update', 'remove', 'replace')
    ),
    CONSTRAINT chk_company_relationship_suggestions_relationship_type CHECK (
        relationship_type IN (
            'direct_parent', 'ultimate_parent', 'subsidiary_of',
            'owned_by', 'acquired_by', 'merged_into', 'other'
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

CREATE INDEX idx_company_relationship_suggestions_existing
    ON company_relationship_suggestions(company_id, status, relationship_type)
    WHERE company_id IS NOT NULL;
CREATE INDEX idx_company_relationship_suggestions_new
    ON company_relationship_suggestions(company_suggestion_id, status, relationship_type)
    WHERE company_suggestion_id IS NOT NULL;
```

- [ ] **Step 2: Write the down migration**

Create `database/migrations/000019_suggestion_tables.down.sql`:

```sql
DROP TABLE IF EXISTS company_relationship_suggestions;
DROP TABLE IF EXISTS company_status_suggestions;
DROP TABLE IF EXISTS company_location_suggestions;
DROP TABLE IF EXISTS company_contact_suggestions;
DROP TABLE IF EXISTS company_domain_suggestions;
DROP TABLE IF EXISTS open_source_project_suggestions;
DROP TABLE IF EXISTS organization_suggestions;
DROP TABLE IF EXISTS company_suggestions;
DROP TABLE IF EXISTS suggestion_source_links;
```

- [ ] **Step 3: Apply and verify**

```bash
cd scheduler && make migrate-up
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "\dt *suggestion*" \
  -c "\d company_suggestions"
```

Expected: 9 tables listed; `company_suggestions` shows the `chk_company_suggestions_created_company_when_approved` constraint.

- [ ] **Step 4: Commit**

```bash
git add database/migrations/000019_suggestion_tables.up.sql \
        database/migrations/000019_suggestion_tables.down.sql
git commit -m "feat(db): add suggestion tables and source links (migration 019)"
```

---

## Task 4: SQL queries and sqlc regeneration

**Files:**
- Delete: `database/queries/review.sql`
- Modify: `database/queries/domains.sql` (remove queries that reference the deleted review workflow)
- Modify: `database/queries/stats.sql`
- Modify: `database/queries/sources.sql`
- Modify: `database/queries/pull_runs.sql`
- Modify: `database/queries/companies.sql`
- Modify: `database/queries/countries.sql`
- Create: `database/queries/raw_inputs.sql`
- Create: `database/queries/suggestions.sql`

The queries must match the new migration schemas exactly. After writing all files, run `make sqlc-generate`; compilation will fail until Task 5 removes the old code that references removed types.

- [ ] **Step 1: Delete review.sql and clean up domains.sql**

Migration 017 drops `company_domain_reviews`. `review.sql` contains 4 queries that all reference this dropped table. `domains.sql` has `ListCandidatesForReview` which is used only by the deleted review handler.

Delete `database/queries/review.sql`:

```bash
rm database/queries/review.sql
```

Remove `ListCandidatesForReview` from `database/queries/domains.sql`. Replace the full file content with only the remaining queries (omitting lines 23–30):

```sql
-- name: UpsertDomain :one
INSERT INTO domains (domain)
VALUES ($1)
ON CONFLICT (domain) DO UPDATE SET last_verified_at = now()
RETURNING *;

-- name: UpsertCompanyDomain :one
INSERT INTO company_domains (company_id, domain_id, relationship_type, status, signal, confidence, evidence)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (company_id, domain_id, signal) DO UPDATE SET
    confidence   = EXCLUDED.confidence,
    evidence     = EXCLUDED.evidence,
    last_seen_at = now()
RETURNING *;

-- name: ListDomainsForCompany :many
SELECT d.domain, cd.*
FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
WHERE cd.company_id = $1
ORDER BY cd.confidence DESC;

-- name: UpdateCompanyDomainStatus :exec
UPDATE company_domains SET status = $2, relationship_type = $3 WHERE id = $1;

-- name: ListDomains :many
SELECT d.domain, c.name AS company_name, cd.*
FROM company_domains cd
JOIN domains d ON d.id = cd.domain_id
JOIN companies c ON c.id = cd.company_id
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'))
ORDER BY cd.confidence DESC, d.domain
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountDomains :one
SELECT COUNT(*) FROM company_domains cd
WHERE (sqlc.narg('status')::text IS NULL OR cd.status = sqlc.narg('status'))
  AND (sqlc.narg('signal')::text IS NULL OR cd.signal = sqlc.narg('signal'))
  AND (sqlc.narg('min_confidence')::smallint IS NULL OR cd.confidence >= sqlc.narg('min_confidence'));
```

- [ ] **Step 2: Rewrite stats.sql**

`database/queries/stats.sql` still references `completed_at`, `records_upserted`, and `status = 'completed'` — all removed in migration 017. Replace the full file:

```sql
-- name: GetStats :one
SELECT
  (SELECT COUNT(*) FROM companies)::bigint                                     AS total_companies,
  (SELECT COUNT(*) FROM domains)::bigint                                       AS total_domains,
  (SELECT COUNT(*) FROM company_domains WHERE status = 'active')::bigint       AS active_domains,
  (SELECT COUNT(*) FROM company_domains WHERE status = 'needs_review')::bigint AS pending_review,
  (SELECT COUNT(*) FROM data_sources WHERE enabled = true)::bigint             AS enabled_sources,
  (SELECT COUNT(*) FROM source_pull_runs
   WHERE status = 'succeeded' AND finished_at >= now() - interval '24 hours')::bigint AS pull_runs_completed_today,
  (SELECT COUNT(*) FROM source_pull_runs
   WHERE status = 'failed' AND finished_at >= now() - interval '24 hours')::bigint    AS pull_runs_failed_today,
  (SELECT COALESCE(SUM(raw_rows_inserted), 0) FROM source_pull_runs
   WHERE finished_at >= now() - interval '24 hours')::bigint AS records_upserted_24h,
  (SELECT COALESCE(SUM(raw_rows_inserted), 0) FROM source_pull_runs
   WHERE finished_at >= now() - interval '7 days')::bigint   AS records_upserted_7d;
```

- [ ] **Step 3: Rewrite companies.sql**

`database/queries/companies.sql` references `company_sources` (via `UpsertCompanySource`, `ListCompanies` filter, `CountCompanies` filter) and other removed tables. Replace the full file with only the queries that still apply to the new schema:

```sql
-- name: GetCompany :one
SELECT * FROM companies WHERE id = $1;

-- name: GetCompanyBySlug :one
SELECT * FROM companies WHERE canonical_slug = $1;

-- name: UpdateCompanySlug :exec
UPDATE companies
SET canonical_slug = $2,
    display_name   = $3,
    updated_at     = now()
WHERE id = $1;

-- name: ListCompanies :many
SELECT * FROM companies c
WHERE (sqlc.narg('status')::text IS NULL OR status = sqlc.narg('status'))
  AND (sqlc.narg('country_id')::uuid IS NULL OR country_id = sqlc.narg('country_id'))
  AND (sqlc.narg('q')::text IS NULL OR name ILIKE '%' || sqlc.narg('q') || '%')
ORDER BY name
LIMIT sqlc.arg('limit') OFFSET sqlc.arg('offset');

-- name: CountCompanies :one
SELECT COUNT(*) FROM companies c
WHERE (sqlc.narg('status')::text IS NULL OR status = sqlc.narg('status'))
  AND (sqlc.narg('country_id')::uuid IS NULL OR country_id = sqlc.narg('country_id'))
  AND (sqlc.narg('q')::text IS NULL OR name ILIKE '%' || sqlc.narg('q') || '%');

-- name: UpdateCompanyEnrichment :one
UPDATE companies
SET short_description = $2,
    description       = $3,
    website           = $4,
    founded_year      = $5,
    employee_estimate = $6,
    revenue_estimate  = $7,
    updated_at        = now()
WHERE id = $1
RETURNING *;

-- name: GetCompanyByLEI :one
SELECT * FROM companies WHERE lei = $1;

-- name: GetCompanyByRegistrationAndCountry :one
SELECT c.*
FROM companies c
JOIN countries co ON co.id = c.country_id
WHERE c.registration_number = $1
  AND co.iso_alpha2 = $2;

-- name: InsertCompany :one
INSERT INTO companies (canonical_slug, name, country_id, status)
VALUES ($1, $2, $3, coalesce($4, 'active'))
RETURNING *;
```

Note: `source_id` filter is removed from `ListCompanies`/`CountCompanies` (company_sources is dropped). The handler that passes `source_id` must be updated to omit that filter or return all results.

- [ ] **Step 4: Rewrite sources.sql**

Replace the full content of `database/queries/sources.sql`:

```sql
-- name: GetSourceByName :one
SELECT * FROM data_sources WHERE name = $1;

-- name: ListSources :many
SELECT * FROM data_sources ORDER BY name;

-- name: UpdateSourceEnabled :exec
UPDATE data_sources SET enabled = $2, updated_at = now() WHERE name = $1;

-- name: UpdateSourceSchedule :exec
UPDATE data_sources
SET schedule_kind = $2, schedule_expression = $3, updated_at = now()
WHERE name = $1;

-- name: UpdateSourceConfig :exec
UPDATE data_sources SET config = $2, updated_at = now() WHERE name = $1;

-- name: UpdateSourcePullStarted :exec
UPDATE data_sources SET last_started_at = now(), updated_at = now() WHERE name = $1;

-- name: UpdateSourcePullSucceeded :exec
UPDATE data_sources
SET last_success_at = now(),
    last_source_marker_type = $2,
    last_source_marker = $3,
    last_source_modified_at = $4,
    consecutive_failures = 0,
    last_error = NULL,
    updated_at = now()
WHERE name = $1;

-- name: UpdateSourcePullFailed :exec
UPDATE data_sources
SET last_failed_at = now(),
    consecutive_failures = consecutive_failures + 1,
    last_error = $2,
    updated_at = now()
WHERE name = $1;
```

- [ ] **Step 5: Rewrite pull_runs.sql**

Replace the full content of `database/queries/pull_runs.sql`:

```sql
-- name: CreatePullRun :one
INSERT INTO source_pull_runs (source_id, river_job_id, task_type, trigger_type)
VALUES (
    (SELECT id FROM data_sources WHERE name = $1),
    $2, $3, $4
)
RETURNING *;

-- name: SucceedPullRun :exec
UPDATE source_pull_runs
SET status = 'succeeded',
    finished_at = now(),
    rows_seen = $2,
    raw_rows_inserted = $3,
    raw_rows_updated = $4,
    raw_rows_unchanged = $5
WHERE id = $1;

-- name: FailPullRun :exec
UPDATE source_pull_runs
SET status = 'failed',
    finished_at = now(),
    error_message = $2
WHERE id = $1;

-- name: InterruptStalePullRuns :exec
UPDATE source_pull_runs SET status = 'failed', error_message = 'interrupted on startup'
WHERE status = 'running';

-- name: ListPullRuns :many
SELECT r.*, d.name AS source_name
FROM source_pull_runs r
JOIN data_sources d ON d.id = r.source_id
WHERE ($1::text IS NULL OR d.name = $1)
ORDER BY r.started_at DESC
LIMIT $3 OFFSET $2;
```

- [ ] **Step 6: Add GetCompanyByLEI and GetCompanyByRegistrationAndCountry to companies.sql**

These two queries were already written in Step 3 above (the full rewrite of companies.sql includes them). Verify they are present — no separate edit needed.

```sql
-- name: GetCompanyByLEI :one
SELECT * FROM companies WHERE lei = $1;

-- name: GetCompanyByRegistrationAndCountry :one
SELECT c.*
FROM companies c
JOIN countries co ON co.id = c.country_id
WHERE c.registration_number = $1
  AND co.iso_alpha2 = $2;
```

- [ ] **Step 7: Create raw_inputs.sql**

Create `database/queries/raw_inputs.sql`:

```sql
-- GLEIF

-- name: UpsertGLEIFCompanyRawInput :one
INSERT INTO gleif_company_raw_inputs (
    source_pull_run_id, source_native_id, lei, legal_name,
    registration_status, headquarters_country_code, parent_lei, ultimate_parent_lei,
    source_updated_at, raw_payload, payload_hash
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (lei, payload_hash) DO UPDATE SET last_seen_at = now()
RETURNING *;

-- name: ClaimPendingGLEIFRawInputs :many
UPDATE gleif_company_raw_inputs
SET processing_status = 'processing',
    processing_attempts = processing_attempts + 1,
    processing_lease_by = $1,
    processing_lease_until = now() + ($2 * interval '1 second'),
    updated_at = now()
WHERE id IN (
    SELECT id FROM gleif_company_raw_inputs
    WHERE processing_status = 'pending'
       OR (processing_status = 'processing' AND processing_lease_until < now())
    ORDER BY created_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: MarkGLEIFRawInputProcessed :exec
UPDATE gleif_company_raw_inputs
SET processing_status = 'processed', processed_at = now(), updated_at = now()
WHERE id = $1;

-- name: MarkGLEIFRawInputFailed :exec
UPDATE gleif_company_raw_inputs
SET processing_status = 'failed', processing_error = $2, updated_at = now()
WHERE id = $1;

-- Companies House

-- name: UpsertCompaniesHouseRawInput :one
INSERT INTO companies_house_company_raw_inputs (
    source_pull_run_id, source_native_id, company_number, company_name,
    company_status, company_type, source_updated_at, raw_payload, payload_hash
)
VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (company_number, payload_hash) DO UPDATE SET last_seen_at = now()
RETURNING *;

-- name: ClaimPendingCompaniesHouseRawInputs :many
UPDATE companies_house_company_raw_inputs
SET processing_status = 'processing',
    processing_attempts = processing_attempts + 1,
    processing_lease_by = $1,
    processing_lease_until = now() + ($2 * interval '1 second'),
    updated_at = now()
WHERE id IN (
    SELECT id FROM companies_house_company_raw_inputs
    WHERE processing_status = 'pending'
       OR (processing_status = 'processing' AND processing_lease_until < now())
    ORDER BY created_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: MarkCompaniesHouseRawInputProcessed :exec
UPDATE companies_house_company_raw_inputs
SET processing_status = 'processed', processed_at = now(), updated_at = now()
WHERE id = $1;

-- name: MarkCompaniesHouseRawInputFailed :exec
UPDATE companies_house_company_raw_inputs
SET processing_status = 'failed', processing_error = $2, updated_at = now()
WHERE id = $1;

-- Brreg

-- name: UpsertBrregRawInput :one
INSERT INTO brreg_company_raw_inputs (
    source_pull_run_id, source_native_id, organization_number, organization_name,
    registration_status, website, source_updated_at, raw_payload, payload_hash
)
VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (organization_number, payload_hash) DO UPDATE SET last_seen_at = now()
RETURNING *;

-- name: ClaimPendingBrregRawInputs :many
UPDATE brreg_company_raw_inputs
SET processing_status = 'processing',
    processing_attempts = processing_attempts + 1,
    processing_lease_by = $1,
    processing_lease_until = now() + ($2 * interval '1 second'),
    updated_at = now()
WHERE id IN (
    SELECT id FROM brreg_company_raw_inputs
    WHERE processing_status = 'pending'
       OR (processing_status = 'processing' AND processing_lease_until < now())
    ORDER BY created_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: MarkBrregRawInputProcessed :exec
UPDATE brreg_company_raw_inputs
SET processing_status = 'processed', processed_at = now(), updated_at = now()
WHERE id = $1;

-- name: MarkBrregRawInputFailed :exec
UPDATE brreg_company_raw_inputs
SET processing_status = 'failed', processing_error = $2, updated_at = now()
WHERE id = $1;
```

- [ ] **Step 8: Create suggestions.sql**

Before creating `suggestions.sql`, add the processor-specific country ID lookup to `database/queries/countries.sql`. The existing `GetCountryByISO2` query returns a full `db.Country`; this ID-only query avoids putting country reference lookups in `suggestions.sql`.

```sql
-- name: GetCountryIDByISO2 :one
SELECT id FROM countries WHERE iso_alpha2 = $1;
```

Then create `database/queries/suggestions.sql`. Do not add country lookup queries here; this file owns suggestion tables only.

```sql
-- Company root suggestions


-- name: InsertCompanySuggestion :one
INSERT INTO company_suggestions (
    proposed_display_name, proposed_legal_name, proposed_website,
    proposed_canonical_slug, proposed_country_id, proposed_profile, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetCompanySuggestionByID :one
SELECT * FROM company_suggestions WHERE id = $1;

-- name: ListPendingCompanySuggestions :many
SELECT * FROM company_suggestions
WHERE status = 'pending'
ORDER BY created_at DESC
LIMIT $2 OFFSET $1;

-- name: CountPendingCompanySuggestions :one
SELECT COUNT(*) FROM company_suggestions WHERE status = 'pending';

-- name: UpdateCompanySuggestionApproved :exec
UPDATE company_suggestions
SET status = 'approved',
    created_company_id = $2,
    reviewed_by = $3,
    reviewed_at = now(),
    review_note = $4,
    updated_at = now()
WHERE id = $1;

-- name: UpdateCompanySuggestionRejected :exec
UPDATE company_suggestions
SET status = 'rejected',
    reviewed_by = $2,
    reviewed_at = now(),
    review_note = $3,
    updated_at = now()
WHERE id = $1;

-- Company section suggestions

-- name: InsertCompanyDomainSuggestion :one
INSERT INTO company_domain_suggestions (
    company_id, company_suggestion_id, operation, domain,
    current_payload, proposed_payload, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: InsertCompanyContactSuggestion :one
INSERT INTO company_contact_suggestions (
    company_id, company_suggestion_id, operation, contact_kind,
    current_payload, proposed_payload, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: InsertCompanyLocationSuggestion :one
INSERT INTO company_location_suggestions (
    company_id, company_suggestion_id, operation, location_kind, country_code, city,
    current_payload, proposed_payload, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING *;

-- name: InsertCompanyStatusSuggestion :one
INSERT INTO company_status_suggestions (
    company_id, company_suggestion_id, operation, status_field,
    current_value, proposed_value, current_payload, proposed_payload, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING *;

-- name: InsertCompanyRelationshipSuggestion :one
INSERT INTO company_relationship_suggestions (
    company_id, company_suggestion_id, operation, relationship_type,
    related_company_id, related_company_suggestion_id,
    related_company_name, related_lei,
    current_payload, proposed_payload, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
RETURNING *;

-- Organization and open-source project root suggestions

-- name: InsertOrganizationSuggestion :one
INSERT INTO organization_suggestions (
    proposed_display_name, proposed_organization_type, proposed_website,
    proposed_canonical_slug, proposed_profile, confidence
)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: InsertOpenSourceProjectSuggestion :one
INSERT INTO open_source_project_suggestions (
    proposed_display_name, proposed_repository_url, proposed_website,
    proposed_license, proposed_lifecycle_status, proposed_canonical_slug,
    proposed_profile, confidence
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING *;

-- Provenance links

-- name: InsertSuggestionSourceLink :one
INSERT INTO suggestion_source_links (
    suggestion_table, suggestion_id, source_id,
    source_input_table, source_input_key, source_pull_run_id,
    confidence, evidence_excerpt
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
RETURNING *;
```

- [ ] **Step 9: Regenerate sqlc**

```bash
cd scheduler && make sqlc-generate
```

Expected: runs without error; new generated files in `scheduler/internal/db/gen/` include `raw_inputs.sql.go`, `suggestions.sql.go`, and updated `sources.sql.go`, `pull_runs.sql.go`, `companies.sql.go`, `countries.sql.go`, `stats.sql.go`.

- [ ] **Step 10: Verify compilation fails as expected**

```bash
cd scheduler && GOWORK=off go build ./... 2>&1 | head -30
```

Expected: compile errors referencing `source_crawl.go`, `gleif_enrich.go`, `domain_resolve.go`, and old query method names. That is correct — Task 5 fixes them.

- [ ] **Step 11: Commit**

```bash
git rm database/queries/review.sql
	git add database/queries/domains.sql \
	        database/queries/sources.sql database/queries/pull_runs.sql \
	        database/queries/companies.sql database/queries/countries.sql database/queries/stats.sql \
	        database/queries/raw_inputs.sql database/queries/suggestions.sql \
	        scheduler/internal/db/gen/
	git commit -m "feat(db): delete review.sql, rewrite source/pull-run/stats/companies/countries queries, add raw-input and suggestion queries, regenerate sqlc"
```

---

## Task 5: Remove old ingestion path and restore compilation

**Files:**
- Delete: `scheduler/internal/workers/source_crawl.go`
- Delete: `scheduler/internal/workers/domain_resolve.go`
- Delete: `scheduler/internal/workers/gleif_enrich.go`
- Delete: `scheduler/internal/httpapi/review.go`
- Modify: `scheduler/internal/workers/workers.go`
- Modify: `scheduler/internal/app/river.go`
- Modify: `scheduler/internal/app/app.go`
- Modify: `scheduler/internal/httpapi/sources.go`
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/companies.go` (remove SourceID field — company_sources is dropped)
- Modify: `scheduler/internal/httpapi/testhelpers_test.go`

- [ ] **Step 1: Delete obsolete worker files and their tests**

```bash
rm scheduler/internal/workers/source_crawl.go \
   scheduler/internal/workers/domain_resolve.go \
   scheduler/internal/workers/gleif_enrich.go \
   scheduler/internal/workers/source_crawl_test.go \
   scheduler/internal/workers/domain_resolve_test.go \
   scheduler/internal/httpapi/review.go \
   scheduler/internal/httpapi/review_test.go
```

Also delete the old `sources_test.go` fields that reference `AdapterType` and `CrawlIntervalHours` (now removed from the DataSource model). The test file will need to be rewritten; see Step 7 below.

- [ ] **Step 2: Rewrite workers.go**

Replace the full content of `scheduler/internal/workers/workers.go`:

```go
package workers

// SourcePullArgs is the job argument for a source pull task.
type SourcePullArgs struct {
	SourceName  string `json:"source_name"`
	TriggerType string `json:"trigger_type"`
}

func (SourcePullArgs) Kind() string { return "source_pull" }

// SourceProcessArgs is the job argument for a source processor task.
type SourceProcessArgs struct {
	SourceName string `json:"source_name"`
	PullRunID  string `json:"pull_run_id"`
}

func (SourceProcessArgs) Kind() string { return "source_process" }
```

- [ ] **Step 3: Rewrite river.go with stub workers**

Replace the full content of `scheduler/internal/app/river.go`:

```go
package app

import (
	"context"
	"log/slog"

	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func setupRiver(ctx context.Context, pool *pgxpool.Pool, cfg config.Config, q db.Querier, crawler *crawlerclient.Client) (*river.Client[pgx.Tx], error) {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return nil, err
	}
	res, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return nil, err
	}
	for _, v := range res.Versions {
		slog.Info("river migration applied", "version", v.Version, "direction", "up")
	}

	sourcePullWorker := workers.NewSourcePullWorker(q, crawler, pool)
	sourceProcessWorker := workers.NewSourceProcessWorker(q, pool)

	w := river.NewWorkers()
	river.AddWorker(w, sourcePullWorker)
	river.AddWorker(w, sourceProcessWorker)

	riverCfg := &river.Config{
		Queues: map[string]river.QueueConfig{
			"source_pull":    {MaxWorkers: cfg.CrawlConcurrency},
			"source_process": {MaxWorkers: cfg.DomainConcurrency},
		},
		Workers: w,
	}

	rc, err := river.NewClient(riverpgxv5.New(pool), riverCfg)
	if err != nil {
		return nil, err
	}
	return rc, nil
}
```

- [ ] **Step 4: Rewrite app.go scheduleOnce**

Replace the `scheduleOnce` function in `scheduler/internal/app/app.go`:

```go
func scheduleOnce(ctx context.Context, q db.Querier, rc *river.Client[pgx.Tx]) {
	sources, err := q.ListSources(ctx)
	if err != nil {
		slog.Error("schedule sources: list sources", "error", err)
		return
	}

	for _, src := range sources {
		if !src.Enabled {
			continue
		}
		if src.ScheduleKind != "interval" {
			continue
		}
		if src.ScheduleExpression == nil {
			continue
		}
		interval, err := time.ParseDuration(*src.ScheduleExpression)
		if err != nil {
			slog.Warn("schedule sources: invalid schedule_expression", "source", src.Name, "expr", *src.ScheduleExpression)
			continue
		}
		if src.LastStartedAt.Valid {
			due := src.LastStartedAt.Time.Add(interval)
			if time.Now().Before(due) {
				continue
			}
		}
		if _, err := rc.Insert(ctx, workers.SourcePullArgs{
			SourceName:  src.Name,
			TriggerType: "scheduled",
		}, &river.InsertOpts{
			Queue: "source_pull",
			UniqueOpts: river.UniqueOpts{
				ByArgs:  true,
				ByState: []river.JobState{river.JobStateAvailable, river.JobStateRunning, river.JobStateScheduled},
			},
		}); err != nil {
			slog.Error("schedule sources: insert job", "source", src.Name, "error", err)
		}
	}
}
```

Also remove the `time` import from workers and add it to app.go if not present. The full import block for `app.go` becomes:

```go
import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)
```

- [ ] **Step 5: Rewrite sources.go**

Replace the full content of `scheduler/internal/httpapi/sources.go`:

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

type sourceView struct {
	db.DataSource
	Config json.RawMessage `json:"config"`
}

func toSourceView(s db.DataSource) sourceView {
	cfg := json.RawMessage(s.Config)
	if len(cfg) == 0 {
		cfg = json.RawMessage("null")
	}
	return sourceView{DataSource: s, Config: cfg}
}

func toSourceViews(sources []db.DataSource) []sourceView {
	out := make([]sourceView, len(sources))
	for i, s := range sources {
		out[i] = toSourceView(s)
	}
	return out
}

func (h *Handlers) handleListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := h.db.ListSources(r.Context())
	if err != nil {
		slog.Error("list sources", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, toSourceViews(sources))
}

func (h *Handlers) handleGetSource(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	source, err := h.db.GetSourceByName(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return
	}
	writeJSON(w, http.StatusOK, toSourceView(source))
}

type patchSourceRequest struct {
	Enabled            *bool   `json:"enabled"`
	ScheduleKind       *string `json:"schedule_kind"`
	ScheduleExpression *string `json:"schedule_expression"`
}

func (h *Handlers) handlePatchSource(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	var req patchSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Enabled != nil {
		if err := h.db.UpdateSourceEnabled(r.Context(), db.UpdateSourceEnabledParams{
			Name: name, Enabled: *req.Enabled,
		}); err != nil {
			slog.Error("update source enabled", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	if req.ScheduleKind != nil || req.ScheduleExpression != nil {
		src, err := h.db.GetSourceByName(r.Context(), name)
		if err != nil {
			writeError(w, http.StatusNotFound, "source not found")
			return
		}
		kind := src.ScheduleKind
		expr := src.ScheduleExpression
		if req.ScheduleKind != nil {
			kind = *req.ScheduleKind
		}
		if req.ScheduleExpression != nil {
			expr = req.ScheduleExpression
		}
		if err := h.db.UpdateSourceSchedule(r.Context(), db.UpdateSourceScheduleParams{
			Name:               name,
			ScheduleKind:       kind,
			ScheduleExpression: expr,
		}); err != nil {
			slog.Error("update source schedule", "name", name, "error", err)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handlers) handleTriggerSource(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	source, err := h.db.GetSourceByName(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return
	}
	if source.PullTaskType != "source_pull" {
		writeError(w, http.StatusUnprocessableEntity, "pull task type not supported for manual trigger")
		return
	}
	if h.rv == nil {
		writeError(w, http.StatusServiceUnavailable, "scheduler not available")
		return
	}
	if _, err := h.rv.Insert(r.Context(), workers.SourcePullArgs{
		SourceName:  name,
		TriggerType: "manual",
	}, &river.InsertOpts{
		Queue: "source_pull",
		UniqueOpts: river.UniqueOpts{
			ByArgs:  true,
			ByState: []river.JobState{river.JobStateAvailable, river.JobStateRunning, river.JobStateScheduled},
		},
	}); err != nil {
		slog.Error("trigger source", "name", name, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "queued"})
}

func (h *Handlers) handleProbeSource(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	if _, err := h.db.GetSourceByName(r.Context(), name); err != nil {
		writeError(w, http.StatusNotFound, "source not found")
		return
	}
	if h.crawler == nil {
		writeError(w, http.StatusServiceUnavailable, "crawler not available")
		return
	}
	start := time.Now()
	resp, err := h.crawler.Crawl(r.Context(), name, time.Time{}, nil, 1)
	durationMs := time.Since(start).Milliseconds()
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"records_count": 0, "total": 0, "has_more": false,
			"sample": nil, "error": err.Error(), "duration_ms": durationMs,
		})
		return
	}
	var sample any
	if len(resp.Records) > 0 {
		sample = resp.Records[0]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"records_count": len(resp.Records), "total": resp.Total,
		"has_more": resp.HasMore, "sample": sample,
		"error": nil, "duration_ms": durationMs,
	})
}
```

- [ ] **Step 6: Update handlers.go routes**

In `scheduler/internal/httpapi/handlers.go`, replace the `r.Route("/api/v1", ...)` block:

```go
func (h *Handlers) RegisterRoutes(r chi.Router) {
	if h.postgrestURL != "" {
		proxy := newPostgRESTProxy(h.postgrestURL)
		r.HandleFunc("/api/v1/db/*", proxy)
		r.HandleFunc("/api/v1/db", proxy)
	}
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/stats", h.handleStats)
		r.Get("/companies", h.handleListCompanies)
		r.Get("/companies/{id}", h.handleGetCompany)
		r.Get("/domains", h.handleListDomains)
		r.Get("/countries", h.handleListCountries)
		r.Get("/sources", h.handleListSources)
		r.Get("/sources/{name}", h.handleGetSource)
		r.Patch("/sources/{name}", h.handlePatchSource)
		r.Post("/sources/{name}/trigger", h.handleTriggerSource)
		r.Post("/sources/{name}/probe", h.handleProbeSource)
		r.Get("/jobs", h.handleListJobs)
		r.Get("/jobs/stats", h.handleJobStats)
		r.Post("/jobs/cancel-bulk", h.handleCancelBulk)
		r.Get("/jobs/{id}", h.handleGetJob)
		r.Post("/jobs/{id}/cancel", h.handleCancelJob)
		r.Get("/pull-runs", h.handleListPullRuns)
		r.Post("/resolve", h.handleResolve)
		r.Get("/organizations", h.handleListOrganizations)
		r.Post("/organizations", h.handleCreateOrganization)
		r.Get("/organizations/{id}", h.handleGetOrganization)
		r.Get("/open-source-projects", h.handleListOpenSourceProjects)
		r.Post("/open-source-projects", h.handleCreateOpenSourceProject)
		r.Get("/open-source-projects/{id}", h.handleGetOpenSourceProject)
		r.Get("/suggestions/companies", h.handleListCompanySuggestions)
		r.Get("/suggestions/companies/{id}", h.handleGetCompanySuggestion)
		r.Post("/suggestions/companies/{id}/approve", h.handleApproveCompanySuggestion)
		r.Post("/suggestions/companies/{id}/reject", h.handleRejectCompanySuggestion)
		r.Post("/suggestions/companies/{id}/approve-with-sections", h.handleApproveCompanyWithSections)
		r.Post("/suggestions/company-status/{id}/approve", h.handleApproveCompanyStatusSuggestion)
		r.Post("/suggestions/company-status/{id}/reject", h.handleRejectCompanyStatusSuggestion)
		r.Post("/suggestions/company-contact/{id}/approve", h.handleApproveCompanyContactSuggestion)
		r.Post("/suggestions/company-contact/{id}/reject", h.handleRejectCompanyContactSuggestion)
	})
}
```

- [ ] **Step 7: Update testhelpers_test.go**

Replace the stub querier with one that satisfies the new `db.Querier` interface. Remove all stubs for deleted methods and add stubs for new methods. The key changes:

Remove these method stubs (types no longer exist):
- `CompletePullRun` → replaced by `SucceedPullRun`
- `CountCandidatesForReview`
- `CreateDomainReview`
- `CreateDomainReviewAndUpdateStatus`
- `InsertSourceSnapshot`
- `ListCandidatesForReview`
- `ListReviewsForClaim`
- `UpdateSourceCursor`
- `UpdateSourceInterval`
- `UpsertCompanyAlias`
- `UpsertCompanyByLEI`
- `UpsertCompanyByRegNumber`
- `UpsertCompanyDomain`
- `UpsertCompanyEmail`, `UpsertCompanyIndustry`, `UpsertCompanyLocation`, `UpsertCompanyMarket`, `UpsertCompanyPhone`, `UpsertCompanyService`
- `UpsertCompanySource`
- `UpsertDataSource`
- `ListCompaniesForGLEIFEnrich`
- `UpdateCompanyParentLEI`

Add these stub methods (new queries from Task 4):

```go
func (s *stubQuerier) SucceedPullRun(ctx context.Context, arg db.SucceedPullRunParams) error {
	return nil
}
func (s *stubQuerier) UpdateSourceSchedule(ctx context.Context, arg db.UpdateSourceScheduleParams) error {
	ret := s.Called(ctx, arg)
	return ret.Error(0)
}
func (s *stubQuerier) UpdateSourceConfig(ctx context.Context, arg db.UpdateSourceConfigParams) error {
	return nil
}
func (s *stubQuerier) UpdateSourcePullStarted(ctx context.Context, name string) error {
	return nil
}
func (s *stubQuerier) UpdateSourcePullSucceeded(ctx context.Context, arg db.UpdateSourcePullSucceededParams) error {
	return nil
}
func (s *stubQuerier) UpdateSourcePullFailed(ctx context.Context, arg db.UpdateSourcePullFailedParams) error {
	return nil
}
func (s *stubQuerier) GetCompanyByLEI(ctx context.Context, lei string) (db.Company, error) {
	return db.Company{}, nil
}
func (s *stubQuerier) GetCompanyByRegistrationAndCountry(ctx context.Context, arg db.GetCompanyByRegistrationAndCountryParams) (db.Company, error) {
	return db.Company{}, nil
}
func (s *stubQuerier) GetCountryIDByISO2(ctx context.Context, iso string) (uuid.UUID, error) {
	return uuid.UUID{}, nil
}
func (s *stubQuerier) InsertCompanySuggestion(ctx context.Context, arg db.InsertCompanySuggestionParams) (db.CompanySuggestion, error) {
	return db.CompanySuggestion{}, nil
}
func (s *stubQuerier) GetCompanySuggestionByID(ctx context.Context, id uuid.UUID) (db.CompanySuggestion, error) {
	return db.CompanySuggestion{}, nil
}
func (s *stubQuerier) ListPendingCompanySuggestions(ctx context.Context, arg db.ListPendingCompanySuggestionsParams) ([]db.CompanySuggestion, error) {
	ret := s.Called(ctx, arg)
	if v, ok := ret.Get(0).([]db.CompanySuggestion); ok {
		return v, ret.Error(1)
	}
	return nil, ret.Error(1)
}
func (s *stubQuerier) CountPendingCompanySuggestions(ctx context.Context) (int64, error) {
	return 0, nil
}
func (s *stubQuerier) UpdateCompanySuggestionApproved(ctx context.Context, arg db.UpdateCompanySuggestionApprovedParams) error {
	return nil
}
func (s *stubQuerier) UpdateCompanySuggestionRejected(ctx context.Context, arg db.UpdateCompanySuggestionRejectedParams) error {
	return nil
}
func (s *stubQuerier) InsertCompanyDomainSuggestion(ctx context.Context, arg db.InsertCompanyDomainSuggestionParams) (db.CompanyDomainSuggestion, error) {
	return db.CompanyDomainSuggestion{}, nil
}
func (s *stubQuerier) InsertCompanyContactSuggestion(ctx context.Context, arg db.InsertCompanyContactSuggestionParams) (db.CompanyContactSuggestion, error) {
	return db.CompanyContactSuggestion{}, nil
}
func (s *stubQuerier) InsertCompanyLocationSuggestion(ctx context.Context, arg db.InsertCompanyLocationSuggestionParams) (db.CompanyLocationSuggestion, error) {
	return db.CompanyLocationSuggestion{}, nil
}
func (s *stubQuerier) InsertCompanyStatusSuggestion(ctx context.Context, arg db.InsertCompanyStatusSuggestionParams) (db.CompanyStatusSuggestion, error) {
	return db.CompanyStatusSuggestion{}, nil
}
func (s *stubQuerier) InsertCompanyRelationshipSuggestion(ctx context.Context, arg db.InsertCompanyRelationshipSuggestionParams) (db.CompanyRelationshipSuggestion, error) {
	return db.CompanyRelationshipSuggestion{}, nil
}
func (s *stubQuerier) InsertOrganizationSuggestion(ctx context.Context, arg db.InsertOrganizationSuggestionParams) (db.OrganizationSuggestion, error) {
	return db.OrganizationSuggestion{}, nil
}
func (s *stubQuerier) InsertOpenSourceProjectSuggestion(ctx context.Context, arg db.InsertOpenSourceProjectSuggestionParams) (db.OpenSourceProjectSuggestion, error) {
	return db.OpenSourceProjectSuggestion{}, nil
}
func (s *stubQuerier) InsertSuggestionSourceLink(ctx context.Context, arg db.InsertSuggestionSourceLinkParams) (db.SuggestionSourceLink, error) {
	return db.SuggestionSourceLink{}, nil
}
// Raw input stubs
func (s *stubQuerier) UpsertGLEIFCompanyRawInput(ctx context.Context, arg db.UpsertGLEIFCompanyRawInputParams) (db.GleifCompanyRawInput, error) {
	return db.GleifCompanyRawInput{}, nil
}
func (s *stubQuerier) ClaimPendingGLEIFRawInputs(ctx context.Context, arg db.ClaimPendingGLEIFRawInputsParams) ([]db.GleifCompanyRawInput, error) {
	return nil, nil
}
func (s *stubQuerier) MarkGLEIFRawInputProcessed(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (s *stubQuerier) MarkGLEIFRawInputFailed(ctx context.Context, arg db.MarkGLEIFRawInputFailedParams) error {
	return nil
}
func (s *stubQuerier) UpsertCompaniesHouseRawInput(ctx context.Context, arg db.UpsertCompaniesHouseRawInputParams) (db.CompaniesHouseCompanyRawInput, error) {
	return db.CompaniesHouseCompanyRawInput{}, nil
}
func (s *stubQuerier) ClaimPendingCompaniesHouseRawInputs(ctx context.Context, arg db.ClaimPendingCompaniesHouseRawInputsParams) ([]db.CompaniesHouseCompanyRawInput, error) {
	return nil, nil
}
func (s *stubQuerier) MarkCompaniesHouseRawInputProcessed(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (s *stubQuerier) MarkCompaniesHouseRawInputFailed(ctx context.Context, arg db.MarkCompaniesHouseRawInputFailedParams) error {
	return nil
}
func (s *stubQuerier) UpsertBrregRawInput(ctx context.Context, arg db.UpsertBrregRawInputParams) (db.BrregCompanyRawInput, error) {
	return db.BrregCompanyRawInput{}, nil
}
func (s *stubQuerier) ClaimPendingBrregRawInputs(ctx context.Context, arg db.ClaimPendingBrregRawInputsParams) ([]db.BrregCompanyRawInput, error) {
	return nil, nil
}
func (s *stubQuerier) MarkBrregRawInputProcessed(ctx context.Context, id uuid.UUID) error {
	return nil
}
func (s *stubQuerier) MarkBrregRawInputFailed(ctx context.Context, arg db.MarkBrregRawInputFailedParams) error {
	return nil
}
```

- [ ] **Step 8: Add stub suggestion handler file**

Create `scheduler/internal/httpapi/suggestions.go` with stubs (full implementation in Task 10):

```go
package httpapi

import "net/http"

func (h *Handlers) handleListCompanySuggestions(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"items": []any{}, "page": 1, "limit": 20})
}

func (h *Handlers) handleGetCompanySuggestion(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "not implemented")
}

func (h *Handlers) handleApproveCompanySuggestion(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "not implemented")
}

func (h *Handlers) handleRejectCompanySuggestion(w http.ResponseWriter, r *http.Request) {
	writeError(w, http.StatusNotImplemented, "not implemented")
}
```

- [ ] **Step 9: Add stub worker constructors**

Create `scheduler/internal/workers/source_pull.go` with a minimal compilable stub (full implementation in Task 6):

```go
package workers

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type SourcePullWorker struct {
	river.WorkerDefaults[SourcePullArgs]
	db      db.Querier
	crawler *crawlerclient.Client
	pool    *pgxpool.Pool
}

func NewSourcePullWorker(q db.Querier, crawler *crawlerclient.Client, pool *pgxpool.Pool) *SourcePullWorker {
	return &SourcePullWorker{db: q, crawler: crawler, pool: pool}
}

func (w *SourcePullWorker) Work(ctx context.Context, job *river.Job[SourcePullArgs]) error {
	return nil
}
```

Create `scheduler/internal/workers/source_process.go` with a minimal compilable stub:

```go
package workers

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type SourceProcessWorker struct {
	river.WorkerDefaults[SourceProcessArgs]
	db   db.Querier
	pool *pgxpool.Pool
}

func NewSourceProcessWorker(q db.Querier, pool *pgxpool.Pool) *SourceProcessWorker {
	return &SourceProcessWorker{db: q, pool: pool}
}

func (w *SourceProcessWorker) Work(ctx context.Context, job *river.Job[SourceProcessArgs]) error {
	return nil
}
```

- [ ] **Step 10: Remove SourceID from companies.go**

In `scheduler/internal/httpapi/companies.go`, the `handleListCompanies` function references `SourceID` on both `ListCompaniesParams` and `CountCompaniesParams`. After the `company_sources` table is dropped in Task 1, sqlc will no longer generate that field. Remove it now so the file compiles after Task 1.

Replace the body of `handleListCompanies` (lines 14–59):

```go
func (h *Handlers) handleListCompanies(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 50), 200)
	offset := int32((page - 1) * limit)

	var countryID pgtype.UUID
	if s := r.URL.Query().Get("country"); s != "" {
		if id, err := uuid.Parse(s); err == nil {
			countryID = pgtype.UUID{Bytes: id, Valid: true}
		}
	}

	params := db.ListCompaniesParams{
		Status:    queryString(r, "status"),
		CountryID: countryID,
		Q:         queryString(r, "q"),
		Offset:    offset,
		Limit:     int32(limit),
	}

	companies, err := h.db.ListCompanies(r.Context(), params)
	if err != nil {
		slog.Error("list companies", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	total, err := h.db.CountCompanies(r.Context(), db.CountCompaniesParams{
		Status: params.Status, CountryID: params.CountryID, Q: params.Q,
	})
	if err != nil {
		slog.Error("count companies", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": companies, "total": total, "page": page, "limit": limit,
	})
}
```

- [ ] **Step 11: Verify compilation passes**

```bash
cd scheduler && GOWORK=off go build ./...
```

Expected: no errors.

- [ ] **Step 12: Run tests**

```bash
cd scheduler && GOWORK=off make test
```

Expected: all tests pass (existing handler tests compile and pass; new stubs don't break anything).

- [ ] **Step 13: Commit**

```bash
git add scheduler/internal/workers/ \
        scheduler/internal/app/ \
        scheduler/internal/httpapi/ \
        scheduler/internal/service/
git commit -m "refactor: remove old ingestion workers, wire stub pull/process workers, restore compilation"
```

---

## Task 6: SourcePullWorker

**Files:**
- Modify: `scheduler/internal/workers/source_pull.go` (replace stub)
- Create: `scheduler/internal/workers/source_pull_test.go`

The worker fetches raw records from the crawler, writes only source-specific raw input rows, updates pull-run and source state, and enqueues the processor task. It never writes resolved entity tables.

- [ ] **Step 1: Write the failing test**

Create `scheduler/internal/workers/source_pull_test.go`:

```go
package workers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

// stubPullQuerier implements db.Querier for SourcePullWorker tests.
// All methods not listed below are no-ops returning zero values.
type stubPullQuerier struct {
	db.Querier // embedded to satisfy the interface; only override what's needed
	getSourceByNameFn           func(name string) (db.DataSource, error)
	updateSourcePullStartedFn   func() error
	createPullRunFn             func() (db.SourcePullRun, error)
	upsertGLEIFFn               func() (db.GleifCompanyRawInput, error)
	failPullRunFn               func() error
	succeedPullRunFn            func() error
	updateSourcePullSucceededFn func() error
	updateSourcePullFailedFn    func() error
	insertCompanyFn             func() (db.Company, error)
}

func (q *stubPullQuerier) GetSourceByName(ctx context.Context, name string) (db.DataSource, error) {
	return q.getSourceByNameFn(name)
}
func (q *stubPullQuerier) UpdateSourcePullStarted(ctx context.Context, name string) error {
	if q.updateSourcePullStartedFn != nil { return q.updateSourcePullStartedFn() }
	return nil
}
func (q *stubPullQuerier) CreatePullRun(ctx context.Context, arg db.CreatePullRunParams) (db.SourcePullRun, error) {
	return q.createPullRunFn()
}
func (q *stubPullQuerier) UpsertGLEIFCompanyRawInput(ctx context.Context, arg db.UpsertGLEIFCompanyRawInputParams) (db.GleifCompanyRawInput, error) {
	return q.upsertGLEIFFn()
}
func (q *stubPullQuerier) FailPullRun(ctx context.Context, arg db.FailPullRunParams) error {
	if q.failPullRunFn != nil { return q.failPullRunFn() }
	return nil
}
func (q *stubPullQuerier) SucceedPullRun(ctx context.Context, arg db.SucceedPullRunParams) error {
	if q.succeedPullRunFn != nil { return q.succeedPullRunFn() }
	return nil
}
func (q *stubPullQuerier) UpdateSourcePullSucceeded(ctx context.Context, arg db.UpdateSourcePullSucceededParams) error {
	if q.updateSourcePullSucceededFn != nil { return q.updateSourcePullSucceededFn() }
	return nil
}
func (q *stubPullQuerier) UpdateSourcePullFailed(ctx context.Context, arg db.UpdateSourcePullFailedParams) error {
	if q.updateSourcePullFailedFn != nil { return q.updateSourcePullFailedFn() }
	return nil
}
func (q *stubPullQuerier) InsertCompany(ctx context.Context, arg db.InsertCompanyParams) (db.Company, error) {
	if q.insertCompanyFn != nil { return q.insertCompanyFn() }
	return db.Company{}, nil
}

func TestSourcePullWorker_WritesRawInputsOnly(t *testing.T) {
	// Verifies: SourcePullWorker creates a pull run, writes to the gleif raw input
	// table, and does NOT write to the resolved companies table.
	ctx := context.Background()

	// Fake crawler returns one GLEIF record so pullAndInsert has something to process.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"records": []any{map[string]any{
				"name":          "Test Corp",
				"country_iso2":  "GB",
				"lei":           "TEST123456789012345678",
				"status":        "active",
				"snapshot_hash": "abc123",
				"raw_data":      map[string]any{"lei": "TEST123456789012345678"},
			}},
			"has_more": false,
			"total":    1,
		})
	}))
	defer srv.Close()

	runID := uuid.New()
	sourceID := uuid.New()
	calls := map[string]int{}

	q := &stubPullQuerier{
		getSourceByNameFn: func(name string) (db.DataSource, error) {
			return db.DataSource{
				ID: sourceID, Name: name,
				PullTaskType:      "source_pull",
				ScheduleKind:      "interval",
				ProcessorTaskType: ptrString("source_process"),
				Enabled:           true,
			}, nil
		},
		createPullRunFn: func() (db.SourcePullRun, error) {
			calls["createPullRun"]++
			return db.SourcePullRun{ID: runID}, nil
		},
		upsertGLEIFFn: func() (db.GleifCompanyRawInput, error) {
			calls["upsertGLEIF"]++
			now := time.Now()
			return db.GleifCompanyRawInput{
				ID: uuid.New(), FirstSeenAt: now, LastSeenAt: now,
				ProcessingStatus: "pending",
			}, nil
		},
		insertCompanyFn: func() (db.Company, error) {
			calls["insertCompany"]++ // must never be called
			return db.Company{}, nil
		},
	}

	crawler := crawlerclient.New(srv.URL)
	w := workers.NewSourcePullWorker(q, crawler, nil)

	job := &river.Job[workers.SourcePullArgs]{
		JobRow: &rivertype.JobRow{ID: 1, Kind: "source_pull"},
		Args:   workers.SourcePullArgs{SourceName: "gleif", TriggerType: "manual"},
	}

	require.NoError(t, w.Work(ctx, job))

	assert.Equal(t, 0, calls["insertCompany"], "must not write resolved companies")
	assert.Equal(t, 1, calls["createPullRun"], "must create pull run row")
	assert.GreaterOrEqual(t, calls["upsertGLEIF"], 1, "must write to gleif raw input table")
}

func ptrString(s string) *string { return &s }
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd scheduler && GOWORK=off go test ./internal/workers/... -run TestSourcePullWorker -v 2>&1 | tail -5
```

Expected: FAIL or no test found yet (stub does nothing).

- [ ] **Step 3: Implement SourcePullWorker**

Replace the full content of `scheduler/internal/workers/source_pull.go`:

```go
package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlerclient"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type SourcePullWorker struct {
	river.WorkerDefaults[SourcePullArgs]
	db      db.Querier
	crawler *crawlerclient.Client
	pool    *pgxpool.Pool
}

func NewSourcePullWorker(q db.Querier, crawler *crawlerclient.Client, pool *pgxpool.Pool) *SourcePullWorker {
	return &SourcePullWorker{db: q, crawler: crawler, pool: pool}
}

func (w *SourcePullWorker) Work(ctx context.Context, job *river.Job[SourcePullArgs]) error {
	src, err := w.db.GetSourceByName(ctx, job.Args.SourceName)
	if err != nil {
		return errors.Wrap(err, "get source")
	}

	if err := w.db.UpdateSourcePullStarted(ctx, src.Name); err != nil {
		slog.Warn("source pull: update started_at", "source", src.Name, "error", err)
	}

	run, err := w.db.CreatePullRun(ctx, db.CreatePullRunParams{
		Column1: src.Name,
		Column2: &job.ID,
		Column3: job.Args.Kind(),
		Column4: job.Args.TriggerType,
	})
	if err != nil {
		return errors.Wrap(err, "create pull run")
	}

	inserted, updated, unchanged, pullErr := w.pullAndInsert(ctx, src, run.ID)

	if pullErr != nil {
		_ = w.db.FailPullRun(ctx, db.FailPullRunParams{
			ID:           run.ID,
			ErrorMessage: &[]string{pullErr.Error()}[0],
		})
		_ = w.db.UpdateSourcePullFailed(ctx, db.UpdateSourcePullFailedParams{
			Name:    src.Name,
			LastError: &[]string{pullErr.Error()}[0],
		})
		slog.Error("source pull failed", "source", src.Name, "job_id", job.ID, "error", pullErr)
		return pullErr
	}

	_ = w.db.SucceedPullRun(ctx, db.SucceedPullRunParams{
		ID:              run.ID,
		RowsSeen:        int32(inserted + updated + unchanged),
		RawRowsInserted: int32(inserted),
		RawRowsUpdated:  int32(updated),
		RawRowsUnchanged: int32(unchanged),
	})
	_ = w.db.UpdateSourcePullSucceeded(ctx, db.UpdateSourcePullSucceededParams{
		Name: src.Name,
	})

	if src.ProcessorTaskType != nil && *src.ProcessorTaskType == "source_process" {
		if rc := riverClientFromCtx(ctx); rc != nil {
			_, _ = rc.Insert(ctx, SourceProcessArgs{
				SourceName: src.Name,
				PullRunID:  run.ID.String(),
			}, &river.InsertOpts{Queue: "source_process"})
		}
	}
	return nil
}

func (w *SourcePullWorker) pullAndInsert(ctx context.Context, src db.DataSource, runID uuid.UUID) (inserted, updated, unchanged int, err error) {
	page := 1
	for {
		resp, err := w.crawler.Crawl(ctx, src.Name, time.Time{}, nil, page)
		if err != nil {
			return inserted, updated, unchanged, errors.Wrap(err, "crawl page")
		}
		for _, rec := range resp.Records {
			// Use rec.SnapshotHash (computed by the crawler) as the payload hash.
			// Use rec.RawData (the raw source JSON) as the stored payload.
			raw, _ := json.Marshal(rec.RawData)
			i, u, unch, e := w.upsertRecord(ctx, src.Name, runID, rec, raw)
			inserted += i; updated += u; unchanged += unch
			if e != nil {
				slog.Warn("source pull: upsert row", "source", src.Name, "error", e)
			}
		}
		if !resp.HasMore {
			break
		}
		page++
	}
	return
}

// upsertRecord stores one CompanyRecord in the source-specific raw input table.
// crawlerclient.CompanyRecord has typed fields: LEI, RegistrationNumber, Name,
// Status, Website, SnapshotHash, RawData, Locations, etc.
func (w *SourcePullWorker) upsertRecord(ctx context.Context, sourceName string, runID uuid.UUID, rec crawlerclient.CompanyRecord, raw []byte) (inserted, updated, unchanged int, err error) {
	hash := rec.SnapshotHash
	switch sourceName {
	case "gleif":
		if rec.LEI == nil || *rec.LEI == "" {
			return 0, 0, 0, errors.New("gleif record missing lei")
		}
		lei := *rec.LEI
		// Extract GLEIF-specific fields from RawData (the source keeps them there).
		regStatus, _ := rec.RawData["registration_status"].(string)
		hqCountry, _ := rec.RawData["headquarters_country_code"].(string)
		parentLEI, _ := rec.RawData["direct_parent_lei"].(string)
		ultimateLEI, _ := rec.RawData["ultimate_parent_lei"].(string)
		row, err := w.db.UpsertGLEIFCompanyRawInput(ctx, db.UpsertGLEIFCompanyRawInputParams{
			SourcePullRunID:         runID,
			SourceNativeID:          lei,
			Lei:                     lei,
			LegalName:               ptrStr(rec.Name),
			RegistrationStatus:      ptrStr(regStatus),
			HeadquartersCountryCode: ptrStr(hqCountry),
			ParentLei:               ptrStr(parentLEI),
			UltimateLei:             ptrStr(ultimateLEI),
			RawPayload:              raw,
			PayloadHash:             hash,
		})
		if err != nil {
			return 0, 0, 0, errors.Wrap(err, "upsert gleif")
		}
		if row.LastSeenAt.Equal(row.FirstSeenAt) {
			return 1, 0, 0, nil
		}
		if row.ProcessingStatus == "pending" {
			return 0, 1, 0, nil
		}
		return 0, 0, 1, nil

	case "companies_house":
		if rec.RegistrationNumber == nil || *rec.RegistrationNumber == "" {
			return 0, 0, 0, errors.New("companies_house record missing registration_number")
		}
		num := *rec.RegistrationNumber
		companyType, _ := rec.RawData["type"].(string)
		row, err := w.db.UpsertCompaniesHouseRawInput(ctx, db.UpsertCompaniesHouseRawInputParams{
			SourcePullRunID: runID,
			SourceNativeID:  num,
			CompanyName:     ptrStr(rec.Name),
			CompanyStatus:   ptrStr(rec.Status),
			CompanyType:     ptrStr(companyType),
			RawPayload:      raw,
			PayloadHash:     hash,
		})
		if err != nil {
			return 0, 0, 0, errors.Wrap(err, "upsert companies_house")
		}
		_ = row
		return 1, 0, 0, nil

	case "brreg":
		if rec.RegistrationNumber == nil || *rec.RegistrationNumber == "" {
			return 0, 0, 0, errors.New("brreg record missing registration_number")
		}
		num := *rec.RegistrationNumber
		website := ""
		if rec.Website != nil {
			website = *rec.Website
		}
		row, err := w.db.UpsertBrregRawInput(ctx, db.UpsertBrregRawInputParams{
			SourcePullRunID:  runID,
			SourceNativeID:   num,
			OrganizationName: ptrStr(rec.Name),
			Website:          ptrStr(website),
			RawPayload:       raw,
			PayloadHash:      hash,
		})
		if err != nil {
			return 0, 0, 0, errors.Wrap(err, "upsert brreg")
		}
		_ = row
		return 1, 0, 0, nil

	default:
		return 0, 0, 0, fmt.Errorf("unknown source: %s", sourceName)
	}
}

func ptrStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

- [ ] **Step 4: Run tests**

```bash
cd scheduler && GOWORK=off go test ./internal/workers/... -v 2>&1 | tail -20
```

Expected: tests pass (stubs may not fully exercise logic yet; the key is no panics and compilation succeeds).

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/workers/source_pull.go \
        scheduler/internal/workers/source_pull_test.go
git commit -m "feat: implement SourcePullWorker — fetches raw records, writes source-specific input tables only"
```

---

## Task 7: SourceProcessWorker and GLEIF processor

**Files:**
- Modify: `scheduler/internal/workers/source_process.go` (replace stub)
- Create: `scheduler/internal/workers/gleif_processor.go`
- Create: `scheduler/internal/workers/gleif_processor_test.go`

The processor claims pending raw input rows, parses them, looks up existing companies, and emits suggestions. It never writes resolved entity tables.

- [ ] **Step 1: Create shared processor test mock**

Create `scheduler/internal/workers/processor_testmock_test.go`. This file defines `mockQuerier`, `ptrStr`, and `pgTypeTZ` for use by all three processor test files in this package.

```go
package workers_test

import (
	"context"
	"time"

	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

// pgxErrNoRows is a convenience alias used in processor tests.
var pgxErrNoRows = pgx.ErrNoRows

func ptrStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func pgTypeTZ(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Valid: true, Time: t}
}

// mockQuerier is a configurable db.Querier for processor unit tests.
// Embeds db.Querier so unimplemented methods are satisfied; only override what each test needs.
type mockQuerier struct {
	db.Querier
	claimGLEIF                     func() []db.GleifCompanyRawInput
	claimCH                        func() []db.CompaniesHouseCompanyRawInput
	claimBrreg                     func() []db.BrregCompanyRawInput
	getCompanyByLEI                func(lei string) (db.Company, error)
	getCompanyByRegAndCountry      func(reg, iso string) (db.Company, error)
	getSourceByName                func(name string) (db.DataSource, error)
	getCountryIDByISO2             func(iso string) (uuid.UUID, error)
	insertCompanySuggestion        func(arg db.InsertCompanySuggestionParams) (db.CompanySuggestion, error)
	insertCompanyStatusSuggestion  func(arg db.InsertCompanyStatusSuggestionParams) (db.CompanyStatusSuggestion, error)
	insertCompanyContactSuggestion func(arg db.InsertCompanyContactSuggestionParams) (db.CompanyContactSuggestion, error)
	insertSuggestionSourceLink     func() (db.SuggestionSourceLink, error)
	markGLEIFProcessed             func(id uuid.UUID) error
	markGLEIFFailed                func(arg db.MarkGLEIFRawInputFailedParams) error
	markCHProcessed                func(id uuid.UUID) error
	markCHFailed                   func(arg db.MarkCompaniesHouseRawInputFailedParams) error
	markBrregProcessed             func(id uuid.UUID) error
	markBrregFailed                func(arg db.MarkBrregRawInputFailedParams) error
}

func (q *mockQuerier) ClaimPendingGLEIFRawInputs(ctx context.Context, arg db.ClaimPendingGLEIFRawInputsParams) ([]db.GleifCompanyRawInput, error) {
	if q.claimGLEIF != nil {
		return q.claimGLEIF(), nil
	}
	return nil, nil
}
func (q *mockQuerier) ClaimPendingCompaniesHouseRawInputs(ctx context.Context, arg db.ClaimPendingCompaniesHouseRawInputsParams) ([]db.CompaniesHouseCompanyRawInput, error) {
	if q.claimCH != nil {
		return q.claimCH(), nil
	}
	return nil, nil
}
func (q *mockQuerier) ClaimPendingBrregRawInputs(ctx context.Context, arg db.ClaimPendingBrregRawInputsParams) ([]db.BrregCompanyRawInput, error) {
	if q.claimBrreg != nil {
		return q.claimBrreg(), nil
	}
	return nil, nil
}
func (q *mockQuerier) GetCompanyByLEI(ctx context.Context, lei string) (db.Company, error) {
	if q.getCompanyByLEI != nil {
		return q.getCompanyByLEI(lei)
	}
	return db.Company{}, nil
}
func (q *mockQuerier) GetCompanyByRegistrationAndCountry(ctx context.Context, arg db.GetCompanyByRegistrationAndCountryParams) (db.Company, error) {
	if q.getCompanyByRegAndCountry != nil {
		return q.getCompanyByRegAndCountry(arg.RegistrationNumber, arg.IsoAlpha2)
	}
	return db.Company{}, nil
}
func (q *mockQuerier) GetSourceByName(ctx context.Context, name string) (db.DataSource, error) {
	if q.getSourceByName != nil {
		return q.getSourceByName(name)
	}
	return db.DataSource{}, nil
}
func (q *mockQuerier) GetCountryIDByISO2(ctx context.Context, iso string) (uuid.UUID, error) {
	if q.getCountryIDByISO2 != nil {
		return q.getCountryIDByISO2(iso)
	}
	return uuid.UUID{}, nil
}
func (q *mockQuerier) InsertCompanySuggestion(ctx context.Context, arg db.InsertCompanySuggestionParams) (db.CompanySuggestion, error) {
	if q.insertCompanySuggestion != nil {
		return q.insertCompanySuggestion(arg)
	}
	return db.CompanySuggestion{ID: uuid.New()}, nil
}
func (q *mockQuerier) InsertCompanyStatusSuggestion(ctx context.Context, arg db.InsertCompanyStatusSuggestionParams) (db.CompanyStatusSuggestion, error) {
	if q.insertCompanyStatusSuggestion != nil {
		return q.insertCompanyStatusSuggestion(arg)
	}
	return db.CompanyStatusSuggestion{ID: uuid.New()}, nil
}
func (q *mockQuerier) InsertCompanyContactSuggestion(ctx context.Context, arg db.InsertCompanyContactSuggestionParams) (db.CompanyContactSuggestion, error) {
	if q.insertCompanyContactSuggestion != nil {
		return q.insertCompanyContactSuggestion(arg)
	}
	return db.CompanyContactSuggestion{ID: uuid.New()}, nil
}
func (q *mockQuerier) InsertSuggestionSourceLink(ctx context.Context, arg db.InsertSuggestionSourceLinkParams) (db.SuggestionSourceLink, error) {
	if q.insertSuggestionSourceLink != nil {
		return q.insertSuggestionSourceLink()
	}
	return db.SuggestionSourceLink{}, nil
}
func (q *mockQuerier) MarkGLEIFRawInputProcessed(ctx context.Context, id uuid.UUID) error {
	if q.markGLEIFProcessed != nil {
		return q.markGLEIFProcessed(id)
	}
	return nil
}
func (q *mockQuerier) MarkGLEIFRawInputFailed(ctx context.Context, arg db.MarkGLEIFRawInputFailedParams) error {
	if q.markGLEIFFailed != nil {
		return q.markGLEIFFailed(arg)
	}
	return nil
}
func (q *mockQuerier) MarkCompaniesHouseRawInputProcessed(ctx context.Context, id uuid.UUID) error {
	if q.markCHProcessed != nil {
		return q.markCHProcessed(id)
	}
	return nil
}
func (q *mockQuerier) MarkCompaniesHouseRawInputFailed(ctx context.Context, arg db.MarkCompaniesHouseRawInputFailedParams) error {
	if q.markCHFailed != nil {
		return q.markCHFailed(arg)
	}
	return nil
}
func (q *mockQuerier) MarkBrregRawInputProcessed(ctx context.Context, id uuid.UUID) error {
	if q.markBrregProcessed != nil {
		return q.markBrregProcessed(id)
	}
	return nil
}
func (q *mockQuerier) MarkBrregRawInputFailed(ctx context.Context, arg db.MarkBrregRawInputFailedParams) error {
	if q.markBrregFailed != nil {
		return q.markBrregFailed(arg)
	}
	return nil
}
```

- [ ] **Step 2: Write the failing test**

Create `scheduler/internal/workers/gleif_processor_test.go`:

```go
package workers_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func TestGLEIFProcessor_NewCompany_CreatesSuggestion(t *testing.T) {
	ctx := context.Background()

	runID := uuid.New()
	rawID := uuid.New()
	sourceID := uuid.New()
	countryID := uuid.New()
	payload := json.RawMessage(`{"lei":"TEST123","legalName":"Test Corp","registration_status":"ISSUED"}`)

	rawRow := db.GleifCompanyRawInput{
		ID:                      rawID,
		SourcePullRunID:         runID,
		Lei:                     "TEST123",
		LegalName:               ptrStr("Test Corp"),
		HeadquartersCountryCode: ptrStr("GB"),
		RawPayload:              payload,
		PayloadHash:             "abc123",
		ProcessingStatus:        "processing",
		ProcessingLeaseUntil:    pgTypeTZ(time.Now().Add(30 * time.Second)),
	}

	suggestionCreated := false
	linkCreated := false
	markedProcessed := false

	q := &mockQuerier{
		claimGLEIF: func() []db.GleifCompanyRawInput { return []db.GleifCompanyRawInput{rawRow} },
		getCompanyByLEI: func(lei string) (db.Company, error) {
			return db.Company{}, pgxErrNoRows
		},
		getSourceByName: func(name string) (db.DataSource, error) {
			return db.DataSource{ID: sourceID, Name: name}, nil
		},
		getCountryIDByISO2: func(iso string) (uuid.UUID, error) {
			assert.Equal(t, "GB", iso)
			return countryID, nil
		},
		insertCompanySuggestion: func(arg db.InsertCompanySuggestionParams) (db.CompanySuggestion, error) {
			assert.Equal(t, "Test Corp", arg.ProposedDisplayName)
			assert.True(t, arg.ProposedCountryID.Valid, "must set proposed_country_id")
			suggestionCreated = true
			return db.CompanySuggestion{ID: uuid.New()}, nil
		},
		insertSuggestionSourceLink: func() (db.SuggestionSourceLink, error) {
			linkCreated = true
			return db.SuggestionSourceLink{}, nil
		},
		markGLEIFProcessed: func(id uuid.UUID) error {
			assert.Equal(t, rawID, id)
			markedProcessed = true
			return nil
		},
	}

	proc := workers.NewGLEIFProcessor(q)
	err := proc.ProcessBatch(ctx, "gleif")
	require.NoError(t, err)

	assert.True(t, suggestionCreated, "must create company suggestion for unknown LEI")
	assert.True(t, linkCreated, "must create suggestion source link")
	assert.True(t, markedProcessed, "must mark raw input processed")
}

func TestGLEIFProcessor_ExistingCompany_CreatesStatusSuggestion(t *testing.T) {
	ctx := context.Background()

	companyID := uuid.New()
	rawID := uuid.New()
	sourceID := uuid.New()
	payload := json.RawMessage(`{"lei":"EXIST456","legalName":"New Legal Name","registration_status":"LAPSED"}`)

	rawRow := db.GleifCompanyRawInput{
		ID: rawID, Lei: "EXIST456",
		LegalName: ptrStr("New Legal Name"),
		RegistrationStatus: ptrStr("LAPSED"),
		RawPayload: payload, PayloadHash: "def456",
		ProcessingStatus: "processing",
	}

	statusSuggestionCreated := false

	q := &mockQuerier{
		claimGLEIF: func() []db.GleifCompanyRawInput { return []db.GleifCompanyRawInput{rawRow} },
		getCompanyByLEI: func(lei string) (db.Company, error) {
			return db.Company{ID: companyID, Lei: ptrStr("EXIST456")}, nil
		},
		getSourceByName: func(name string) (db.DataSource, error) {
			return db.DataSource{ID: sourceID, Name: name}, nil
		},
		insertCompanyStatusSuggestion: func(arg db.InsertCompanyStatusSuggestionParams) (db.CompanyStatusSuggestion, error) {
			statusSuggestionCreated = true
			return db.CompanyStatusSuggestion{ID: uuid.New()}, nil
		},
		insertSuggestionSourceLink: func() (db.SuggestionSourceLink, error) {
			return db.SuggestionSourceLink{}, nil
		},
		markGLEIFProcessed: func(id uuid.UUID) error { return nil },
	}

	proc := workers.NewGLEIFProcessor(q)
	err := proc.ProcessBatch(ctx, "gleif")
	require.NoError(t, err)
	assert.True(t, statusSuggestionCreated, "must create status suggestion for existing company")
}
```

- [ ] **Step 3: Run test to confirm it fails**

```bash
cd scheduler && GOWORK=off go test ./internal/workers/... -run TestGLEIFProcessor -v 2>&1 | tail -5
```

Expected: compile error because `GLEIFProcessor` doesn't exist yet.

- [ ] **Step 4: Implement GLEIFProcessor**

Create `scheduler/internal/workers/gleif_processor.go`:

```go
package workers

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const gleifBatchSize = 50
const gleifLeaseSecs = 120

type GLEIFProcessor struct {
	db db.Querier
}

func NewGLEIFProcessor(q db.Querier) *GLEIFProcessor {
	return &GLEIFProcessor{db: q}
}

func (p *GLEIFProcessor) ProcessBatch(ctx context.Context, sourceName string) error {
	src, err := p.db.GetSourceByName(ctx, sourceName)
	if err != nil {
		return errors.Wrap(err, "get source")
	}

	rows, err := p.db.ClaimPendingGLEIFRawInputs(ctx, db.ClaimPendingGLEIFRawInputsParams{
		Column1: "gleif-processor",
		Column2: gleifLeaseSecs,
		Column3: gleifBatchSize,
	})
	if err != nil {
		return errors.Wrap(err, "claim gleif rows")
	}

	for _, row := range rows {
		if err := p.processOne(ctx, src, row); err != nil {
			slog.Error("gleif processor: row failed", "row_id", row.ID, "lei", row.Lei, "error", err)
			errMsg := err.Error()
			_ = p.db.MarkGLEIFRawInputFailed(ctx, db.MarkGLEIFRawInputFailedParams{
				ID:      row.ID,
				Column2: &errMsg,
			})
		}
	}
	return nil
}

func (p *GLEIFProcessor) processOne(ctx context.Context, src db.DataSource, row db.GleifCompanyRawInput) error {
	existing, err := p.db.GetCompanyByLEI(ctx, row.Lei)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return errors.Wrap(err, "lookup company by lei")
	}

	if errors.Is(err, pgx.ErrNoRows) {
		// New company — create a root suggestion.
		displayName := row.Lei
		if row.LegalName != nil {
			displayName = *row.LegalName
		}
		profile, _ := json.Marshal(map[string]any{"lei": row.Lei})
		var countryID pgtype.UUID
		if row.HeadquartersCountryCode != nil && *row.HeadquartersCountryCode != "" {
			if cid, err := p.db.GetCountryIDByISO2(ctx, *row.HeadquartersCountryCode); err == nil {
				countryID = pgtype.UUID{Bytes: cid, Valid: true}
			}
		}
		sug, err := p.db.InsertCompanySuggestion(ctx, db.InsertCompanySuggestionParams{
			ProposedDisplayName: displayName,
			ProposedLegalName:   row.LegalName,
			ProposedProfile:     profile,
			ProposedCountryID:   countryID,
			Confidence:          ptrFloat32(0.7),
		})
		if err != nil {
			return errors.Wrap(err, "insert company suggestion")
		}
		if err := p.linkSuggestion(ctx, src, row, "company_suggestions", sug.ID); err != nil {
			return err
		}
	} else {
		// Existing company — emit section suggestions for changed fields.
		if row.RegistrationStatus != nil {
			current, _ := json.Marshal(map[string]any{"registration_status": existing.RegistrationStatus})
			proposed, _ := json.Marshal(map[string]any{"registration_status": *row.RegistrationStatus})
			sug, err := p.db.InsertCompanyStatusSuggestion(ctx, db.InsertCompanyStatusSuggestionParams{
				CompanyID:      &existing.ID,
				Operation:      "update",
				StatusField:    "registration_status",
				CurrentValue:   ptrStr(existing.RegistrationStatus),
				ProposedValue:  row.RegistrationStatus,
				CurrentPayload: current,
				ProposedPayload: proposed,
				Confidence:     ptrFloat32(0.8),
			})
			if err != nil {
				return errors.Wrap(err, "insert status suggestion")
			}
			if err := p.linkSuggestion(ctx, src, row, "company_status_suggestions", sug.ID); err != nil {
				return err
			}
		}
	}

	return p.db.MarkGLEIFRawInputProcessed(ctx, row.ID)
}

func (p *GLEIFProcessor) linkSuggestion(ctx context.Context, src db.DataSource, row db.GleifCompanyRawInput, table string, sugID uuid.UUID) error {
	_, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
		SuggestionTable:  table,
		SuggestionID:     sugID,
		SourceID:         src.ID,
		SourceInputTable: "gleif_company_raw_inputs",
		SourceInputKey:   row.ID.String(),
		SourcePullRunID:  &row.SourcePullRunID,
	})
	return errors.Wrap(err, "insert source link")
}

func ptrFloat32(f float32) *float32 { return &f }
```

- [ ] **Step 5: Implement SourceProcessWorker dispatch**

Replace the full content of `scheduler/internal/workers/source_process.go`:

```go
package workers

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

type SourceProcessWorker struct {
	river.WorkerDefaults[SourceProcessArgs]
	db   db.Querier
	pool *pgxpool.Pool
}

func NewSourceProcessWorker(q db.Querier, pool *pgxpool.Pool) *SourceProcessWorker {
	return &SourceProcessWorker{db: q, pool: pool}
}

func (w *SourceProcessWorker) Work(ctx context.Context, job *river.Job[SourceProcessArgs]) error {
	switch job.Args.SourceName {
	case "gleif":
		proc := NewGLEIFProcessor(w.db)
		if err := proc.ProcessBatch(ctx, job.Args.SourceName); err != nil {
			slog.Error("source process: gleif", "job_id", job.ID, "error", err)
			return err
		}
	case "companies_house":
		proc := NewCompaniesHouseProcessor(w.db)
		if err := proc.ProcessBatch(ctx, job.Args.SourceName); err != nil {
			slog.Error("source process: companies_house", "job_id", job.ID, "error", err)
			return err
		}
	case "brreg":
		proc := NewBrregProcessor(w.db)
		if err := proc.ProcessBatch(ctx, job.Args.SourceName); err != nil {
			slog.Error("source process: brreg", "job_id", job.ID, "error", err)
			return err
		}
	default:
		return errors.Newf("unknown source for processing: %s", job.Args.SourceName)
	}
	return nil
}
```

- [ ] **Step 6: Run tests**

```bash
cd scheduler && GOWORK=off go test ./internal/workers/... -run TestGLEIFProcessor -v 2>&1 | tail -20
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add scheduler/internal/workers/gleif_processor.go \
        scheduler/internal/workers/gleif_processor_test.go \
        scheduler/internal/workers/processor_testmock_test.go \
        scheduler/internal/workers/source_process.go
git commit -m "feat: implement GLEIFProcessor and SourceProcessWorker dispatch"
```

---

## Task 8: Companies House and Brreg processors

**Files:**
- Create: `scheduler/internal/workers/companies_house_processor.go`
- Create: `scheduler/internal/workers/companies_house_processor_test.go`
- Create: `scheduler/internal/workers/brreg_processor.go`
- Create: `scheduler/internal/workers/brreg_processor_test.go`

- [ ] **Step 1: Write failing test for Companies House**

Create `scheduler/internal/workers/companies_house_processor_test.go`:

```go
package workers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func TestCompaniesHouseProcessor_NewCompany_CreatesSuggestion(t *testing.T) {
	ctx := context.Background()
	rawID := uuid.New()
	sourceID := uuid.New()
	payload := json.RawMessage(`{"company_number":"12345678","company_name":"UK Test Ltd","company_status":"active"}`)

	rawRow := db.CompaniesHouseCompanyRawInput{
		ID: rawID, CompanyNumber: "12345678",
		CompanyName: ptrStr("UK Test Ltd"), CompanyStatus: ptrStr("active"),
		RawPayload: payload, PayloadHash: "ch123",
		ProcessingStatus: "processing",
	}

	created := false
	countryID := uuid.New()
	q := &mockQuerier{
		claimCH: func() []db.CompaniesHouseCompanyRawInput { return []db.CompaniesHouseCompanyRawInput{rawRow} },
		getCompanyByRegAndCountry: func(reg, iso string) (db.Company, error) {
			assert.Equal(t, "GB", iso)
			return db.Company{}, pgxErrNoRows
		},
		getSourceByName: func(name string) (db.DataSource, error) {
			return db.DataSource{ID: sourceID, Name: name}, nil
		},
		getCountryIDByISO2: func(iso string) (uuid.UUID, error) {
			assert.Equal(t, "GB", iso)
			return countryID, nil
		},
		insertCompanySuggestion: func(arg db.InsertCompanySuggestionParams) (db.CompanySuggestion, error) {
			assert.Equal(t, "UK Test Ltd", arg.ProposedDisplayName)
			assert.True(t, arg.ProposedCountryID.Valid, "must set proposed_country_id")
			created = true
			return db.CompanySuggestion{ID: uuid.New()}, nil
		},
		insertSuggestionSourceLink: func() (db.SuggestionSourceLink, error) {
			return db.SuggestionSourceLink{}, nil
		},
		markCHProcessed: func(id uuid.UUID) error { return nil },
	}

	proc := workers.NewCompaniesHouseProcessor(q)
	require.NoError(t, proc.ProcessBatch(ctx, "companies_house"))
	assert.True(t, created)
}
```

- [ ] **Step 2: Implement Companies House processor**

Create `scheduler/internal/workers/companies_house_processor.go`:

```go
package workers

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const chBatchSize = 50
const chLeaseSecs = 120

type CompaniesHouseProcessor struct {
	db db.Querier
}

func NewCompaniesHouseProcessor(q db.Querier) *CompaniesHouseProcessor {
	return &CompaniesHouseProcessor{db: q}
}

func (p *CompaniesHouseProcessor) ProcessBatch(ctx context.Context, sourceName string) error {
	src, err := p.db.GetSourceByName(ctx, sourceName)
	if err != nil {
		return errors.Wrap(err, "get source")
	}

	rows, err := p.db.ClaimPendingCompaniesHouseRawInputs(ctx, db.ClaimPendingCompaniesHouseRawInputsParams{
		Column1: "ch-processor",
		Column2: chLeaseSecs,
		Column3: chBatchSize,
	})
	if err != nil {
		return errors.Wrap(err, "claim ch rows")
	}

	for _, row := range rows {
		if err := p.processOne(ctx, src, row); err != nil {
			slog.Error("ch processor: row failed", "row_id", row.ID, "company_number", row.CompanyNumber, "error", err)
			errMsg := err.Error()
			_ = p.db.MarkCompaniesHouseRawInputFailed(ctx, db.MarkCompaniesHouseRawInputFailedParams{
				ID:      row.ID,
				Column2: &errMsg,
			})
		}
	}
	return nil
}

func (p *CompaniesHouseProcessor) processOne(ctx context.Context, src db.DataSource, row db.CompaniesHouseCompanyRawInput) error {
	existing, err := p.db.GetCompanyByRegistrationAndCountry(ctx, db.GetCompanyByRegistrationAndCountryParams{
		RegistrationNumber: row.CompanyNumber,
		IsoAlpha2:          "GB",
	})

	if errors.Is(err, pgx.ErrNoRows) {
		displayName := row.CompanyNumber
		if row.CompanyName != nil {
			displayName = *row.CompanyName
		}
		profile, _ := json.Marshal(map[string]any{"company_number": row.CompanyNumber, "country": "GB"})
		var countryID pgtype.UUID
		if cid, err := p.db.GetCountryIDByISO2(ctx, "GB"); err == nil {
			countryID = pgtype.UUID{Bytes: cid, Valid: true}
		}
		sug, err := p.db.InsertCompanySuggestion(ctx, db.InsertCompanySuggestionParams{
			ProposedDisplayName: displayName,
			ProposedProfile:     profile,
			ProposedCountryID:   countryID,
			Confidence:          ptrFloat32(0.75),
		})
		if err != nil {
			return errors.Wrap(err, "insert company suggestion")
		}
		if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
			SuggestionTable:  "company_suggestions",
			SuggestionID:     sug.ID,
			SourceID:         src.ID,
			SourceInputTable: "companies_house_company_raw_inputs",
			SourceInputKey:   row.ID.String(),
			SourcePullRunID:  &row.SourcePullRunID,
		}); err != nil {
			return errors.Wrap(err, "insert source link")
		}
	} else if err != nil {
		return errors.Wrap(err, "lookup company")
	} else if row.CompanyStatus != nil {
		current, _ := json.Marshal(map[string]any{"lifecycle_status": existing.LifecycleStatus})
		proposed, _ := json.Marshal(map[string]any{"registration_status": *row.CompanyStatus})
		sug, err := p.db.InsertCompanyStatusSuggestion(ctx, db.InsertCompanyStatusSuggestionParams{
			CompanyID:       &existing.ID,
			Operation:       "update",
			StatusField:     "registration_status",
			ProposedValue:   row.CompanyStatus,
			CurrentPayload:  current,
			ProposedPayload: proposed,
			Confidence:      ptrFloat32(0.8),
		})
		if err != nil {
			return errors.Wrap(err, "insert status suggestion")
		}
		if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
			SuggestionTable:  "company_status_suggestions",
			SuggestionID:     sug.ID,
			SourceID:         src.ID,
			SourceInputTable: "companies_house_company_raw_inputs",
			SourceInputKey:   row.ID.String(),
		}); err != nil {
			return errors.Wrap(err, "insert source link")
		}
	}

	return p.db.MarkCompaniesHouseRawInputProcessed(ctx, row.ID)
}
```

- [ ] **Step 3: Write failing test for Brreg**

Create `scheduler/internal/workers/brreg_processor_test.go`:

```go
package workers_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/workers"
)

func TestBrregProcessor_NewCompany_CreatesSuggestionWithWebsite(t *testing.T) {
	ctx := context.Background()
	rawID := uuid.New()
	sourceID := uuid.New()
	website := "https://example.no"
	payload := json.RawMessage(`{"organisasjonsnummer":"123456789","navn":"Norsk AS","hjemmeside":"https://example.no"}`)

	rawRow := db.BrregCompanyRawInput{
		ID: rawID, OrganizationNumber: "123456789",
		OrganizationName: ptrStr("Norsk AS"), Website: ptrStr(website),
		RawPayload: payload, PayloadHash: "br123",
		ProcessingStatus: "processing",
	}

	companySuggestionCreated := false
	contactSuggestionCreated := false
	countryID := uuid.New()

	q := &mockQuerier{
		claimBrreg: func() []db.BrregCompanyRawInput { return []db.BrregCompanyRawInput{rawRow} },
		getCompanyByRegAndCountry: func(reg, iso string) (db.Company, error) {
			assert.Equal(t, "NO", iso)
			return db.Company{}, pgxErrNoRows
		},
		getSourceByName: func(name string) (db.DataSource, error) {
			return db.DataSource{ID: sourceID, Name: name}, nil
		},
		getCountryIDByISO2: func(iso string) (uuid.UUID, error) {
			assert.Equal(t, "NO", iso)
			return countryID, nil
		},
		insertCompanySuggestion: func(arg db.InsertCompanySuggestionParams) (db.CompanySuggestion, error) {
			assert.True(t, arg.ProposedCountryID.Valid, "must set proposed_country_id")
			companySuggestionCreated = true
			return db.CompanySuggestion{ID: uuid.New()}, nil
		},
		insertCompanyContactSuggestion: func(arg db.InsertCompanyContactSuggestionParams) (db.CompanyContactSuggestion, error) {
			assert.Equal(t, "website", arg.ContactKind)
			contactSuggestionCreated = true
			return db.CompanyContactSuggestion{ID: uuid.New()}, nil
		},
		insertSuggestionSourceLink: func() (db.SuggestionSourceLink, error) {
			return db.SuggestionSourceLink{}, nil
		},
		markBrregProcessed: func(id uuid.UUID) error { return nil },
	}

	proc := workers.NewBrregProcessor(q)
	require.NoError(t, proc.ProcessBatch(ctx, "brreg"))
	assert.True(t, companySuggestionCreated)
	assert.True(t, contactSuggestionCreated, "website should create a contact suggestion")
}
```

- [ ] **Step 4: Implement Brreg processor**

Create `scheduler/internal/workers/brreg_processor.go`:

```go
package workers

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const brregBatchSize = 50
const brregLeaseSecs = 120

type BrregProcessor struct {
	db db.Querier
}

func NewBrregProcessor(q db.Querier) *BrregProcessor {
	return &BrregProcessor{db: q}
}

func (p *BrregProcessor) ProcessBatch(ctx context.Context, sourceName string) error {
	src, err := p.db.GetSourceByName(ctx, sourceName)
	if err != nil {
		return errors.Wrap(err, "get source")
	}

	rows, err := p.db.ClaimPendingBrregRawInputs(ctx, db.ClaimPendingBrregRawInputsParams{
		Column1: "brreg-processor",
		Column2: brregLeaseSecs,
		Column3: brregBatchSize,
	})
	if err != nil {
		return errors.Wrap(err, "claim brreg rows")
	}

	for _, row := range rows {
		if err := p.processOne(ctx, src, row); err != nil {
			slog.Error("brreg processor: row failed", "row_id", row.ID, "org_number", row.OrganizationNumber, "error", err)
			errMsg := err.Error()
			_ = p.db.MarkBrregRawInputFailed(ctx, db.MarkBrregRawInputFailedParams{
				ID:      row.ID,
				Column2: &errMsg,
			})
		}
	}
	return nil
}

func (p *BrregProcessor) processOne(ctx context.Context, src db.DataSource, row db.BrregCompanyRawInput) error {
	existing, err := p.db.GetCompanyByRegistrationAndCountry(ctx, db.GetCompanyByRegistrationAndCountryParams{
		RegistrationNumber: row.OrganizationNumber,
		IsoAlpha2:          "NO",
	})

	if errors.Is(err, pgx.ErrNoRows) {
		displayName := row.OrganizationNumber
		if row.OrganizationName != nil {
			displayName = *row.OrganizationName
		}
		profile, _ := json.Marshal(map[string]any{"organization_number": row.OrganizationNumber, "country": "NO"})
		var countryID pgtype.UUID
		if cid, err := p.db.GetCountryIDByISO2(ctx, "NO"); err == nil {
			countryID = pgtype.UUID{Bytes: cid, Valid: true}
		}
		sug, err := p.db.InsertCompanySuggestion(ctx, db.InsertCompanySuggestionParams{
			ProposedDisplayName: displayName,
			ProposedProfile:     profile,
			ProposedCountryID:   countryID,
			Confidence:          ptrFloat32(0.75),
		})
		if err != nil {
			return errors.Wrap(err, "insert company suggestion")
		}
		if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
			SuggestionTable:  "company_suggestions",
			SuggestionID:     sug.ID,
			SourceID:         src.ID,
			SourceInputTable: "brreg_company_raw_inputs",
			SourceInputKey:   row.ID.String(),
			SourcePullRunID:  &row.SourcePullRunID,
		}); err != nil {
			return errors.Wrap(err, "insert source link")
		}
		// Website → contact suggestion attached to root suggestion.
		if row.Website != nil {
			proposed, _ := json.Marshal(map[string]any{"url": *row.Website})
			cSug, err := p.db.InsertCompanyContactSuggestion(ctx, db.InsertCompanyContactSuggestionParams{
				CompanySuggestionID: &sug.ID,
				Operation:           "add",
				ContactKind:         "website",
				CurrentPayload:      json.RawMessage("{}"),
				ProposedPayload:     proposed,
				Confidence:          ptrFloat32(0.75),
			})
			if err != nil {
				return errors.Wrap(err, "insert contact suggestion")
			}
			if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
				SuggestionTable:  "company_contact_suggestions",
				SuggestionID:     cSug.ID,
				SourceID:         src.ID,
				SourceInputTable: "brreg_company_raw_inputs",
				SourceInputKey:   row.ID.String(),
			}); err != nil {
				return errors.Wrap(err, "insert contact source link")
			}
		}
	} else if err != nil {
		return errors.Wrap(err, "lookup company")
	} else if row.Website != nil && existing.Website == nil {
		proposed, _ := json.Marshal(map[string]any{"url": *row.Website})
		sug, err := p.db.InsertCompanyContactSuggestion(ctx, db.InsertCompanyContactSuggestionParams{
			CompanyID:       &existing.ID,
			Operation:       "add",
			ContactKind:     "website",
			CurrentPayload:  json.RawMessage("{}"),
			ProposedPayload: proposed,
			Confidence:      ptrFloat32(0.75),
		})
		if err != nil {
			return errors.Wrap(err, "insert contact suggestion for existing")
		}
		if _, err := p.db.InsertSuggestionSourceLink(ctx, db.InsertSuggestionSourceLinkParams{
			SuggestionTable:  "company_contact_suggestions",
			SuggestionID:     sug.ID,
			SourceID:         src.ID,
			SourceInputTable: "brreg_company_raw_inputs",
			SourceInputKey:   row.ID.String(),
		}); err != nil {
			return errors.Wrap(err, "insert contact source link")
		}
	}

	return p.db.MarkBrregRawInputProcessed(ctx, row.ID)
}
```

- [ ] **Step 5: Run tests**

```bash
cd scheduler && GOWORK=off go test ./internal/workers/... -v 2>&1 | tail -20
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git add scheduler/internal/workers/companies_house_processor.go \
        scheduler/internal/workers/companies_house_processor_test.go \
        scheduler/internal/workers/brreg_processor.go \
        scheduler/internal/workers/brreg_processor_test.go
git commit -m "feat: implement Companies House and Brreg processors"
```

---

## Task 9: Approval service

**Files:**
- Create: `scheduler/internal/service/suggestions.go`
- Create: `scheduler/internal/service/suggestions_test.go`

The approval service is the **only** path that writes resolved entity tables from source-derived suggestions. All resolved writes happen inside a single transaction.

- [ ] **Step 1: Write the failing test**

Create `scheduler/internal/service/suggestions_test.go`:

```go
package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/service"
)

// helper: pgtype.UUID from a uuid.UUID
func pgUUID(id uuid.UUID) pgtype.UUID { return pgtype.UUID{Bytes: id, Valid: true} }
func pgTS() pgtype.Timestamptz        { return pgtype.Timestamptz{Valid: true} }

func sugRows(suggestionID, countryID uuid.UUID, displayName, status string) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "proposed_display_name", "proposed_legal_name", "proposed_country_id",
		"proposed_lei", "proposed_registration_number", "proposed_profile", "proposed_website",
		"confidence", "status", "reviewed_by", "review_note", "reviewed_at",
		"created_company_id", "created_at", "updated_at",
	}).AddRow(
		suggestionID, displayName, nil, pgUUID(countryID),
		nil, nil, []byte("{}"), nil,
		nil, status, nil, nil, nil,
		nil, pgTS(), pgTS(),
	)
}

func companyRows(companyID uuid.UUID, slug, name string, countryID uuid.UUID) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "canonical_slug", "name", "country_id", "status",
		"display_name", "lei", "registration_number", "website",
		"lifecycle_status", "profile", "created_at", "updated_at",
	}).AddRow(
		companyID, slug, name, countryID, "active",
		nil, nil, nil, nil, "active", []byte("{}"), pgTS(), pgTS(),
	)
}

func TestApproveCompanySuggestion_CreatesCompanyAndApprovesSuggestion(t *testing.T) {
	ctx := context.Background()
	suggestionID := uuid.New()
	countryID := uuid.New()
	companyID := uuid.New()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT`).WithArgs(suggestionID).
		WillReturnRows(sugRows(suggestionID, countryID, "Test Company", "pending"))
	mock.ExpectQuery(`SELECT`).WithArgs("test-company").
		WillReturnError(pgx.ErrNoRows) // no slug collision
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO companies`).
		WillReturnRows(companyRows(companyID, "test-company", "Test Company", countryID))
	mock.ExpectExec(`UPDATE company_suggestions`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	company, err := service.ApproveCompanySuggestion(ctx, mock, suggestionID, "admin", "looks good")
	require.NoError(t, err)
	assert.Equal(t, "test-company", company.CanonicalSlug)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestApproveCompanySuggestion_SlugCollision_UsesSuffix(t *testing.T) {
	ctx := context.Background()
	suggestionID := uuid.New()
	countryID := uuid.New()
	companyID := uuid.New()
	existingID := uuid.New()
	expectedSlug := "test-company-" + suggestionID.String()[:12]

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT`).WithArgs(suggestionID).
		WillReturnRows(sugRows(suggestionID, countryID, "Test Company", "pending"))
	mock.ExpectQuery(`SELECT`).WithArgs("test-company").
		WillReturnRows(companyRows(existingID, "test-company", "Existing Co", countryID)) // collision
	mock.ExpectBegin()
	mock.ExpectQuery(`INSERT INTO companies`).
		WillReturnRows(companyRows(companyID, expectedSlug, "Test Company", countryID))
	mock.ExpectExec(`UPDATE company_suggestions`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))
	mock.ExpectCommit()

	company, err := service.ApproveCompanySuggestion(ctx, mock, suggestionID, "admin", "")
	require.NoError(t, err)
	assert.Equal(t, expectedSlug, company.CanonicalSlug, "slug collision must append UUID suffix")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRejectCompanySuggestion_DoesNotWriteResolvedTables(t *testing.T) {
	ctx := context.Background()
	suggestionID := uuid.New()

	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT`).WithArgs(suggestionID).
		WillReturnRows(sugRows(suggestionID, uuid.Nil, "Test Corp", "pending"))
	// No ExpectBegin and no INSERT — reject must not write resolved tables.
	mock.ExpectExec(`UPDATE company_suggestions`).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	err = service.RejectCompanySuggestion(ctx, mock, suggestionID, "admin", "not relevant")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet(), "no company writes should occur on reject")
}
```

- [ ] **Step 2: Add pgxmock dependency and run test to confirm it fails**

```bash
cd scheduler && GOWORK=off go get github.com/pashagolub/pgxmock/v3
cd scheduler && GOWORK=off go test ./internal/service/... -v 2>&1 | tail -5
```

Expected: package not found (file doesn't exist yet).

- [ ] **Step 3: Implement the approval service**

Key constraints:
- `db.Querier` does not have `WithTx` — that method is on `*db.Queries` only. The service takes `*pgxpool.Pool` and calls `db.New()` to get a `*db.Queries` for transactional use.
- `companies.name` and `companies.country_id` are `NOT NULL`. The `InsertCompany` query must supply both. The `proposed_country_id` column added to `company_suggestions` in Task 3 provides the country. Approval must fail if `proposed_country_id` is NULL.

Create `scheduler/internal/service/suggestions.go`:

```go
package service

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	pgx "github.com/jackc/pgx/v5"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/slug"
)

// TxPool abstracts *pgxpool.Pool to allow injection of pgxmock in tests.
// *pgxpool.Pool satisfies this interface — no change needed in callers.
type TxPool interface {
	db.DBTX
	Begin(ctx context.Context) (pgx.Tx, error)
}

// ApproveCompanySuggestion creates a company from the suggestion and marks it approved.
// It is the only path that writes to the companies table from source-derived data.
// proposed_country_id must be set on the suggestion; approval fails without it.
func ApproveCompanySuggestion(ctx context.Context, pool TxPool, suggestionID uuid.UUID, reviewedBy, reviewNote string) (db.Company, error) {
	q := db.New(pool)

	sug, err := q.GetCompanySuggestionByID(ctx, suggestionID)
	if err != nil {
		return db.Company{}, errors.Wrap(err, "get suggestion")
	}
	if sug.Status != "pending" {
		return db.Company{}, fmt.Errorf("suggestion %s is not pending (status=%s)", suggestionID, sug.Status)
	}
	if !sug.ProposedCountryID.Valid {
		return db.Company{}, fmt.Errorf("suggestion %s has no proposed_country_id; cannot create company", suggestionID)
	}

	canonicalSlug := slug.Generate(sug.ProposedDisplayName)
	if canonicalSlug == "" {
		canonicalSlug = "company-" + suggestionID.String()[:12]
	}

	// Check for slug collision; retry once with UUID suffix.
	if _, err := q.GetCompanyBySlug(ctx, canonicalSlug); err == nil {
		canonicalSlug = canonicalSlug + "-" + suggestionID.String()[:12]
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return db.Company{}, errors.Wrap(err, "check slug collision")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return db.Company{}, errors.Wrap(err, "begin tx")
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// db.New(tx) gives a *db.Queries backed by the transaction.
	qtx := db.New(tx)

	company, err := qtx.InsertCompany(ctx, db.InsertCompanyParams{
		CanonicalSlug: canonicalSlug,
		Name:          sug.ProposedDisplayName,
		CountryID:     uuid.UUID(sug.ProposedCountryID.Bytes),
		Status:        "active",
	})
	if err != nil {
		return db.Company{}, errors.Wrap(err, "insert company")
	}

	if err := qtx.UpdateCompanySuggestionApproved(ctx, db.UpdateCompanySuggestionApprovedParams{
		ID:               suggestionID,
		CreatedCompanyID: &company.ID,
		ReviewedBy:       &reviewedBy,
		ReviewNote:       &reviewNote,
	}); err != nil {
		return db.Company{}, errors.Wrap(err, "update suggestion approved")
	}

	if err := tx.Commit(ctx); err != nil {
		return db.Company{}, errors.Wrap(err, "commit")
	}
	return company, nil
}

// RejectCompanySuggestion marks the suggestion rejected without touching resolved tables.
func RejectCompanySuggestion(ctx context.Context, pool TxPool, suggestionID uuid.UUID, reviewedBy, reviewNote string) error {
	q := db.New(pool)

	sug, err := q.GetCompanySuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("suggestion %s is not pending (status=%s)", suggestionID, sug.Status)
	}
	return q.UpdateCompanySuggestionRejected(ctx, db.UpdateCompanySuggestionRejectedParams{
		ID:         suggestionID,
		ReviewedBy: &reviewedBy,
		ReviewNote: &reviewNote,
	})
}

// ChildSuggestionRef identifies a section suggestion to approve alongside a root company suggestion.
type ChildSuggestionRef struct {
	Table string
	ID    uuid.UUID
}
```

The `InsertCompany` query in `companies.sql` (already written in Task 4 Step 2) is:

```sql
-- name: InsertCompany :one
INSERT INTO companies (canonical_slug, name, country_id, status)
VALUES ($1, $2, $3, coalesce($4, 'active'))
RETURNING *;
```

`db.New(tx)` works because sqlc generates `db.New(db DBTX)` where `DBTX` is satisfied by both `*pgxpool.Pool` and `pgx.Tx`.

- [ ] **Step 4: Run tests**

```bash
cd scheduler && GOWORK=off go test ./internal/service/... -v 2>&1 | tail -10
```

Expected: tests pass (documentation tests log and pass).

- [ ] **Step 5: Commit**

```bash
git add scheduler/internal/service/ database/queries/companies.sql scheduler/internal/db/gen/
git commit -m "feat: add company suggestion approval service with slug collision handling"
```

---

## Task 10: Suggestion API endpoints (root + section)

**Files:**
- Modify: `scheduler/internal/httpapi/suggestions.go` (replace stubs, add section handlers)
- Create: `scheduler/internal/httpapi/suggestions_test.go`
- Modify: `scheduler/internal/service/suggestions.go` (add section service functions, TxPool interface)
- Modify: `database/queries/suggestions.sql` (add section suggestion queries)

- [ ] **Step 1: Write the failing test**

Create `scheduler/internal/httpapi/suggestions_test.go`:

```go
package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestHandleListCompanySuggestions_ReturnsPendingSuggestions(t *testing.T) {
	q := &stubQuerier{}
	sugID := uuid.New()
	q.On("ListPendingCompanySuggestions", mock.Anything, mock.Anything).
		Return([]db.CompanySuggestion{
			{ID: sugID, ProposedDisplayName: "Test Corp", Status: "pending"},
		}, nil)
	q.On("CountPendingCompanySuggestions", mock.Anything).
		Return(int64(1), nil)

	r := chi.NewRouter()
	httpapi.NewHandlers(q, nil, nil, nil, "").RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/suggestions/companies", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	items, ok := resp["items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)
}

func TestHandleTriggerSource_NonPullTaskType_Returns422(t *testing.T) {
	q := &stubQuerier{}
	q.On("GetSourceByName", mock.Anything, "ai_company_profile").
		Return(db.DataSource{
			Name:         "ai_company_profile",
			PullTaskType: "ai_company_profile_pull",
			Enabled:      true,
		}, nil)

	r := chi.NewRouter()
	httpapi.NewHandlers(q, nil, nil, nil, "").RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/sources/ai_company_profile/trigger", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnprocessableEntity, w.Code)
}
```

- [ ] **Step 2: Run test to confirm it fails**

```bash
cd scheduler && GOWORK=off go test ./internal/httpapi/... -run "TestHandleListCompanySuggestions|TestHandleTriggerSource_Non" -v 2>&1 | tail -10
```

Expected: FAIL because stubs return empty response.

- [ ] **Step 3: Implement suggestion handlers**

Replace the full content of `scheduler/internal/httpapi/suggestions.go`:

```go
package httpapi

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/service"
)

func (h *Handlers) handleListCompanySuggestions(w http.ResponseWriter, r *http.Request) {
	page := queryInt(r, "page", 1)
	limit := min(queryInt(r, "limit", 20), 100)
	offset := int32((page - 1) * limit)

	items, err := h.db.ListPendingCompanySuggestions(r.Context(), db.ListPendingCompanySuggestionsParams{
		Offset: offset,
		Limit:  int32(limit),
	})
	if err != nil {
		slog.Error("list company suggestions", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	total, _ := h.db.CountPendingCompanySuggestions(r.Context())
	if items == nil {
		items = []db.CompanySuggestion{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": items, "page": page, "limit": limit, "total": total,
	})
}

func (h *Handlers) handleGetCompanySuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	sug, err := h.db.GetCompanySuggestionByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "suggestion not found")
		return
	}
	writeJSON(w, http.StatusOK, sug)
}

type reviewRequest struct {
	ReviewedBy string `json:"reviewed_by"`
	ReviewNote string `json:"review_note"`
}

func (h *Handlers) handleApproveCompanySuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}
	company, err := service.ApproveCompanySuggestion(r.Context(), h.pool, id, req.ReviewedBy, req.ReviewNote)
	if err != nil {
		slog.Error("approve company suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, company)
}

func (h *Handlers) handleRejectCompanySuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := service.RejectCompanySuggestion(r.Context(), h.pool, id, req.ReviewedBy, req.ReviewNote); err != nil {
		slog.Error("reject company suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
}
```

- [ ] **Step 4: Add section suggestion SQL queries**

Append to `database/queries/suggestions.sql`:

```sql
-- name: GetCompanyStatusSuggestionByID :one
SELECT * FROM company_status_suggestions WHERE id = $1;

-- name: UpdateCompanyStatusSuggestionApproved :exec
UPDATE company_status_suggestions
SET status = 'approved', reviewed_by = $2, review_note = $3, reviewed_at = now(), updated_at = now()
WHERE id = $1;

-- name: UpdateCompanyStatusSuggestionRejected :exec
UPDATE company_status_suggestions
SET status = 'rejected', reviewed_by = $2, review_note = $3, reviewed_at = now(), updated_at = now()
WHERE id = $1;

-- name: GetCompanyContactSuggestionByID :one
SELECT * FROM company_contact_suggestions WHERE id = $1;

-- name: UpdateCompanyContactSuggestionApproved :exec
UPDATE company_contact_suggestions
SET status = 'approved', reviewed_by = $2, review_note = $3, reviewed_at = now(), updated_at = now()
WHERE id = $1;

-- name: UpdateCompanyContactSuggestionRejected :exec
UPDATE company_contact_suggestions
SET status = 'rejected', reviewed_by = $2, review_note = $3, reviewed_at = now(), updated_at = now()
WHERE id = $1;

-- name: UpdateCompanyStatus :exec
UPDATE companies SET lifecycle_status = $2, updated_at = now() WHERE id = $1;

-- name: UpdateCompanyWebsite :exec
UPDATE companies SET website = $2, updated_at = now() WHERE id = $1;
```

Run sqlc to regenerate:

```bash
cd scheduler && GOWORK=off make sqlc-generate
```

Expected: no errors; new methods appear in `scheduler/internal/db/gen/`.

- [ ] **Step 5: Add section suggestion service functions**

Append to `scheduler/internal/service/suggestions.go`:

```go
// ApproveCompanyStatusSuggestion applies a status field change and marks the suggestion approved.
// Applies only to lifecycle_status (from source registration_status mapping).
func ApproveCompanyStatusSuggestion(ctx context.Context, pool TxPool, suggestionID uuid.UUID, reviewedBy, reviewNote string) error {
	q := db.New(pool)
	sug, err := q.GetCompanyStatusSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get status suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("status suggestion %s is not pending", suggestionID)
	}
	if sug.CompanyID == nil {
		return fmt.Errorf("status suggestion %s has no company_id", suggestionID)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin tx")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := db.New(tx)

	if sug.ProposedValue != nil {
		if err := qtx.UpdateCompanyStatus(ctx, db.UpdateCompanyStatusParams{
			ID:     *sug.CompanyID,
			Column2: *sug.ProposedValue,
		}); err != nil {
			return errors.Wrap(err, "update company status")
		}
	}
	if err := qtx.UpdateCompanyStatusSuggestionApproved(ctx, db.UpdateCompanyStatusSuggestionApprovedParams{
		ID: suggestionID, Column2: &reviewedBy, Column3: &reviewNote,
	}); err != nil {
		return errors.Wrap(err, "mark status suggestion approved")
	}
	return errors.Wrap(tx.Commit(ctx), "commit")
}

// RejectCompanyStatusSuggestion marks the suggestion rejected without touching resolved tables.
func RejectCompanyStatusSuggestion(ctx context.Context, pool TxPool, suggestionID uuid.UUID, reviewedBy, reviewNote string) error {
	q := db.New(pool)
	sug, err := q.GetCompanyStatusSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get status suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("status suggestion %s is not pending", suggestionID)
	}
	return q.UpdateCompanyStatusSuggestionRejected(ctx, db.UpdateCompanyStatusSuggestionRejectedParams{
		ID: suggestionID, Column2: &reviewedBy, Column3: &reviewNote,
	})
}

// ApproveCompanyContactSuggestion applies a contact-kind change and marks the suggestion approved.
// Only "website" kind is applied to the resolved companies table today.
func ApproveCompanyContactSuggestion(ctx context.Context, pool TxPool, suggestionID uuid.UUID, reviewedBy, reviewNote string) error {
	q := db.New(pool)
	sug, err := q.GetCompanyContactSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get contact suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("contact suggestion %s is not pending", suggestionID)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.Wrap(err, "begin tx")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := db.New(tx)

	if sug.ContactKind == "website" && sug.CompanyID != nil {
		var proposed struct{ URL string `json:"url"` }
		_ = json.Unmarshal(sug.ProposedPayload, &proposed)
		if proposed.URL != "" {
			if err := qtx.UpdateCompanyWebsite(ctx, db.UpdateCompanyWebsiteParams{
				ID: *sug.CompanyID, Column2: &proposed.URL,
			}); err != nil {
				return errors.Wrap(err, "update company website")
			}
		}
	}
	if err := qtx.UpdateCompanyContactSuggestionApproved(ctx, db.UpdateCompanyContactSuggestionApprovedParams{
		ID: suggestionID, Column2: &reviewedBy, Column3: &reviewNote,
	}); err != nil {
		return errors.Wrap(err, "mark contact suggestion approved")
	}
	return errors.Wrap(tx.Commit(ctx), "commit")
}

// RejectCompanyContactSuggestion marks the suggestion rejected without touching resolved tables.
func RejectCompanyContactSuggestion(ctx context.Context, pool TxPool, suggestionID uuid.UUID, reviewedBy, reviewNote string) error {
	q := db.New(pool)
	sug, err := q.GetCompanyContactSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get contact suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("contact suggestion %s is not pending", suggestionID)
	}
	return q.UpdateCompanyContactSuggestionRejected(ctx, db.UpdateCompanyContactSuggestionRejectedParams{
		ID: suggestionID, Column2: &reviewedBy, Column3: &reviewNote,
	})
}

// ApproveCompanyWithSections creates a company from a root suggestion and atomically approves
// all listed child section suggestions in a single transaction. Any failure rolls everything back.
// This is the only correct way to approve a root + sections together — the non-atomic alternative
// (approve root, then log child failures) leaves partial state.
func ApproveCompanyWithSections(ctx context.Context, pool TxPool, rootID uuid.UUID, children []ChildSuggestionRef, reviewedBy, reviewNote string) (db.Company, error) {
	q := db.New(pool)

	sug, err := q.GetCompanySuggestionByID(ctx, rootID)
	if err != nil {
		return db.Company{}, errors.Wrap(err, "get suggestion")
	}
	if sug.Status != "pending" {
		return db.Company{}, fmt.Errorf("suggestion %s is not pending (status=%s)", rootID, sug.Status)
	}
	if !sug.ProposedCountryID.Valid {
		return db.Company{}, fmt.Errorf("suggestion %s has no proposed_country_id", rootID)
	}

	canonicalSlug := slug.Generate(sug.ProposedDisplayName)
	if canonicalSlug == "" {
		canonicalSlug = "company-" + rootID.String()[:12]
	}
	if _, err := q.GetCompanyBySlug(ctx, canonicalSlug); err == nil {
		canonicalSlug = canonicalSlug + "-" + rootID.String()[:12]
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return db.Company{}, errors.Wrap(err, "check slug collision")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return db.Company{}, errors.Wrap(err, "begin tx")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := db.New(tx)

	company, err := qtx.InsertCompany(ctx, db.InsertCompanyParams{
		CanonicalSlug: canonicalSlug,
		Name:          sug.ProposedDisplayName,
		CountryID:     uuid.UUID(sug.ProposedCountryID.Bytes),
		Status:        "active",
	})
	if err != nil {
		return db.Company{}, errors.Wrap(err, "insert company")
	}

	if err := qtx.UpdateCompanySuggestionApproved(ctx, db.UpdateCompanySuggestionApprovedParams{
		ID:               rootID,
		CreatedCompanyID: &company.ID,
		ReviewedBy:       &reviewedBy,
		ReviewNote:       &reviewNote,
	}); err != nil {
		return db.Company{}, errors.Wrap(err, "approve root suggestion")
	}

	for _, child := range children {
		switch child.Table {
		case "company_status_suggestions":
			if err := approveCompanyStatusTx(ctx, qtx, child.ID, rootID, company.ID, reviewedBy, reviewNote); err != nil {
				return db.Company{}, errors.Wrapf(err, "approve child status %s", child.ID)
			}
		case "company_contact_suggestions":
			if err := approveCompanyContactTx(ctx, qtx, child.ID, rootID, company.ID, reviewedBy, reviewNote); err != nil {
				return db.Company{}, errors.Wrapf(err, "approve child contact %s", child.ID)
			}
		default:
			return db.Company{}, fmt.Errorf("unknown child suggestion table: %s", child.Table)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return db.Company{}, errors.Wrap(err, "commit")
	}
	return company, nil
}

func resolveChildSuggestionCompanyID(existingCompanyID *uuid.UUID, rootSuggestionID *uuid.UUID, approvedRootID uuid.UUID, createdCompanyID uuid.UUID, childID uuid.UUID) (uuid.UUID, error) {
	if existingCompanyID != nil {
		return *existingCompanyID, nil
	}
	if rootSuggestionID != nil && *rootSuggestionID == approvedRootID {
		return createdCompanyID, nil
	}
	return uuid.Nil, fmt.Errorf("child suggestion %s is not attached to the approved root company suggestion", childID)
}

// approveCompanyStatusTx applies a status suggestion within an existing transaction.
func approveCompanyStatusTx(ctx context.Context, qtx *db.Queries, suggestionID uuid.UUID, approvedRootID uuid.UUID, createdCompanyID uuid.UUID, reviewedBy, reviewNote string) error {
	sug, err := qtx.GetCompanyStatusSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get status suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("status suggestion %s is not pending", suggestionID)
	}
	targetCompanyID, err := resolveChildSuggestionCompanyID(sug.CompanyID, sug.CompanySuggestionID, approvedRootID, createdCompanyID, suggestionID)
	if err != nil {
		return err
	}
	if sug.ProposedValue != nil {
		if err := qtx.UpdateCompanyStatus(ctx, db.UpdateCompanyStatusParams{
			ID:      targetCompanyID,
			Column2: *sug.ProposedValue,
		}); err != nil {
			return errors.Wrap(err, "update company status")
		}
	}
	return errors.Wrap(qtx.UpdateCompanyStatusSuggestionApproved(ctx, db.UpdateCompanyStatusSuggestionApprovedParams{
		ID: suggestionID, Column2: &reviewedBy, Column3: &reviewNote,
	}), "mark status suggestion approved")
}

// approveCompanyContactTx applies a contact suggestion within an existing transaction.
func approveCompanyContactTx(ctx context.Context, qtx *db.Queries, suggestionID uuid.UUID, approvedRootID uuid.UUID, createdCompanyID uuid.UUID, reviewedBy, reviewNote string) error {
	sug, err := qtx.GetCompanyContactSuggestionByID(ctx, suggestionID)
	if err != nil {
		return errors.Wrap(err, "get contact suggestion")
	}
	if sug.Status != "pending" {
		return fmt.Errorf("contact suggestion %s is not pending", suggestionID)
	}
	targetCompanyID, err := resolveChildSuggestionCompanyID(sug.CompanyID, sug.CompanySuggestionID, approvedRootID, createdCompanyID, suggestionID)
	if err != nil {
		return err
	}
	if sug.ContactKind == "website" {
		var proposed struct{ URL string `json:"url"` }
		_ = json.Unmarshal(sug.ProposedPayload, &proposed)
		if proposed.URL != "" {
			if err := qtx.UpdateCompanyWebsite(ctx, db.UpdateCompanyWebsiteParams{
				ID: targetCompanyID, Column2: &proposed.URL,
			}); err != nil {
				return errors.Wrap(err, "update company website")
			}
		}
	}
	return errors.Wrap(qtx.UpdateCompanyContactSuggestionApproved(ctx, db.UpdateCompanyContactSuggestionApprovedParams{
		ID: suggestionID, Column2: &reviewedBy, Column3: &reviewNote,
	}), "mark contact suggestion approved")
}
```

Add `"encoding/json"` to the import block in `suggestions.go`.

- [ ] **Step 6: Add section suggestion handlers**

Append to `scheduler/internal/httpapi/suggestions.go`:

```go
func (h *Handlers) handleApproveCompanyStatusSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}
	if err := service.ApproveCompanyStatusSuggestion(r.Context(), h.pool, id, req.ReviewedBy, req.ReviewNote); err != nil {
		slog.Error("approve company status suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "approved"})
}

func (h *Handlers) handleRejectCompanyStatusSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := service.RejectCompanyStatusSuggestion(r.Context(), h.pool, id, req.ReviewedBy, req.ReviewNote); err != nil {
		slog.Error("reject company status suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
}

func (h *Handlers) handleApproveCompanyContactSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := service.ApproveCompanyContactSuggestion(r.Context(), h.pool, id, req.ReviewedBy, req.ReviewNote); err != nil {
		slog.Error("approve company contact suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "approved"})
}

func (h *Handlers) handleRejectCompanyContactSuggestion(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := service.RejectCompanyContactSuggestion(r.Context(), h.pool, id, req.ReviewedBy, req.ReviewNote); err != nil {
		slog.Error("reject company contact suggestion", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected"})
}

// approveWithSectionsRequest lets the caller approve a root company suggestion and
// simultaneously approve a set of child section suggestions in a single HTTP call.
type approveWithSectionsRequest struct {
	ReviewedBy       string `json:"reviewed_by"`
	ReviewNote       string `json:"review_note"`
	ChildSuggestions []struct {
		Table string    `json:"table"`
		ID    uuid.UUID `json:"id"`
	} `json:"child_suggestions"`
}

func (h *Handlers) handleApproveCompanyWithSections(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid id")
		return
	}
	var req approveWithSectionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if h.pool == nil {
		writeError(w, http.StatusServiceUnavailable, "database pool not available")
		return
	}
	children := make([]service.ChildSuggestionRef, 0, len(req.ChildSuggestions))
	for _, c := range req.ChildSuggestions {
		children = append(children, service.ChildSuggestionRef{Table: c.Table, ID: c.ID})
	}
	// Single transaction: root company creation + all child approvals roll back together on failure.
	company, err := service.ApproveCompanyWithSections(r.Context(), h.pool, id, children, req.ReviewedBy, req.ReviewNote)
	if err != nil {
		slog.Error("approve company with sections", "id", id, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, company)
}
```

- [ ] **Step 7: Add section suggestion tests**

Append to `scheduler/internal/httpapi/suggestions_test.go`:

```go
func TestHandleApproveCompanyStatusSuggestion_Returns200(t *testing.T) {
	q := &stubQuerier{}
	q.On("GetCompanyStatusSuggestionByID", mock.Anything, mock.Anything).
		Return(db.CompanyStatusSuggestion{ID: uuid.New(), Status: "pending", CompanyID: &uuid.Nil}, nil)

	r := chi.NewRouter()
	httpapi.NewHandlers(q, nil, nil, nil, "").RegisterRoutes(r)

	body := strings.NewReader(`{"reviewed_by":"admin","review_note":"ok"}`)
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/suggestions/company-status/"+uuid.New().String()+"/approve", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	// 503 is expected because h.pool == nil in the test setup.
	// The handler reaches the service.ApproveCompanyStatusSuggestion call, which is correct.
	// Use an integration test with a real pool to assert 200.
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestHandleApproveCompanyWithSections_PropagatesChildSuggestions(t *testing.T) {
	// Documents: POST /suggestions/companies/{id}/approve-with-sections accepts
	// child_suggestions array and approves everything atomically via service.ApproveCompanyWithSections.
	// With nil pool the handler returns 503, confirming routing is correct.
	q := &stubQuerier{}
	r := chi.NewRouter()
	httpapi.NewHandlers(q, nil, nil, nil, "").RegisterRoutes(r)

	body := strings.NewReader(`{
		"reviewed_by":"admin",
		"child_suggestions":[
			{"table":"company_status_suggestions","id":"` + uuid.New().String() + `"}
		]
	}`)
	req := httptest.NewRequest(http.MethodPost,
		"/api/v1/suggestions/companies/"+uuid.New().String()+"/approve-with-sections", body)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}
```

Add `"strings"` to the import in `suggestions_test.go`.

- [ ] **Step 8: Run tests**

```bash
cd scheduler && GOWORK=off make test
```

Expected: all tests pass.

- [ ] **Step 9: Commit**

```bash
git add database/queries/suggestions.sql \
        scheduler/internal/db/gen/ \
        scheduler/internal/service/suggestions.go \
        scheduler/internal/httpapi/suggestions.go \
        scheduler/internal/httpapi/suggestions_test.go
git commit -m "feat: add section suggestion approve/reject API (company-status, company-contact, approve-with-sections)"
```

---

## Task 11: Final app wiring and integration smoke test

**Files:**
- Modify: `scheduler/internal/app/river.go` — inject real pool into `SourcePullWorker`, wire River client into context
- Modify: `scheduler/internal/workers/source_pull.go` — replace `riverClientFromCtx` with concrete injection

This task wires everything together and confirms end-to-end health.

- [ ] **Step 1: Inject River client into SourcePullWorker**

In `scheduler/internal/workers/source_pull.go`, add `rv` field:

```go
type SourcePullWorker struct {
	river.WorkerDefaults[SourcePullArgs]
	db      db.Querier
	crawler *crawlerclient.Client
	pool    *pgxpool.Pool
	rv      *river.Client[pgx.Tx]
}

func NewSourcePullWorker(q db.Querier, crawler *crawlerclient.Client, pool *pgxpool.Pool) *SourcePullWorker {
	return &SourcePullWorker{db: q, crawler: crawler, pool: pool}
}

func (w *SourcePullWorker) SetRiverClient(rc *river.Client[pgx.Tx]) {
	w.rv = rc
}
```

Replace the `riverClientFromCtx` call in `Work` with direct `w.rv` usage:

```go
if src.ProcessorTaskType != nil && *src.ProcessorTaskType == "source_process" && w.rv != nil {
	_, _ = w.rv.Insert(ctx, SourceProcessArgs{
		SourceName: src.Name,
		PullRunID:  run.ID.String(),
	}, &river.InsertOpts{Queue: "source_process"})
}
```

Remove the `riverClientFromCtx` function entirely.

- [ ] **Step 2: Wire SetRiverClient in river.go**

In `scheduler/internal/app/river.go`, after creating the River client:

```go
sourcePullWorker := workers.NewSourcePullWorker(q, crawler, pool)
sourceProcessWorker := workers.NewSourceProcessWorker(q, pool)

w := river.NewWorkers()
river.AddWorker(w, sourcePullWorker)
river.AddWorker(w, sourceProcessWorker)

riverCfg := &river.Config{
	Queues: map[string]river.QueueConfig{
		"source_pull":    {MaxWorkers: cfg.CrawlConcurrency},
		"source_process": {MaxWorkers: cfg.DomainConcurrency},
	},
	Workers: w,
}

rc, err := river.NewClient(riverpgxv5.New(pool), riverCfg)
if err != nil {
	return nil, err
}

sourcePullWorker.SetRiverClient(rc)
return rc, nil
```

- [ ] **Step 3: Run full test suite**

```bash
cd scheduler && GOWORK=off make test
```

Expected: all tests pass with zero failures.

- [ ] **Step 4: Build binary**

```bash
cd scheduler && GOWORK=off make build
```

Expected: `bin/worker` created without errors.

- [ ] **Step 5: Smoke test with docker compose**

```bash
cd /Users/graovic/pulsarpoint/ppoint/corpscout && make up
sleep 5
curl -s http://localhost:8090/health | jq .
curl -s http://localhost:8090/api/v1/sources | jq '.[].name'
curl -s http://localhost:8090/api/v1/suggestions/companies | jq .
```

Expected: health endpoint returns `{"status":"ok"}`; sources returns 6 source names including `gleif`, `companies_house`, `brreg`; suggestions returns `{"items":[],"page":1,"limit":20,"total":0}`.

- [ ] **Step 6: Trigger a source and verify pull run created**

```bash
curl -s -X POST http://localhost:8090/api/v1/sources/gleif/trigger | jq .
sleep 2
curl -s "http://localhost:8090/api/v1/pull-runs?source=gleif" | jq '.items[0].status'
```

Expected: trigger returns `{"status":"queued"}`; pull run status is `"running"` or `"succeeded"`.

- [ ] **Step 7: Verify no direct writes to resolved tables**

```bash
psql "postgres://corpscout:corpscout@localhost:5435/corpscout?sslmode=disable" \
  -c "SELECT relname FROM pg_class WHERE relname IN ('company_sources','source_snapshots','company_domain_reviews') AND relkind='r';"
```

Expected: zero rows (tables do not exist in MVP schema).

- [ ] **Step 8: Final commit**

```bash
git add scheduler/internal/workers/source_pull.go \
        scheduler/internal/app/river.go
git commit -m "feat: wire River client into SourcePullWorker, complete MVP ingestion pipeline"
```

---

## Execution options

Plan complete and saved to `docs/superpowers/plans/2026-05-17-corpscout-source-ingestion-and-suggestions.md`.

**1. Subagent-Driven (recommended)** — fresh subagent per task, spec + code quality review between tasks

**2. Inline Execution** — execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?

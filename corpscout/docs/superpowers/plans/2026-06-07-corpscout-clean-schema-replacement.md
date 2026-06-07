# Corpscout Clean Schema Replacement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Corpscout POC database model with the approved clean schema for source execution, normalized source records, legal entities, central identities, brands, and web presence.

**Architecture:** This is a destructive database replacement for the POC tables in `companycollect/corpscout/database/migrations`. Keep stable reference/service concepts such as countries, NACE, exchange rates, LLM providers, and Temporal schedule metadata. Replace old source-specific and central-company tables with `registry`, `source_records`, `entities`, `identity`, and `web` schemas, then update sqlc queries and scheduler code to compile against the new schema.

**Tech Stack:** PostgreSQL migrations with `golang-migrate`, Go scheduler service, sqlc, pgx, Temporal, `log/slog`, `github.com/cockroachdb/errors`.

---

## Source Documents

- Schema design: `companycollect/corpscout/docs/company-identity-clean-replacement-schema.md`
- Current migrations: `companycollect/corpscout/database/migrations`
- sqlc config: `companycollect/corpscout/database/sqlc.yaml`
- Scheduler DB package: `companycollect/corpscout/scheduler/internal/db`
- Scheduler HTTP API: `companycollect/corpscout/scheduler/internal/httpapi`
- Scheduler app wiring: `companycollect/corpscout/scheduler/internal/app`

## Scope

This plan covers the first working milestone:

1. Create destructive migration files that remove old POC tables/schemas and create the new schema.
2. Preserve stable reference/service tables or recreate equivalent versions.
3. Replace old sqlc queries with new registry/source-record queries.
4. Regenerate sqlc and update scheduler compile errors.
5. Add database migration shape tests.

This plan does not fully rebuild the `/sources` UI or implement source Parquet import logic. It prepares the database and backend API boundary so those can be implemented next.

## File Structure

Create:

- `companycollect/corpscout/database/migrations/000110_clean_identity_schema_replacement.up.sql`
- `companycollect/corpscout/database/migrations/000110_clean_identity_schema_replacement.down.sql`
- `companycollect/corpscout/database/queries/registry_sources.sql`
- `companycollect/corpscout/database/queries/source_records.sql`
- `companycollect/corpscout/database/queries/entities_identity.sql`
- `companycollect/corpscout/scheduler/internal/db/clean_identity_schema_replacement_migration_test.go`

Modify:

- `companycollect/corpscout/database/queries/sources.sql`
- `companycollect/corpscout/database/queries/companies.sql`
- `companycollect/corpscout/database/queries/company_relationships.sql`
- `companycollect/corpscout/database/sqlc.yaml`
- `companycollect/corpscout/scheduler/internal/httpapi/source_read.go`
- `companycollect/corpscout/scheduler/internal/httpapi/source_patch.go`
- `companycollect/corpscout/scheduler/internal/httpapi/source_config.go`
- `companycollect/corpscout/scheduler/internal/httpapi/source_schedule.go`
- `companycollect/corpscout/scheduler/internal/httpapi/workflow_triggers.go`
- `companycollect/corpscout/scheduler/internal/httpapi/companies.go`
- `companycollect/corpscout/scheduler/internal/app/temporal.go`

Generated:

- `companycollect/corpscout/scheduler/internal/db/gen/*`

Delete or retire after replacement queries compile:

- `companycollect/corpscout/database/queries/brreg_source_profile.sql`
- `companycollect/corpscout/database/queries/ariregister_source_profile.sql`
- `companycollect/corpscout/database/queries/france_source_profile.sql`
- `companycollect/corpscout/database/queries/countrydata_finland_prh_ytj.sql`
- `companycollect/corpscout/database/queries/countrydata_united_states_colorado_entities.sql`
- `companycollect/corpscout/database/queries/countrydata_united_states_irs_eo_bmf.sql`
- `companycollect/corpscout/database/queries/countrydata_united_states_sam_gov_entity.sql`
- `companycollect/corpscout/database/queries/countrydata_united_states_sec_edgar.sql`

Retire only when the Go code no longer references them.

---

### Task 1: Add Migration Shape Tests

**Files:**
- Create: `companycollect/corpscout/scheduler/internal/db/clean_identity_schema_replacement_migration_test.go`

- [ ] **Step 1: Create the failing migration shape test**

Create `companycollect/corpscout/scheduler/internal/db/clean_identity_schema_replacement_migration_test.go`:

```go
package db

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCleanIdentitySchemaReplacementMigrationCreatesNewSchemas(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000110_clean_identity_schema_replacement.up.sql")
	require.NoError(t, err)
	sql := string(body)

	required := []string{
		"CREATE SCHEMA IF NOT EXISTS registry",
		"CREATE SCHEMA IF NOT EXISTS source_records",
		"CREATE SCHEMA IF NOT EXISTS entities",
		"CREATE SCHEMA IF NOT EXISTS identity",
		"CREATE SCHEMA IF NOT EXISTS web",
		"CREATE TABLE registry.sources",
		"CREATE TABLE registry.source_countries",
		"CREATE TABLE registry.source_runs",
		"CREATE TABLE registry.source_exports",
		"CREATE TABLE registry.source_export_files",
		"CREATE TABLE source_records.companies",
		"CREATE TABLE source_records.company_names",
		"CREATE TABLE source_records.identifiers",
		"CREATE TABLE source_records.legal_forms",
		"CREATE TABLE source_records.addresses",
		"CREATE TABLE source_records.contacts",
		"CREATE TABLE source_records.industries",
		"CREATE TABLE source_records.websites",
		"CREATE TABLE source_records.source_evidence",
		"CREATE TABLE entities.legal_entities",
		"CREATE TABLE entities.legal_entity_source_links",
		"CREATE TABLE entities.legal_entity_relationships",
		"CREATE TABLE identity.companies",
		"CREATE TABLE identity.company_legal_entity_links",
		"CREATE TABLE identity.brands",
		"CREATE TABLE identity.brand_company_links",
		"CREATE TABLE identity.brand_legal_entity_links",
		"CREATE TABLE identity.brand_relationships",
		"CREATE TABLE identity.company_relationships",
		"CREATE TABLE identity.relationship_edges",
		"CREATE TABLE web.domains",
		"CREATE TABLE web.websites",
		"CREATE TABLE web.company_website_links",
		"CREATE TABLE web.legal_entity_website_links",
		"CREATE TABLE web.brand_website_links",
		"CREATE TABLE web.source_record_website_links",
	}

	for _, needle := range required {
		require.Contains(t, sql, needle)
	}
}

func TestCleanIdentitySchemaReplacementMigrationDropsLegacyPOCTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000110_clean_identity_schema_replacement.up.sql")
	require.NoError(t, err)
	sql := string(body)

	requiredDrops := []string{
		"DROP SCHEMA IF EXISTS brreg_workflow CASCADE",
		"DROP SCHEMA IF EXISTS brreg_source CASCADE",
		"DROP SCHEMA IF EXISTS ariregister_workflow CASCADE",
		"DROP SCHEMA IF EXISTS ariregister_source CASCADE",
		"DROP SCHEMA IF EXISTS france_workflow CASCADE",
		"DROP SCHEMA IF EXISTS france_source CASCADE",
		"DROP SCHEMA IF EXISTS cvr_workflow CASCADE",
		"DROP SCHEMA IF EXISTS se_workflow CASCADE",
		"DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE",
		"DROP TABLE IF EXISTS companies CASCADE",
		"DROP TABLE IF EXISTS data_sources CASCADE",
		"DROP TABLE IF EXISTS source_pull_runs CASCADE",
		"DROP TABLE IF EXISTS domains CASCADE",
		"DROP TABLE IF EXISTS suggestions CASCADE",
	}

	for _, needle := range requiredDrops {
		require.Contains(t, sql, needle)
	}
}

func TestCleanIdentitySchemaReplacementMigrationHasTranslationStateWithoutRowErrors(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000110_clean_identity_schema_replacement.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "legal_name_en TEXT")
	require.Contains(t, sql, "translation_source_hash TEXT")
	require.Contains(t, sql, "is_translated BOOLEAN NOT NULL DEFAULT false")
	require.Contains(t, sql, "translation_required_fields TEXT[] NOT NULL DEFAULT '{}'::text[]")
	require.Contains(t, sql, "translated_fields TEXT[] NOT NULL DEFAULT '{}'::text[]")
	require.Contains(t, sql, "untranslated_fields TEXT[] NOT NULL DEFAULT '{}'::text[]")
	require.Contains(t, sql, "translation_status TEXT NOT NULL DEFAULT 'not_required'")
	require.NotContains(t, sql, "translation_version TEXT")
	require.NotContains(t, sql, "translation_error TEXT")
}

func TestCleanIdentitySchemaReplacementDownMigrationDropsNewSchemas(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000110_clean_identity_schema_replacement.down.sql")
	require.NoError(t, err)
	sql := string(body)

	for _, schema := range []string{"web", "identity", "entities", "source_records", "registry"} {
		require.Contains(t, sql, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	}

	if strings.Contains(sql, "DROP TABLE countries") {
		t.Fatal("down migration must not drop preserved reference table countries")
	}
}
```

- [ ] **Step 2: Run the failing test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run CleanIdentitySchemaReplacement -count=1
```

Expected: FAIL because `000110_clean_identity_schema_replacement.up.sql` and `.down.sql` do not exist yet.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/db/clean_identity_schema_replacement_migration_test.go
git commit -m "test: specify clean corpscout identity schema migration"
```

---

### Task 2: Create Destructive Schema Replacement Migration

**Files:**
- Create: `companycollect/corpscout/database/migrations/000110_clean_identity_schema_replacement.up.sql`
- Create: `companycollect/corpscout/database/migrations/000110_clean_identity_schema_replacement.down.sql`

- [ ] **Step 1: Create the up migration header and destructive cleanup**

Create `companycollect/corpscout/database/migrations/000110_clean_identity_schema_replacement.up.sql` with this opening section:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Clean replacement for POC schemas and tables.
-- Reference/service tables intentionally preserved:
-- countries, nace_*, exchange_rate_*, llm_providers, temporal_schedule_metadata.

DROP SCHEMA IF EXISTS source_translation CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_sec_edgar CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_sam_gov_entity CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_irs_eo_bmf CASCADE;
DROP SCHEMA IF EXISTS countrydata_united_states_colorado_entities CASCADE;
DROP SCHEMA IF EXISTS countrydata_finland_prh_ytj CASCADE;
DROP SCHEMA IF EXISTS se_workflow CASCADE;
DROP SCHEMA IF EXISTS france_source CASCADE;
DROP SCHEMA IF EXISTS france_workflow CASCADE;
DROP SCHEMA IF EXISTS cvr_workflow CASCADE;
DROP SCHEMA IF EXISTS ariregister_source CASCADE;
DROP SCHEMA IF EXISTS ariregister_workflow CASCADE;
DROP SCHEMA IF EXISTS brreg_source CASCADE;
DROP SCHEMA IF EXISTS brreg_workflow CASCADE;
DROP SCHEMA IF EXISTS dagster_brreg CASCADE;

DROP VIEW IF EXISTS v_company_services CASCADE;
DROP VIEW IF EXISTS v_company_markets CASCADE;
DROP VIEW IF EXISTS v_company_industries CASCADE;
DROP VIEW IF EXISTS v_company_emails CASCADE;
DROP VIEW IF EXISTS v_company_phones CASCADE;
DROP VIEW IF EXISTS v_company_locations CASCADE;
DROP VIEW IF EXISTS v_resolved_entities CASCADE;
DROP VIEW IF EXISTS v_domains CASCADE;
DROP VIEW IF EXISTS v_company_domains CASCADE;
DROP VIEW IF EXISTS v_company_sources CASCADE;
DROP VIEW IF EXISTS v_companies CASCADE;

DROP TABLE IF EXISTS company_relationship_suggestions CASCADE;
DROP TABLE IF EXISTS company_status_suggestions CASCADE;
DROP TABLE IF EXISTS company_location_suggestions CASCADE;
DROP TABLE IF EXISTS company_contact_suggestions CASCADE;
DROP TABLE IF EXISTS company_domain_suggestions CASCADE;
DROP TABLE IF EXISTS open_source_project_suggestions CASCADE;
DROP TABLE IF EXISTS organization_suggestions CASCADE;
DROP TABLE IF EXISTS company_suggestions CASCADE;
DROP TABLE IF EXISTS suggestion_company_relationships CASCADE;
DROP TABLE IF EXISTS suggestion_company_services CASCADE;
DROP TABLE IF EXISTS suggestion_company_markets CASCADE;
DROP TABLE IF EXISTS suggestion_company_industries CASCADE;
DROP TABLE IF EXISTS suggestion_company_financials CASCADE;
DROP TABLE IF EXISTS suggestion_company_phones CASCADE;
DROP TABLE IF EXISTS suggestion_company_emails CASCADE;
DROP TABLE IF EXISTS suggestion_company_locations CASCADE;
DROP TABLE IF EXISTS suggestion_company_domains CASCADE;
DROP TABLE IF EXISTS suggestion_company_profiles CASCADE;
DROP TABLE IF EXISTS suggestion_source_links CASCADE;
DROP TABLE IF EXISTS suggestions CASCADE;

DROP TABLE IF EXISTS cve_entity_links CASCADE;
DROP TABLE IF EXISTS cve_entity_link_suggestions CASCADE;
DROP TABLE IF EXISTS cpe_entity_links CASCADE;
DROP TABLE IF EXISTS cpe_entity_link_suggestions CASCADE;
DROP TABLE IF EXISTS open_source_projects CASCADE;
DROP TABLE IF EXISTS organizations CASCADE;

DROP TABLE IF EXISTS domain_crawl_job_pages CASCADE;
DROP TABLE IF EXISTS domain_crawl_jobs CASCADE;
DROP TABLE IF EXISTS domain_import_batches CASCADE;

DROP TABLE IF EXISTS company_financials CASCADE;
DROP TABLE IF EXISTS company_addresses CASCADE;
DROP TABLE IF EXISTS company_relationships CASCADE;
DROP TABLE IF EXISTS company_domain_reviews CASCADE;
DROP TABLE IF EXISTS company_domains CASCADE;
DROP TABLE IF EXISTS domains CASCADE;
DROP TABLE IF EXISTS company_services CASCADE;
DROP TABLE IF EXISTS company_markets CASCADE;
DROP TABLE IF EXISTS company_industries CASCADE;
DROP TABLE IF EXISTS company_emails CASCADE;
DROP TABLE IF EXISTS company_phones CASCADE;
DROP TABLE IF EXISTS company_locations CASCADE;
DROP TABLE IF EXISTS company_sources CASCADE;
DROP TABLE IF EXISTS company_aliases CASCADE;
DROP TABLE IF EXISTS companies CASCADE;

DROP TABLE IF EXISTS companies_house_sic_codes CASCADE;
DROP TABLE IF EXISTS domain_discovery_raw_inputs CASCADE;
DROP TABLE IF EXISTS ai_company_profile_raw_inputs CASCADE;
DROP TABLE IF EXISTS brreg_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS companies_house_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS gleif_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS ariregister_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS cvr_company_raw_inputs CASCADE;
DROP TABLE IF EXISTS brreg_enhanced_raw_inputs CASCADE;

DROP TABLE IF EXISTS source_snapshots CASCADE;
DROP TABLE IF EXISTS source_processor_states CASCADE;
DROP TABLE IF EXISTS source_sync_checkpoints CASCADE;
DROP TABLE IF EXISTS source_pull_runs CASCADE;
DROP TABLE IF EXISTS data_sources CASCADE;
DROP TABLE IF EXISTS temporal_executions CASCADE;
DROP TABLE IF EXISTS translation_cache CASCADE;
```

- [ ] **Step 2: Add registry schema DDL**

Append:

```sql
CREATE SCHEMA IF NOT EXISTS registry;

CREATE TABLE registry.sources (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  slug TEXT UNIQUE NOT NULL,
  display_name TEXT NOT NULL,
  description TEXT,
  coverage_scope TEXT NOT NULL DEFAULT 'unknown',
  default_country_id UUID REFERENCES countries(id) ON DELETE SET NULL,
  executable_path TEXT NOT NULL,
  working_directory TEXT,
  default_args JSONB NOT NULL DEFAULT '{}'::jsonb,
  environment_contract JSONB NOT NULL DEFAULT '{}'::jsonb,
  output_contract_version TEXT NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT true,
  schedule_enabled BOOLEAN NOT NULL DEFAULT false,
  schedule_kind TEXT NOT NULL DEFAULT 'manual',
  schedule_expression TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_registry_sources_slug CHECK (slug ~ '^[a-z0-9][a-z0-9_-]{1,126}[a-z0-9]$'),
  CONSTRAINT chk_registry_sources_coverage_scope CHECK (coverage_scope IN ('single_country', 'multi_country', 'global', 'unknown')),
  CONSTRAINT chk_registry_sources_schedule_kind CHECK (schedule_kind IN ('manual', 'interval', 'cron', 'event')),
  CONSTRAINT chk_registry_sources_default_args CHECK (jsonb_typeof(default_args) = 'object'),
  CONSTRAINT chk_registry_sources_environment_contract CHECK (jsonb_typeof(environment_contract) = 'object'),
  CONSTRAINT chk_registry_sources_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE registry.source_countries (
  source_id UUID NOT NULL REFERENCES registry.sources(id) ON DELETE CASCADE,
  country_id UUID NOT NULL REFERENCES countries(id) ON DELETE CASCADE,
  coverage_status TEXT NOT NULL DEFAULT 'declared',
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (source_id, country_id),
  CONSTRAINT chk_registry_source_countries_status CHECK (coverage_status IN ('declared', 'observed', 'disabled')),
  CONSTRAINT chk_registry_source_countries_evidence CHECK (jsonb_typeof(evidence) = 'object')
);

CREATE TABLE registry.source_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES registry.sources(id) ON DELETE CASCADE,
  temporal_workflow_id TEXT,
  temporal_run_id TEXT,
  command TEXT NOT NULL,
  args JSONB NOT NULL DEFAULT '{}'::jsonb,
  trigger_type TEXT NOT NULL DEFAULT 'manual',
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  exit_code INTEGER,
  stdout_result JSONB NOT NULL DEFAULT '{}'::jsonb,
  error_message TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_registry_source_runs_trigger CHECK (trigger_type IN ('manual', 'scheduled', 'retry', 'backfill')),
  CONSTRAINT chk_registry_source_runs_status CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
  CONSTRAINT chk_registry_source_runs_args CHECK (jsonb_typeof(args) = 'object'),
  CONSTRAINT chk_registry_source_runs_stdout CHECK (jsonb_typeof(stdout_result) = 'object'),
  CONSTRAINT chk_registry_source_runs_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE registry.source_exports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES registry.sources(id) ON DELETE CASCADE,
  source_run_id UUID REFERENCES registry.source_runs(id) ON DELETE SET NULL,
  manifest_path TEXT NOT NULL,
  manifest_sha256 TEXT,
  export_kind TEXT NOT NULL DEFAULT 'source',
  schema_version TEXT NOT NULL,
  run_key TEXT NOT NULL,
  created_at_source TIMESTAMPTZ,
  records_seen BIGINT NOT NULL DEFAULT 0,
  records_exported BIGINT NOT NULL DEFAULT 0,
  decode_errors BIGINT NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, run_key),
  CONSTRAINT chk_registry_source_exports_kind CHECK (export_kind IN ('source', 'final', 'snapshot', 'other')),
  CONSTRAINT chk_registry_source_exports_counts CHECK (records_seen >= 0 AND records_exported >= 0 AND decode_errors >= 0),
  CONSTRAINT chk_registry_source_exports_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE registry.source_export_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_export_id UUID NOT NULL REFERENCES registry.source_exports(id) ON DELETE CASCADE,
  file_name TEXT NOT NULL,
  file_path TEXT NOT NULL,
  row_count BIGINT NOT NULL DEFAULT 0,
  sha256 TEXT,
  schema_hash TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (source_export_id, file_name),
  CONSTRAINT chk_registry_source_export_files_count CHECK (row_count >= 0),
  CONSTRAINT chk_registry_source_export_files_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE registry.import_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_export_id UUID NOT NULL REFERENCES registry.source_exports(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'running',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  records_seen BIGINT NOT NULL DEFAULT 0,
  records_inserted BIGINT NOT NULL DEFAULT 0,
  records_updated BIGINT NOT NULL DEFAULT 0,
  records_unchanged BIGINT NOT NULL DEFAULT 0,
  records_failed BIGINT NOT NULL DEFAULT 0,
  error_message TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT chk_registry_import_runs_status CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled')),
  CONSTRAINT chk_registry_import_runs_counts CHECK (
    records_seen >= 0 AND records_inserted >= 0 AND records_updated >= 0 AND records_unchanged >= 0 AND records_failed >= 0
  ),
  CONSTRAINT chk_registry_import_runs_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE TABLE registry.import_run_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  import_run_id UUID NOT NULL REFERENCES registry.import_runs(id) ON DELETE CASCADE,
  source_export_file_id UUID NOT NULL REFERENCES registry.source_export_files(id) ON DELETE CASCADE,
  status TEXT NOT NULL DEFAULT 'pending',
  rows_seen BIGINT NOT NULL DEFAULT 0,
  rows_inserted BIGINT NOT NULL DEFAULT 0,
  rows_updated BIGINT NOT NULL DEFAULT 0,
  rows_failed BIGINT NOT NULL DEFAULT 0,
  error_message TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (import_run_id, source_export_file_id),
  CONSTRAINT chk_registry_import_run_files_status CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'skipped')),
  CONSTRAINT chk_registry_import_run_files_counts CHECK (rows_seen >= 0 AND rows_inserted >= 0 AND rows_updated >= 0 AND rows_failed >= 0),
  CONSTRAINT chk_registry_import_run_files_metadata CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE INDEX idx_registry_sources_enabled ON registry.sources(enabled, slug);
CREATE INDEX idx_registry_source_runs_source_started ON registry.source_runs(source_id, started_at DESC);
CREATE INDEX idx_registry_source_exports_source_created ON registry.source_exports(source_id, created_at DESC);
CREATE INDEX idx_registry_import_runs_export_started ON registry.import_runs(source_export_id, started_at DESC);
```

- [ ] **Step 3: Add source_records schema DDL**

Copy the full source-record table definitions from `company-identity-clean-replacement-schema.md` into the migration before running sqlc. Treat that doc as the source of truth for column names, constraints, and relationships; if the migration needs to differ, update the design doc in the same commit and explain the difference in the commit body.

The migration must include this `source_records.companies` definition:

```sql
CREATE SCHEMA IF NOT EXISTS source_records;

CREATE TABLE source_records.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  source_id UUID NOT NULL REFERENCES registry.sources(id) ON DELETE CASCADE,
  source_export_id UUID REFERENCES registry.source_exports(id) ON DELETE SET NULL,
  source_record_id TEXT NOT NULL,
  source_native_id TEXT,
  country_id UUID REFERENCES countries(id) ON DELETE SET NULL,
  jurisdiction_code TEXT,
  registration_number TEXT,
  legal_name TEXT,
  legal_name_en TEXT,
  legal_name_normalized TEXT,
  lifecycle_status TEXT,
  is_active BOOLEAN,
  primary_website TEXT,
  source_updated_at TIMESTAMPTZ,
  source_payload_hash TEXT,
  record_hash TEXT NOT NULL,
  raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  translation_source_hash TEXT,
  is_translated BOOLEAN NOT NULL DEFAULT false,
  translation_required_fields TEXT[] NOT NULL DEFAULT '{}'::text[],
  translated_fields TEXT[] NOT NULL DEFAULT '{}'::text[],
  untranslated_fields TEXT[] NOT NULL DEFAULT '{}'::text[],
  translation_status TEXT NOT NULL DEFAULT 'not_required',
  translated_at TIMESTAMPTZ,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (source_id, source_record_id),
  CONSTRAINT chk_source_records_companies_translation_status CHECK (
    translation_status IN ('not_required', 'pending', 'partial', 'translated', 'failed')
  ),
  CONSTRAINT chk_source_records_companies_raw CHECK (jsonb_typeof(raw_payload) = 'object'),
  CONSTRAINT chk_source_records_companies_normalized CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_source_records_companies_evidence CHECK (jsonb_typeof(evidence) = 'object')
);
```

The same migration must also include the complete approved DDL from the design doc for these child tables:

```text
source_records.company_names
source_records.identifiers
source_records.legal_forms
source_records.addresses
source_records.contacts
source_records.industries
source_records.websites
source_records.source_evidence
```

Use the exact column definitions from `company-identity-clean-replacement-schema.md`. Add indexes:

```sql
CREATE INDEX idx_source_records_companies_source_country ON source_records.companies(source_id, country_id);
CREATE INDEX idx_source_records_companies_country_name ON source_records.companies(country_id, legal_name_normalized);
CREATE INDEX idx_source_records_companies_translation_queue ON source_records.companies(translation_status, updated_at)
  WHERE translation_status IN ('pending', 'partial', 'failed');
CREATE INDEX idx_source_records_company_names_company ON source_records.company_names(source_company_id);
CREATE INDEX idx_source_records_identifiers_lookup ON source_records.identifiers(identifier_type, identifier_value_normalized);
CREATE INDEX idx_source_records_addresses_company ON source_records.addresses(source_company_id);
CREATE INDEX idx_source_records_industries_nace ON source_records.industries(nace_code_id) WHERE nace_code_id IS NOT NULL;
CREATE INDEX idx_source_records_websites_host ON source_records.websites(host) WHERE host IS NOT NULL;
```

- [ ] **Step 4: Add entities, identity, and web schema DDL**

Copy the complete approved DDL from `company-identity-clean-replacement-schema.md` for:

```text
entities.legal_entities
entities.legal_entity_source_links
entities.legal_entity_relationships
identity.companies
identity.company_legal_entity_links
identity.brands
identity.brand_company_links
identity.brand_legal_entity_links
identity.brand_source_links
identity.company_relationships
identity.brand_relationships
identity.relationship_edges
web.domains
web.websites
web.company_website_links
web.legal_entity_website_links
web.brand_website_links
web.source_record_website_links
```

Required relationship indexes:

```sql
CREATE INDEX idx_entities_legal_entities_country_reg ON entities.legal_entities(country_id, registration_number);
CREATE INDEX idx_entities_legal_entities_name ON entities.legal_entities(canonical_name_normalized);
CREATE INDEX idx_entities_source_links_source_company ON entities.legal_entity_source_links(source_company_id);
CREATE INDEX idx_identity_companies_name ON identity.companies(canonical_name_normalized);
CREATE INDEX idx_identity_company_legal_links_legal_entity ON identity.company_legal_entity_links(legal_entity_id);
CREATE INDEX idx_identity_brands_name ON identity.brands(canonical_name_normalized);
CREATE INDEX idx_identity_edges_subject ON identity.relationship_edges(subject_type, subject_id, status);
CREATE INDEX idx_identity_edges_object ON identity.relationship_edges(object_type, object_id, status);
CREATE INDEX idx_web_domains_normalized ON web.domains(normalized_domain);
CREATE INDEX idx_web_websites_domain ON web.websites(domain_id);
```

- [ ] **Step 5: Add compatibility views for initial API work**

Append:

```sql
CREATE VIEW registry.v_sources AS
SELECT
  source.id,
  source.slug,
  source.display_name,
  source.description,
  source.coverage_scope,
  source.default_country_id,
  country.iso_alpha2 AS default_country_iso2,
  source.executable_path,
  source.enabled,
  source.schedule_enabled,
  source.schedule_kind,
  source.schedule_expression,
  source.created_at,
  source.updated_at,
  latest_run.status AS last_run_status,
  latest_run.started_at AS last_started_at,
  latest_run.finished_at AS last_finished_at,
  latest_run.error_message AS last_error,
  latest_export.manifest_path AS last_manifest_path,
  latest_export.schema_version AS last_schema_version,
  latest_export.records_exported AS last_records_exported
FROM registry.sources source
LEFT JOIN countries country ON country.id = source.default_country_id
LEFT JOIN LATERAL (
  SELECT run.status, run.started_at, run.finished_at, run.error_message
  FROM registry.source_runs run
  WHERE run.source_id = source.id
  ORDER BY run.started_at DESC
  LIMIT 1
) latest_run ON true
LEFT JOIN LATERAL (
  SELECT export.manifest_path, export.schema_version, export.records_exported
  FROM registry.source_exports export
  WHERE export.source_id = source.id
  ORDER BY export.created_at DESC
  LIMIT 1
) latest_export ON true;

CREATE VIEW entities.v_legal_entity_source_coverage AS
SELECT
  legal.id AS legal_entity_id,
  legal.canonical_name,
  legal.country_id,
  count(link.id)::integer AS source_count,
  jsonb_agg(
    jsonb_build_object(
      'source_id', link.source_id,
      'source_company_id', link.source_company_id,
      'match_method', link.match_method,
      'match_confidence', link.match_confidence,
      'field_coverage', link.field_coverage
    )
    ORDER BY link.match_confidence DESC
  ) AS sources
FROM entities.legal_entities legal
LEFT JOIN entities.legal_entity_source_links link ON link.legal_entity_id = legal.id
GROUP BY legal.id;
```

- [ ] **Step 6: Create the down migration**

Create `companycollect/corpscout/database/migrations/000110_clean_identity_schema_replacement.down.sql`:

```sql
DROP VIEW IF EXISTS entities.v_legal_entity_source_coverage;
DROP VIEW IF EXISTS registry.v_sources;

DROP SCHEMA IF EXISTS web CASCADE;
DROP SCHEMA IF EXISTS identity CASCADE;
DROP SCHEMA IF EXISTS entities CASCADE;
DROP SCHEMA IF EXISTS source_records CASCADE;
DROP SCHEMA IF EXISTS registry CASCADE;
```

The down migration intentionally does not recreate POC tables.

- [ ] **Step 7: Run migration shape tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/db -run CleanIdentitySchemaReplacement -count=1
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/database/migrations/000110_clean_identity_schema_replacement.up.sql corpscout/database/migrations/000110_clean_identity_schema_replacement.down.sql
git commit -m "feat: replace corpscout poc schema"
```

---

### Task 3: Replace sqlc Queries For Registry And Source Records

**Files:**
- Create: `companycollect/corpscout/database/queries/registry_sources.sql`
- Create: `companycollect/corpscout/database/queries/source_records.sql`
- Create: `companycollect/corpscout/database/queries/entities_identity.sql`
- Modify: `companycollect/corpscout/database/queries/sources.sql`

- [ ] **Step 1: Replace source list queries**

Replace `companycollect/corpscout/database/queries/sources.sql` with:

```sql
-- name: GetRegistrySourceBySlug :one
SELECT * FROM registry.sources WHERE slug = $1;

-- name: ListRegistrySources :many
SELECT * FROM registry.v_sources ORDER BY display_name, slug;

-- name: UpdateRegistrySourceEnabled :exec
UPDATE registry.sources
SET enabled = $2, updated_at = now()
WHERE slug = $1;

-- name: UpdateRegistrySourceScheduleEnabled :exec
UPDATE registry.sources
SET schedule_enabled = $2, updated_at = now()
WHERE slug = $1;

-- name: UpdateRegistrySourceSchedule :exec
UPDATE registry.sources
SET schedule_kind = $2, schedule_expression = $3, updated_at = now()
WHERE slug = $1;

-- name: UpdateRegistrySourceDefaultArgs :exec
UPDATE registry.sources
SET default_args = $2, updated_at = now()
WHERE slug = $1;
```

- [ ] **Step 2: Add registry write/read queries**

Create `companycollect/corpscout/database/queries/registry_sources.sql`:

```sql
-- name: UpsertRegistrySource :one
INSERT INTO registry.sources (
  slug, display_name, description, coverage_scope, default_country_id,
  executable_path, working_directory, default_args, environment_contract,
  output_contract_version, enabled, schedule_enabled, schedule_kind,
  schedule_expression, metadata
)
VALUES (
  sqlc.arg('slug')::text,
  sqlc.arg('display_name')::text,
  sqlc.narg('description')::text,
  sqlc.arg('coverage_scope')::text,
  sqlc.narg('default_country_id')::uuid,
  sqlc.arg('executable_path')::text,
  sqlc.narg('working_directory')::text,
  sqlc.arg('default_args')::jsonb,
  sqlc.arg('environment_contract')::jsonb,
  sqlc.arg('output_contract_version')::text,
  sqlc.arg('enabled')::boolean,
  sqlc.arg('schedule_enabled')::boolean,
  sqlc.arg('schedule_kind')::text,
  sqlc.narg('schedule_expression')::text,
  sqlc.arg('metadata')::jsonb
)
ON CONFLICT (slug) DO UPDATE SET
  display_name = EXCLUDED.display_name,
  description = EXCLUDED.description,
  coverage_scope = EXCLUDED.coverage_scope,
  default_country_id = EXCLUDED.default_country_id,
  executable_path = EXCLUDED.executable_path,
  working_directory = EXCLUDED.working_directory,
  default_args = EXCLUDED.default_args,
  environment_contract = EXCLUDED.environment_contract,
  output_contract_version = EXCLUDED.output_contract_version,
  enabled = EXCLUDED.enabled,
  schedule_enabled = EXCLUDED.schedule_enabled,
  schedule_kind = EXCLUDED.schedule_kind,
  schedule_expression = EXCLUDED.schedule_expression,
  metadata = EXCLUDED.metadata,
  updated_at = now()
RETURNING *;

-- name: CreateSourceRun :one
INSERT INTO registry.source_runs (
  source_id, temporal_workflow_id, temporal_run_id, command, args,
  trigger_type, status, metadata
)
VALUES (
  sqlc.arg('source_id')::uuid,
  sqlc.narg('temporal_workflow_id')::text,
  sqlc.narg('temporal_run_id')::text,
  sqlc.arg('command')::text,
  sqlc.arg('args')::jsonb,
  sqlc.arg('trigger_type')::text,
  sqlc.arg('status')::text,
  sqlc.arg('metadata')::jsonb
)
RETURNING *;

-- name: FinishSourceRun :one
UPDATE registry.source_runs
SET
  status = sqlc.arg('status')::text,
  finished_at = now(),
  exit_code = sqlc.narg('exit_code')::integer,
  stdout_result = sqlc.arg('stdout_result')::jsonb,
  error_message = sqlc.narg('error_message')::text,
  metadata = sqlc.arg('metadata')::jsonb
WHERE id = sqlc.arg('id')::uuid
RETURNING *;

-- name: CreateSourceExport :one
INSERT INTO registry.source_exports (
  source_id, source_run_id, manifest_path, manifest_sha256, export_kind,
  schema_version, run_key, created_at_source, records_seen,
  records_exported, decode_errors, metadata
)
VALUES (
  sqlc.arg('source_id')::uuid,
  sqlc.narg('source_run_id')::uuid,
  sqlc.arg('manifest_path')::text,
  sqlc.narg('manifest_sha256')::text,
  sqlc.arg('export_kind')::text,
  sqlc.arg('schema_version')::text,
  sqlc.arg('run_key')::text,
  sqlc.narg('created_at_source')::timestamptz,
  sqlc.arg('records_seen')::bigint,
  sqlc.arg('records_exported')::bigint,
  sqlc.arg('decode_errors')::bigint,
  sqlc.arg('metadata')::jsonb
)
RETURNING *;
```

- [ ] **Step 3: Add source record query file**

Create `companycollect/corpscout/database/queries/source_records.sql`:

```sql
-- name: UpsertSourceRecordCompany :one
INSERT INTO source_records.companies (
  source_id, source_export_id, source_record_id, source_native_id, country_id,
  jurisdiction_code, registration_number, legal_name, legal_name_en,
  legal_name_normalized, lifecycle_status, is_active, primary_website,
  source_updated_at, source_payload_hash, record_hash, raw_payload,
  normalized_payload, evidence, translation_source_hash, is_translated,
  translation_required_fields, translated_fields, untranslated_fields,
  translation_status, translated_at
)
VALUES (
  sqlc.arg('source_id')::uuid,
  sqlc.narg('source_export_id')::uuid,
  sqlc.arg('source_record_id')::text,
  sqlc.narg('source_native_id')::text,
  sqlc.narg('country_id')::uuid,
  sqlc.narg('jurisdiction_code')::text,
  sqlc.narg('registration_number')::text,
  sqlc.narg('legal_name')::text,
  sqlc.narg('legal_name_en')::text,
  sqlc.narg('legal_name_normalized')::text,
  sqlc.narg('lifecycle_status')::text,
  sqlc.narg('is_active')::boolean,
  sqlc.narg('primary_website')::text,
  sqlc.narg('source_updated_at')::timestamptz,
  sqlc.narg('source_payload_hash')::text,
  sqlc.arg('record_hash')::text,
  sqlc.arg('raw_payload')::jsonb,
  sqlc.arg('normalized_payload')::jsonb,
  sqlc.arg('evidence')::jsonb,
  sqlc.narg('translation_source_hash')::text,
  sqlc.arg('is_translated')::boolean,
  sqlc.arg('translation_required_fields')::text[],
  sqlc.arg('translated_fields')::text[],
  sqlc.arg('untranslated_fields')::text[],
  sqlc.arg('translation_status')::text,
  sqlc.narg('translated_at')::timestamptz
)
ON CONFLICT (source_id, source_record_id) DO UPDATE SET
  source_export_id = EXCLUDED.source_export_id,
  source_native_id = EXCLUDED.source_native_id,
  country_id = EXCLUDED.country_id,
  jurisdiction_code = EXCLUDED.jurisdiction_code,
  registration_number = EXCLUDED.registration_number,
  legal_name = EXCLUDED.legal_name,
  legal_name_en = EXCLUDED.legal_name_en,
  legal_name_normalized = EXCLUDED.legal_name_normalized,
  lifecycle_status = EXCLUDED.lifecycle_status,
  is_active = EXCLUDED.is_active,
  primary_website = EXCLUDED.primary_website,
  source_updated_at = EXCLUDED.source_updated_at,
  source_payload_hash = EXCLUDED.source_payload_hash,
  record_hash = EXCLUDED.record_hash,
  raw_payload = EXCLUDED.raw_payload,
  normalized_payload = EXCLUDED.normalized_payload,
  evidence = EXCLUDED.evidence,
  translation_source_hash = EXCLUDED.translation_source_hash,
  is_translated = EXCLUDED.is_translated,
  translation_required_fields = EXCLUDED.translation_required_fields,
  translated_fields = EXCLUDED.translated_fields,
  untranslated_fields = EXCLUDED.untranslated_fields,
  translation_status = EXCLUDED.translation_status,
  translated_at = EXCLUDED.translated_at,
  last_seen_at = now(),
  updated_at = now()
RETURNING *;

-- name: ListSourceRecordCompaniesForTranslation :many
SELECT *
FROM source_records.companies
WHERE translation_status IN ('pending', 'partial', 'failed')
ORDER BY updated_at
LIMIT $1;

-- name: ListSourceRecordCompaniesByCountry :many
SELECT *
FROM source_records.companies
WHERE country_id = $1
ORDER BY legal_name_normalized NULLS LAST, id
LIMIT $2 OFFSET $3;
```

- [ ] **Step 4: Add entity/identity read queries**

Create `companycollect/corpscout/database/queries/entities_identity.sql`:

```sql
-- name: GetLegalEntity :one
SELECT * FROM entities.legal_entities WHERE id = $1;

-- name: GetIdentityCompany :one
SELECT * FROM identity.companies WHERE id = $1;

-- name: GetBrand :one
SELECT * FROM identity.brands WHERE id = $1;

-- name: ListLegalEntitySourceLinks :many
SELECT *
FROM entities.legal_entity_source_links
WHERE legal_entity_id = $1
ORDER BY is_primary DESC, match_confidence DESC, created_at;

-- name: ListCompanyLegalEntityLinks :many
SELECT *
FROM identity.company_legal_entity_links
WHERE company_id = $1
ORDER BY is_primary DESC, created_at;
```

- [ ] **Step 5: Run sqlc and capture compile failures**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

Expected: sqlc may fail if remaining query files reference dropped tables. Remove or rewrite those query files until sqlc succeeds.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/database/queries corpscout/scheduler/internal/db/gen
git commit -m "feat: add clean schema sqlc queries"
```

---

### Task 4: Repair Scheduler API Compile Errors

**Files:**
- Modify files under `companycollect/corpscout/scheduler/internal/httpapi`
- Modify files under `companycollect/corpscout/scheduler/internal/app`

- [ ] **Step 1: Run scheduler tests to get compile errors**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: FAIL with compile errors for removed sqlc methods/types such as `ListSources`, `GetSourceByName`, or old company DTOs.

- [ ] **Step 2: Update source read handler**

In `scheduler/internal/httpapi/source_read.go`, replace old `db.DataSource` usage with generated `db.RegistrySource` and `db.RegistryVSources` rows.

Required behavior:

```text
GET /sources returns registry.v_sources rows
GET /sources/{slug} returns registry.sources row by slug
```

Keep external JSON names stable where possible:

```json
{
  "id": "...",
  "name": "finland_prh_ytj",
  "display_name": "Finland PRH YTJ",
  "coverage_scope": "single_country",
  "executable_path": "...",
  "enabled": true,
  "schedule_enabled": false,
  "schedule_kind": "manual",
  "schedule_expression": null,
  "last_run_status": null,
  "last_manifest_path": null
}
```

- [ ] **Step 3: Update source patch/config/schedule handlers**

Update these handlers to call:

```text
UpdateRegistrySourceEnabled
UpdateRegistrySourceScheduleEnabled
UpdateRegistrySourceSchedule
UpdateRegistrySourceDefaultArgs
```

Return safe external errors:

```go
slog.Error("update registry source", "source_slug", slug, "error", err)
writeError(w, http.StatusInternalServerError, "internal error")
```

Lower-level code should wrap errors with `github.com/cockroachdb/errors`; handlers log once.

- [ ] **Step 4: Temporarily disable old source-specific detail endpoints**

For source-specific routes that depend on removed tables such as BRREG source explorer, Ariregister source entries, countrydata raw records, and old workflow task state, return:

```go
writeError(w, http.StatusNotImplemented, "source-specific legacy endpoint retired by clean schema replacement")
```

Add focused tests for one disabled route per handler file touched. Expected response: HTTP 501 with the safe message.

- [ ] **Step 5: Run tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./...
```

Expected: PASS or only failures from integration tests requiring unavailable external services. Fix compile failures before moving on.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/httpapi corpscout/scheduler/internal/app corpscout/scheduler/internal/db/gen
git commit -m "feat: wire scheduler api to clean registry schema"
```

---

### Task 5: Add Source Execution Activity Skeleton

**Files:**
- Create: `companycollect/corpscout/scheduler/internal/sourceexec/activity.go`
- Create: `companycollect/corpscout/scheduler/internal/sourceexec/activity_test.go`
- Modify: `companycollect/corpscout/scheduler/internal/app/temporal.go`

- [ ] **Step 1: Write activity tests**

Create `scheduler/internal/sourceexec/activity_test.go`:

```go
package sourceexec

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildCommandUsesSourceConfig(t *testing.T) {
	source := SourceCommand{
		ExecutablePath: "/bin/echo",
		WorkingDirectory: "/tmp",
		DefaultArgs: json.RawMessage(`{"args":["sync","--source","prhytj"]}`),
	}

	cmd, err := BuildCommand(context.Background(), source, TriggerInput{ExtraArgs: []string{"--build-export"}})
	require.NoError(t, err)
	require.Equal(t, "/bin/echo", cmd.Path)
	require.Equal(t, "/tmp", cmd.Dir)
	require.Equal(t, []string{"sync", "--source", "prhytj", "--build-export"}, cmd.Args)
}

func TestBuildCommandRejectsMissingExecutable(t *testing.T) {
	_, err := BuildCommand(context.Background(), SourceCommand{}, TriggerInput{})
	require.ErrorContains(t, err, "executable path is required")
}

func TestRunCommandParsesJSONStdout(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "source.sh")
	err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '{\"status\":\"ok\",\"manifest_path\":\"/tmp/manifest.json\"}'\n"), 0o755)
	require.NoError(t, err)

	result, err := RunCommand(context.Background(), Command{Path: script})
	require.NoError(t, err)
	require.Equal(t, "ok", result.Status)
	require.Equal(t, "/tmp/manifest.json", result.ManifestPath)
}
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/sourceexec -count=1
```

Expected: FAIL because package does not exist.

- [ ] **Step 3: Implement activity skeleton**

Create `scheduler/internal/sourceexec/activity.go`:

```go
package sourceexec

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"

	"github.com/cockroachdb/errors"
)

type SourceCommand struct {
	ExecutablePath string          `json:"executable_path"`
	WorkingDirectory string        `json:"working_directory,omitempty"`
	DefaultArgs    json.RawMessage `json:"default_args"`
}

type TriggerInput struct {
	ExtraArgs []string `json:"extra_args,omitempty"`
}

type Command struct {
	Path string
	Dir  string
	Args []string
}

type commandArgs struct {
	Args []string `json:"args"`
}

type Result struct {
	Status       string `json:"status"`
	ManifestPath string `json:"manifest_path,omitempty"`
	RunID        string `json:"run_id,omitempty"`
}

func BuildCommand(ctx context.Context, source SourceCommand, input TriggerInput) (Command, error) {
	_ = ctx
	if source.ExecutablePath == "" {
		return Command{}, errors.New("executable path is required")
	}

	var parsed commandArgs
	if len(source.DefaultArgs) > 0 {
		if err := json.Unmarshal(source.DefaultArgs, &parsed); err != nil {
			return Command{}, errors.Wrap(err, "parse source default args")
		}
	}

	args := make([]string, 0, len(parsed.Args)+len(input.ExtraArgs))
	args = append(args, parsed.Args...)
	args = append(args, input.ExtraArgs...)

	return Command{Path: source.ExecutablePath, Dir: source.WorkingDirectory, Args: args}, nil
}

func RunCommand(ctx context.Context, command Command) (Result, error) {
	cmd := exec.CommandContext(ctx, command.Path, command.Args...)
	if command.Dir != "" {
		cmd.Dir = command.Dir
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return Result{}, errors.Wrapf(err, "run source command stderr=%s", stderr.String())
	}

	var result Result
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return Result{}, errors.Wrap(err, "parse source command stdout")
	}
	return result, nil
}
```

- [ ] **Step 4: Register skeleton in Temporal app wiring**

Add registration in `scheduler/internal/app/temporal.go` only after existing worker construction is cleaned up enough to compile. Register concrete functions directly with Temporal worker APIs, following the AGENTS.md instruction to avoid local registry interfaces.

- [ ] **Step 5: Run package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
GOWORK=off go test ./internal/sourceexec -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/scheduler/internal/sourceexec corpscout/scheduler/internal/app/temporal.go
git commit -m "feat: add source executable activity skeleton"
```

---

### Task 6: Verify Migration, sqlc, And Scheduler

**Files:**
- Generated: `companycollect/corpscout/scheduler/internal/db/gen/*`

- [ ] **Step 1: Generate sqlc**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

Expected: PASS.

- [ ] **Step 2: Run scheduler tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make test
```

Expected: PASS.

- [ ] **Step 3: Run migration on test database**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make migrate-test-up
```

Expected: PASS. If the local test database already has partial POC state, the destructive migration must still apply cleanly.

- [ ] **Step 4: Inspect new schemas**

Run:

```bash
psql "$CORPSCOUT_TEST_DATABASE_URL" -c "select schema_name from information_schema.schemata where schema_name in ('registry','source_records','entities','identity','web') order by schema_name;"
```

Expected output includes:

```text
entities
identity
registry
source_records
web
```

- [ ] **Step 5: Commit generated code and any final fixes**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout
git commit -m "chore: verify clean corpscout schema replacement"
```

---

## Self-Review

Spec coverage:

- Source execution registry is covered by Task 2 and Task 3.
- Shared source records, child tables, `_en` columns, and aggregate translation state are covered by Task 2.
- Legal entities, identity companies, brands, and web links are covered by Task 2.
- Destructive POC cleanup is covered by Task 2.
- sqlc/query repair is covered by Task 3.
- Scheduler/API compile repair is covered by Task 4.
- Source executable activity skeleton is covered by Task 5.
- Verification is covered by Task 6.

No placeholder scan:

- This plan intentionally avoids `TBD`, `TODO`, and open-ended “handle edge cases” wording.
- The migration DDL contract is bounded to exact sections of `company-identity-clean-replacement-schema.md`; the engineer must copy those already-approved table definitions into the migration and keep the doc synchronized if implementation changes are required.

Type consistency:

- `registry.sources.slug` is the external source key.
- `source_records.companies.id` is the source-company FK target for child tables.
- `entities.legal_entities.id` is the legal entity FK target.
- `identity.companies.id` and `identity.brands.id` are central identity FK targets.

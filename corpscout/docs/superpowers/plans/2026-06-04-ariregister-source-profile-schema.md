# Ariregister Source Profile Schema Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an `ariregister_source` schema that parses current `ariregister_workflow.raw_records` general-data JSON into source-profile tables and exposes list/detail views in the same operational style as `brreg_source`.

**Architecture:** Keep `ariregister_workflow` as the raw ingestion/provenance layer and add `ariregister_source` as the parsed source-profile layer. Normalize current raw records through a Temporal workflow that reads `ariregister_workflow.raw_records`, builds source-profile rows in Go, and merges them into PostgreSQL with COPY/staging tables. Use `_en` columns for every Estonian text field that can reasonably be translated, and expose `v_missing_translations`, `mv_company_explorer`, and `v_company_detail` projections for UI/API use.

**Tech Stack:** PostgreSQL migrations, sqlc, Go 1.26, pgx/v5 COPY, Temporal workflows/activities, React/TypeScript source-detail UI, `log/slog`, `github.com/cockroachdb/errors`.

---

## Scope

This plan covers the first Ariregister source-profile slice based on the official general-data JSON bulk file (`ettevotja_rekvisiidid__yldandmed.json.zip`).

Included source arrays:

- `aadressid`
- `sidevahendid`
- `teatatud_tegevusalad`
- `info_majandusaasta_aruannetest`
- `kapitalid`
- `staatused`
- `arinimed`
- `oiguslikud_vormid`
- `majandusaastad`
- `pohikirjad`
- `markused_kaardil`

Not included in the first slice:

- Shareholders, beneficial owners, management roles, and filings not present in the current general-data JSON sample. Add those later when using the authenticated detailed-company API or another official dataset.
- Domain discovery and currency conversion workflows. The schema has compatible tables/columns where useful, but this plan only populates values present in raw Ariregister data.

## File Structure

Create:

- `database/migrations/000093_ariregister_source_profile_tables.up.sql` - creates `ariregister_source`, source-profile tables, views, and indexes.
- `database/migrations/000093_ariregister_source_profile_tables.down.sql` - drops `ariregister_source`.
- `database/queries/ariregister_source_profile.sql` - sqlc read queries for explorer/detail counts and refresh.
- `scheduler/internal/db/ariregister_source_profile_migration_test.go` - migration contract tests.
- `scheduler/internal/ariregister/companydata/sourceprofile/source_profile.go` - parser and row-building logic from raw JSON into source-profile batch rows.
- `scheduler/internal/ariregister/companydata/sourceprofile/source_profile_test.go` - parser behavior tests.
- `scheduler/internal/ariregister/db/source_profile.go` - source-profile command/result types and filters.
- `scheduler/internal/ariregister/db/source_profile_copy.go` - raw-record selection, COPY staging, and merge.
- `scheduler/internal/ariregister/db/source_profile_copy_sql.go` - stage-table and merge SQL constants.
- `scheduler/internal/ariregister/db/source_profile_test.go` - gateway tests.
- `scheduler/internal/ariregister/actions/source_profile_actions.go` - Temporal activity boundary.
- `scheduler/internal/ariregister/workflow/source_profile.go` - Temporal workflow and refresh workflow.
- `scheduler/internal/ariregister/workflow/source_profile_test.go` - Temporal workflow tests.
- `scheduler/internal/app/ariregister_source_profile_temporal.go` - direct worker registration.
- `ui/app/components/app/AriregisterSourceProfileActionForm.tsx` - action form for profile normalization.

Modify:

- `database/sqlc.yaml` only if generated model renames are needed to avoid collisions.
- `scheduler/internal/app/temporal.go` - construct Ariregister source-profile actions and add workers.
- `scheduler/internal/httpapi/handlers.go` - add workflow trigger route.
- `scheduler/internal/httpapi/workflow_triggers.go` - add Ariregister source-profile trigger handler and request decoder.
- `scheduler/internal/httpapi/workflow_triggers_test.go` - trigger tests.
- `scheduler/internal/httpapi/source_read.go` or source metadata mapping - expose Ariregister source-entry capability if source detail UI uses capability flags.
- `ui/app/lib/api.ts` - add source-profile trigger client method.
- `ui/app/components/app/AriregisterRawInputActionSheet.tsx` - add "Build source profile" action.
- `ui/app/components/app/source-detail/sourceDetailUtils.ts` - enable Ariregister source entries once the API/view path exists.
- `ui/app/routes/sources_.$name.source_entries.tsx` - if currently BRREG-specific, generalize source entry rendering or route Ariregister through equivalent API.

## Schema Shape

The first migration creates these table families:

- `ariregister_source.companies`
- `ariregister_source.company_names`
- `ariregister_source.company_statuses`
- `ariregister_source.legal_forms`
- `ariregister_source.addresses`
- `ariregister_source.contacts`
- `ariregister_source.websites`
- `ariregister_source.domains`
- `ariregister_source.industries`
- `ariregister_source.capital`
- `ariregister_source.financial_year_periods`
- `ariregister_source.annual_reports`
- `ariregister_source.articles`
- `ariregister_source.registry_notes`
- `ariregister_source.action_tasks`

Translation columns:

- Company-level: `legal_name_en`, `registration_status_label_en`, `legal_form_label_en`, `legal_form_subtype_label_en`, `region_label_en`, `region_label_long_en`, `active_label_en`.
- Status/legal-form history: `status_label_en`, `legal_form_label_en`, `legal_form_subtype_label_en`.
- Address: `country_label_en`, `ehak_name_en`, `street_text_en`, `normalized_full_address_en`.
- Contact: `contact_type_label_en`.
- Industry: `emtak_label_en`, `emtak_version_label_en`.
- Capital: `capital_currency_label_en`.
- Annual report: `report_address_en`, `activity_label_en`, `activity_version_label_en`.
- Articles/notes: `explanation_en`, `note_type_label_en`, `note_text_en`.

## Task 1: Migration Contract Tests

**Files:**
- Create: `scheduler/internal/db/ariregister_source_profile_migration_test.go`

- [ ] **Step 1: Write the failing migration contract test**

Create `scheduler/internal/db/ariregister_source_profile_migration_test.go`:

```go
package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAriregisterSourceProfileMigrationDefinesSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000093_ariregister_source_profile_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS ariregister_source")
	for _, table := range []string{
		"companies",
		"company_names",
		"company_statuses",
		"legal_forms",
		"addresses",
		"contacts",
		"websites",
		"domains",
		"industries",
		"capital",
		"financial_year_periods",
		"annual_reports",
		"articles",
		"registry_notes",
		"action_tasks",
	} {
		require.Contains(t, sql, "CREATE TABLE ariregister_source."+table, table)
	}
	require.Contains(t, sql, "REFERENCES ariregister_workflow.raw_records")
	require.Contains(t, sql, "CREATE MATERIALIZED VIEW ariregister_source.mv_company_explorer")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW ariregister_source.v_company_detail")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW ariregister_source.v_missing_translations")
}

func TestAriregisterSourceProfileMigrationDefinesTranslationColumns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000093_ariregister_source_profile_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	for _, column := range []string{
		"legal_name_en",
		"registration_status_label_en",
		"legal_form_label_en",
		"legal_form_subtype_label_en",
		"region_label_en",
		"region_label_long_en",
		"active_label_en",
		"status_label_en",
		"country_label_en",
		"ehak_name_en",
		"street_text_en",
		"normalized_full_address_en",
		"contact_type_label_en",
		"emtak_label_en",
		"emtak_version_label_en",
		"capital_currency_label_en",
		"report_address_en",
		"activity_label_en",
		"activity_version_label_en",
		"explanation_en",
		"note_type_label_en",
		"note_text_en",
	} {
		require.True(t, strings.Contains(sql, column+" TEXT") || strings.Contains(sql, column+" TEXT,"),
			"missing translatable column %s", column)
	}
}

func TestAriregisterSourceProfileDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000093_ariregister_source_profile_tables.down.sql")
	require.NoError(t, err)

	require.Contains(t, string(body), "DROP SCHEMA IF EXISTS ariregister_source CASCADE")
}
```

- [ ] **Step 2: Run the migration test and verify it fails**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db -run 'TestAriregisterSourceProfile' -count=1
```

Expected: fails because migration `000093_ariregister_source_profile_tables.*.sql` does not exist.

- [ ] **Step 3: Commit the failing test**

```bash
git add scheduler/internal/db/ariregister_source_profile_migration_test.go
git commit -m "test(ariregister): define source profile migration contract"
```

## Task 2: Source Schema Migration

**Files:**
- Create: `database/migrations/000093_ariregister_source_profile_tables.up.sql`
- Create: `database/migrations/000093_ariregister_source_profile_tables.down.sql`

- [ ] **Step 1: Create the up migration**

Create `database/migrations/000093_ariregister_source_profile_tables.up.sql` with this structure. Keep the table and column names exactly as shown so parser, sqlc queries, and UI projections can depend on them.

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS ariregister_source;

CREATE TABLE ariregister_source.companies (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  registry_code TEXT NOT NULL,
  source_native_id TEXT NOT NULL,
  country_iso2 TEXT NOT NULL DEFAULT 'EE',
  legal_name TEXT NOT NULL,
  legal_name_normalized TEXT NOT NULL,
  legal_name_en TEXT,
  registration_status TEXT,
  registration_status_label TEXT,
  registration_status_label_en TEXT,
  lifecycle_status TEXT NOT NULL DEFAULT 'unknown',
  legal_form_code TEXT,
  legal_form_number INTEGER,
  legal_form_label TEXT,
  legal_form_label_en TEXT,
  legal_form_subtype TEXT,
  legal_form_subtype_label TEXT,
  legal_form_subtype_label_en TEXT,
  region_code INTEGER,
  region_label TEXT,
  region_label_en TEXT,
  region_label_long TEXT,
  region_label_long_en TEXT,
  active_label TEXT,
  active_label_en TEXT,
  first_registered_on DATE,
  deleted_on DATE,
  evks_registered_at DATE,
  has_missing_beneficial_owner_discrepancy_notice BOOLEAN,
  founded_without_contribution BOOLEAN,
  waived_form_requirements BOOLEAN,
  is_accounting_required BOOLEAN,
  reports_beneficial_owners BOOLEAN,
  is_active BOOLEAN,
  last_annual_report_year INTEGER,
  employee_count INTEGER,
  employee_count_source TEXT,
  employee_band TEXT,
  source_updated_at TIMESTAMPTZ,
  payload_hash TEXT NOT NULL,
  profile_version TEXT NOT NULL DEFAULT 'ariregister.source_profile.v1',
  row_status TEXT NOT NULL DEFAULT 'active',
  normalized_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_company_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  superseded_at TIMESTAMPTZ,
  CONSTRAINT chk_ariregister_source_companies_status CHECK (row_status IN ('active', 'superseded')),
  CONSTRAINT chk_ariregister_source_companies_lifecycle CHECK (lifecycle_status IN ('active', 'inactive', 'deleted', 'unknown')),
  CONSTRAINT chk_ariregister_source_companies_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
  CONSTRAINT chk_ariregister_source_companies_payload_object CHECK (jsonb_typeof(normalized_payload) = 'object'),
  CONSTRAINT chk_ariregister_source_companies_raw_object CHECK (jsonb_typeof(raw_company_payload) = 'object'),
  CONSTRAINT chk_ariregister_source_companies_evidence_object CHECK (jsonb_typeof(evidence) = 'object'),
  CONSTRAINT chk_ariregister_source_companies_metadata_object CHECK (jsonb_typeof(metadata) = 'object')
);

CREATE UNIQUE INDEX uq_ariregister_source_companies_active_registry
  ON ariregister_source.companies(registry_code)
  WHERE row_status = 'active';
CREATE INDEX idx_ariregister_source_companies_raw_record ON ariregister_source.companies(raw_record_id);
CREATE INDEX idx_ariregister_source_companies_name ON ariregister_source.companies(legal_name_normalized);
CREATE INDEX idx_ariregister_source_companies_status ON ariregister_source.companies(row_status, lifecycle_status, updated_at DESC);

CREATE TABLE ariregister_source.company_names (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  name TEXT NOT NULL,
  name_en TEXT,
  started_on DATE,
  ended_on DATE,
  raw_name_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_company_names_raw_object CHECK (jsonb_typeof(raw_name_payload) = 'object'),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.company_statuses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  status_code TEXT NOT NULL,
  status_label TEXT,
  status_label_en TEXT,
  started_on DATE,
  raw_status_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_company_statuses_raw_object CHECK (jsonb_typeof(raw_status_payload) = 'object'),
  UNIQUE (company_id, card_region, card_number, card_type, entry_number, status_code)
);

CREATE TABLE ariregister_source.legal_forms (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  legal_form_code TEXT,
  legal_form_number INTEGER,
  legal_form_label TEXT,
  legal_form_label_en TEXT,
  legal_form_subtype TEXT,
  legal_form_subtype_label TEXT,
  legal_form_subtype_label_en TEXT,
  started_on DATE,
  ended_on DATE,
  raw_legal_form_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_legal_forms_raw_object CHECK (jsonb_typeof(raw_legal_form_payload) = 'object'),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.addresses (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  address_type TEXT NOT NULL DEFAULT 'registered',
  country_code TEXT,
  country_label TEXT,
  country_label_en TEXT,
  ehak_code TEXT,
  ehak_name TEXT,
  ehak_name_en TEXT,
  street_text TEXT,
  street_text_en TEXT,
  postal_code TEXT,
  ads_oid TEXT,
  adr_id BIGINT,
  normalized_full_address TEXT,
  normalized_full_address_en TEXT,
  normalized_full_address_detail TEXT,
  code_address TEXT,
  adob_id TEXT,
  ads_type TEXT,
  started_on DATE,
  ended_on DATE,
  raw_address_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_addresses_raw_object CHECK (jsonb_typeof(raw_address_payload) = 'object'),
  CONSTRAINT chk_ariregister_source_addresses_type CHECK (address_type IN ('registered', 'report', 'other')),
  UNIQUE (company_id, source_entry_id)
);

CREATE INDEX idx_ariregister_source_addresses_company ON ariregister_source.addresses(company_id);
CREATE INDEX idx_ariregister_source_addresses_postal ON ariregister_source.addresses(postal_code) WHERE postal_code IS NOT NULL;
CREATE INDEX idx_ariregister_source_addresses_ehak ON ariregister_source.addresses(ehak_code) WHERE ehak_code IS NOT NULL;

CREATE TABLE ariregister_source.contacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES ariregister_workflow.raw_records(id) ON DELETE SET NULL,
  source_entry_id BIGINT,
  contact_type TEXT NOT NULL,
  contact_type_label TEXT,
  contact_type_label_en TEXT,
  value TEXT NOT NULL,
  normalized_value TEXT,
  source TEXT NOT NULL DEFAULT 'ariregister',
  status TEXT NOT NULL DEFAULT 'active',
  is_primary BOOLEAN NOT NULL DEFAULT false,
  ended_on DATE,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_contact_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_contacts_status CHECK (status IN ('active', 'removed', 'superseded')),
  CONSTRAINT chk_ariregister_source_contacts_raw_object CHECK (jsonb_typeof(raw_contact_payload) = 'object'),
  UNIQUE (company_id, contact_type, normalized_value)
);

CREATE INDEX idx_ariregister_source_contacts_company ON ariregister_source.contacts(company_id, contact_type, status);

CREATE TABLE ariregister_source.websites (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES ariregister_workflow.raw_records(id) ON DELETE SET NULL,
  contact_id UUID REFERENCES ariregister_source.contacts(id) ON DELETE SET NULL,
  url TEXT NOT NULL,
  normalized_url TEXT NOT NULL,
  host TEXT,
  path TEXT,
  website_type TEXT NOT NULL DEFAULT 'official_site',
  source TEXT NOT NULL DEFAULT 'ariregister_contact',
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT NOT NULL DEFAULT 90,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  title TEXT,
  title_en TEXT,
  description TEXT,
  description_en TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_websites_confidence CHECK (confidence BETWEEN 1 AND 100),
  UNIQUE (company_id, normalized_url)
);

CREATE INDEX idx_ariregister_source_websites_host ON ariregister_source.websites(host) WHERE host IS NOT NULL;

CREATE TABLE ariregister_source.domains (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID REFERENCES ariregister_workflow.raw_records(id) ON DELETE SET NULL,
  website_id UUID REFERENCES ariregister_source.websites(id) ON DELETE SET NULL,
  domain TEXT NOT NULL,
  normalized_domain TEXT NOT NULL,
  registrable_domain TEXT NOT NULL,
  domain_type TEXT NOT NULL DEFAULT 'official',
  source TEXT NOT NULL DEFAULT 'ariregister_website',
  status TEXT NOT NULL DEFAULT 'active',
  confidence SMALLINT NOT NULL DEFAULT 90,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  best_signal TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_domains_confidence CHECK (confidence BETWEEN 1 AND 100),
  UNIQUE (company_id, normalized_domain)
);

CREATE INDEX idx_ariregister_source_domains_domain ON ariregister_source.domains(normalized_domain);

CREATE TABLE ariregister_source.industries (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  nace_code_id UUID REFERENCES nace_codes(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  classification_type TEXT NOT NULL DEFAULT 'declared_activity',
  source_field TEXT NOT NULL DEFAULT 'teatatud_tegevusalad',
  position SMALLINT NOT NULL DEFAULT 1,
  emtak_code TEXT NOT NULL,
  emtak_label TEXT,
  emtak_label_en TEXT,
  emtak_version INTEGER,
  emtak_version_label TEXT,
  emtak_version_label_en TEXT,
  nace_code TEXT,
  nace_revision TEXT,
  nace_title TEXT,
  nace_title_en TEXT,
  mapping_method TEXT,
  mapping_confidence REAL,
  is_primary BOOLEAN NOT NULL DEFAULT false,
  started_on DATE,
  ended_on DATE,
  raw_industry_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_industries_position CHECK (position BETWEEN 1 AND 50),
  CONSTRAINT chk_ariregister_source_industries_confidence CHECK (mapping_confidence IS NULL OR mapping_confidence BETWEEN 0 AND 1),
  CONSTRAINT chk_ariregister_source_industries_raw_object CHECK (jsonb_typeof(raw_industry_payload) = 'object'),
  UNIQUE (company_id, classification_type, position)
);

CREATE INDEX idx_ariregister_source_industries_company ON ariregister_source.industries(company_id);
CREATE INDEX idx_ariregister_source_industries_emtak ON ariregister_source.industries(emtak_code);
CREATE INDEX idx_ariregister_source_industries_nace ON ariregister_source.industries(nace_code_id) WHERE nace_code_id IS NOT NULL;

CREATE TABLE ariregister_source.capital (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  capital_amount NUMERIC(20, 2),
  capital_currency TEXT,
  capital_currency_label TEXT,
  capital_currency_label_en TEXT,
  introduced_on DATE,
  ended_on DATE,
  raw_capital_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_capital_raw_object CHECK (jsonb_typeof(raw_capital_payload) = 'object'),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.financial_year_periods (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  period_start_month_day TEXT,
  period_end_month_day TEXT,
  started_on DATE,
  ended_on DATE,
  raw_period_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.annual_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  fiscal_year INTEGER,
  period_start DATE,
  period_end DATE,
  employee_count INTEGER,
  report_address TEXT,
  report_address_en TEXT,
  activity_emtak_code TEXT,
  activity_label TEXT,
  activity_label_en TEXT,
  activity_version TEXT,
  activity_version_label TEXT,
  activity_version_label_en TEXT,
  activity_nace_code TEXT,
  raw_report_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_reports_year CHECK (fiscal_year IS NULL OR fiscal_year BETWEEN 1800 AND 2200),
  CONSTRAINT chk_ariregister_source_reports_employee_count CHECK (employee_count IS NULL OR employee_count >= 0),
  UNIQUE (company_id, source_entry_id)
);

CREATE INDEX idx_ariregister_source_reports_company_year ON ariregister_source.annual_reports(company_id, fiscal_year DESC);

CREATE TABLE ariregister_source.articles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  confirmed_on DATE,
  changed_on DATE,
  explanation TEXT,
  explanation_en TEXT,
  contains_special_rights BOOLEAN,
  started_on DATE,
  ended_on DATE,
  raw_articles_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.registry_notes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  raw_record_id UUID NOT NULL REFERENCES ariregister_workflow.raw_records(id) ON DELETE RESTRICT,
  source_entry_id BIGINT,
  card_region INTEGER,
  card_number INTEGER,
  card_type TEXT,
  entry_number INTEGER,
  column_number INTEGER,
  note_type TEXT,
  note_type_label TEXT,
  note_type_label_en TEXT,
  note_text TEXT,
  note_text_en TEXT,
  started_on DATE,
  ended_on DATE,
  raw_note_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (company_id, source_entry_id)
);

CREATE TABLE ariregister_source.action_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id UUID NOT NULL REFERENCES ariregister_source.companies(id) ON DELETE CASCADE,
  action_type TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_row_id UUID NOT NULL,
  target_key TEXT NOT NULL DEFAULT '',
  source_fingerprint TEXT NOT NULL,
  source_column TEXT,
  target_column TEXT,
  source_text TEXT,
  source_lang TEXT NOT NULL DEFAULT 'et',
  target_lang TEXT NOT NULL DEFAULT 'en',
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts INTEGER NOT NULL DEFAULT 3,
  lease_until TIMESTAMPTZ,
  last_started_at TIMESTAMPTZ,
  last_finished_at TIMESTAMPTZ,
  result JSONB NOT NULL DEFAULT '{}'::jsonb,
  model TEXT,
  prompt_version TEXT,
  error TEXT,
  error_category TEXT,
  error_code TEXT,
  retry_strategy TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_ariregister_source_action_type CHECK (action_type IN ('translate_field', 'discover_domains', 'build_suggestion')),
  CONSTRAINT chk_ariregister_source_action_status CHECK (status IN ('pending', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')),
  CONSTRAINT chk_ariregister_source_action_attempt CHECK (attempt_count >= 0 AND max_attempts > 0),
  CONSTRAINT chk_ariregister_source_action_result_object CHECK (jsonb_typeof(result) = 'object'),
  CONSTRAINT chk_ariregister_source_action_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)
);

CREATE INDEX idx_ariregister_source_action_queue
  ON ariregister_source.action_tasks(action_type, status, lease_until, updated_at)
  WHERE status IN ('pending', 'running', 'failed_retryable');
CREATE INDEX idx_ariregister_source_action_company
  ON ariregister_source.action_tasks(company_id, action_type, status);

CREATE OR REPLACE VIEW ariregister_source.v_missing_translations AS
SELECT company.id AS company_id, 'ariregister_source.companies'::text AS source_table, company.id AS source_row_id, 'legal_name'::text AS source_column, 'legal_name_en'::text AS target_column, company.legal_name AS source_text, encode(digest(company.legal_name, 'sha256'), 'hex') AS source_text_hash, 90::integer AS priority
FROM ariregister_source.companies company
WHERE company.row_status = 'active' AND nullif(btrim(company.legal_name), '') IS NOT NULL AND nullif(btrim(company.legal_name_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'registration_status_label', 'registration_status_label_en', company.registration_status_label, encode(digest(company.registration_status_label, 'sha256'), 'hex'), 20 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.registration_status_label), '') IS NOT NULL AND nullif(btrim(company.registration_status_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'legal_form_label', 'legal_form_label_en', company.legal_form_label, encode(digest(company.legal_form_label, 'sha256'), 'hex'), 20 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.legal_form_label), '') IS NOT NULL AND nullif(btrim(company.legal_form_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'legal_form_subtype_label', 'legal_form_subtype_label_en', company.legal_form_subtype_label, encode(digest(company.legal_form_subtype_label, 'sha256'), 'hex'), 30 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.legal_form_subtype_label), '') IS NOT NULL AND nullif(btrim(company.legal_form_subtype_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'region_label', 'region_label_en', company.region_label, encode(digest(company.region_label, 'sha256'), 'hex'), 40 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.region_label), '') IS NOT NULL AND nullif(btrim(company.region_label_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'region_label_long', 'region_label_long_en', company.region_label_long, encode(digest(company.region_label_long, 'sha256'), 'hex'), 40 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.region_label_long), '') IS NOT NULL AND nullif(btrim(company.region_label_long_en), '') IS NULL
UNION ALL SELECT company.id, 'ariregister_source.companies', company.id, 'active_label', 'active_label_en', company.active_label, encode(digest(company.active_label, 'sha256'), 'hex'), 30 FROM ariregister_source.companies company WHERE company.row_status = 'active' AND nullif(btrim(company.active_label), '') IS NOT NULL AND nullif(btrim(company.active_label_en), '') IS NULL
UNION ALL SELECT status.company_id, 'ariregister_source.company_statuses', status.id, 'status_label', 'status_label_en', status.status_label, encode(digest(status.status_label, 'sha256'), 'hex'), 30 FROM ariregister_source.company_statuses status WHERE nullif(btrim(status.status_label), '') IS NOT NULL AND nullif(btrim(status.status_label_en), '') IS NULL
UNION ALL SELECT form.company_id, 'ariregister_source.legal_forms', form.id, 'legal_form_label', 'legal_form_label_en', form.legal_form_label, encode(digest(form.legal_form_label, 'sha256'), 'hex'), 30 FROM ariregister_source.legal_forms form WHERE nullif(btrim(form.legal_form_label), '') IS NOT NULL AND nullif(btrim(form.legal_form_label_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'country_label', 'country_label_en', address.country_label, encode(digest(address.country_label, 'sha256'), 'hex'), 50 FROM ariregister_source.addresses address WHERE nullif(btrim(address.country_label), '') IS NOT NULL AND nullif(btrim(address.country_label_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'ehak_name', 'ehak_name_en', address.ehak_name, encode(digest(address.ehak_name, 'sha256'), 'hex'), 50 FROM ariregister_source.addresses address WHERE nullif(btrim(address.ehak_name), '') IS NOT NULL AND nullif(btrim(address.ehak_name_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'street_text', 'street_text_en', address.street_text, encode(digest(address.street_text, 'sha256'), 'hex'), 80 FROM ariregister_source.addresses address WHERE nullif(btrim(address.street_text), '') IS NOT NULL AND nullif(btrim(address.street_text_en), '') IS NULL
UNION ALL SELECT address.company_id, 'ariregister_source.addresses', address.id, 'normalized_full_address', 'normalized_full_address_en', address.normalized_full_address, encode(digest(address.normalized_full_address, 'sha256'), 'hex'), 80 FROM ariregister_source.addresses address WHERE nullif(btrim(address.normalized_full_address), '') IS NOT NULL AND nullif(btrim(address.normalized_full_address_en), '') IS NULL
UNION ALL SELECT contact.company_id, 'ariregister_source.contacts', contact.id, 'contact_type_label', 'contact_type_label_en', contact.contact_type_label, encode(digest(contact.contact_type_label, 'sha256'), 'hex'), 50 FROM ariregister_source.contacts contact WHERE nullif(btrim(contact.contact_type_label), '') IS NOT NULL AND nullif(btrim(contact.contact_type_label_en), '') IS NULL
UNION ALL SELECT industry.company_id, 'ariregister_source.industries', industry.id, 'emtak_label', 'emtak_label_en', industry.emtak_label, encode(digest(industry.emtak_label, 'sha256'), 'hex'), 20 FROM ariregister_source.industries industry WHERE nullif(btrim(industry.emtak_label), '') IS NOT NULL AND nullif(btrim(industry.emtak_label_en), '') IS NULL
UNION ALL SELECT industry.company_id, 'ariregister_source.industries', industry.id, 'emtak_version_label', 'emtak_version_label_en', industry.emtak_version_label, encode(digest(industry.emtak_version_label, 'sha256'), 'hex'), 40 FROM ariregister_source.industries industry WHERE nullif(btrim(industry.emtak_version_label), '') IS NOT NULL AND nullif(btrim(industry.emtak_version_label_en), '') IS NULL
UNION ALL SELECT capital.company_id, 'ariregister_source.capital', capital.id, 'capital_currency_label', 'capital_currency_label_en', capital.capital_currency_label, encode(digest(capital.capital_currency_label, 'sha256'), 'hex'), 50 FROM ariregister_source.capital capital WHERE nullif(btrim(capital.capital_currency_label), '') IS NOT NULL AND nullif(btrim(capital.capital_currency_label_en), '') IS NULL
UNION ALL SELECT report.company_id, 'ariregister_source.annual_reports', report.id, 'report_address', 'report_address_en', report.report_address, encode(digest(report.report_address, 'sha256'), 'hex'), 80 FROM ariregister_source.annual_reports report WHERE nullif(btrim(report.report_address), '') IS NOT NULL AND nullif(btrim(report.report_address_en), '') IS NULL
UNION ALL SELECT report.company_id, 'ariregister_source.annual_reports', report.id, 'activity_label', 'activity_label_en', report.activity_label, encode(digest(report.activity_label, 'sha256'), 'hex'), 20 FROM ariregister_source.annual_reports report WHERE nullif(btrim(report.activity_label), '') IS NOT NULL AND nullif(btrim(report.activity_label_en), '') IS NULL
UNION ALL SELECT report.company_id, 'ariregister_source.annual_reports', report.id, 'activity_version_label', 'activity_version_label_en', report.activity_version_label, encode(digest(report.activity_version_label, 'sha256'), 'hex'), 40 FROM ariregister_source.annual_reports report WHERE nullif(btrim(report.activity_version_label), '') IS NOT NULL AND nullif(btrim(report.activity_version_label_en), '') IS NULL
UNION ALL SELECT article.company_id, 'ariregister_source.articles', article.id, 'explanation', 'explanation_en', article.explanation, encode(digest(article.explanation, 'sha256'), 'hex'), 70 FROM ariregister_source.articles article WHERE nullif(btrim(article.explanation), '') IS NOT NULL AND nullif(btrim(article.explanation_en), '') IS NULL
UNION ALL SELECT note.company_id, 'ariregister_source.registry_notes', note.id, 'note_type_label', 'note_type_label_en', note.note_type_label, encode(digest(note.note_type_label, 'sha256'), 'hex'), 60 FROM ariregister_source.registry_notes note WHERE nullif(btrim(note.note_type_label), '') IS NOT NULL AND nullif(btrim(note.note_type_label_en), '') IS NULL
UNION ALL SELECT note.company_id, 'ariregister_source.registry_notes', note.id, 'note_text', 'note_text_en', note.note_text, encode(digest(note.note_text, 'sha256'), 'hex'), 60 FROM ariregister_source.registry_notes note WHERE nullif(btrim(note.note_text), '') IS NOT NULL AND nullif(btrim(note.note_text_en), '') IS NULL;

CREATE MATERIALIZED VIEW ariregister_source.mv_company_explorer AS
WITH primary_address AS (
  SELECT DISTINCT ON (company_id) company_id, ehak_name AS city_or_area, postal_code, normalized_full_address
  FROM ariregister_source.addresses
  ORDER BY company_id, started_on DESC NULLS LAST, created_at DESC
),
primary_industry AS (
  SELECT DISTINCT ON (company_id) company_id, emtak_code AS primary_industry_code, coalesce(emtak_label_en, emtak_label) AS primary_industry_label, nace_code AS primary_nace_code, coalesce(nace_title_en, nace_title) AS primary_nace_title
  FROM ariregister_source.industries
  ORDER BY company_id, is_primary DESC, position ASC
),
latest_report AS (
  SELECT DISTINCT ON (company_id) company_id, fiscal_year AS latest_financial_year, employee_count
  FROM ariregister_source.annual_reports
  ORDER BY company_id, fiscal_year DESC NULLS LAST, period_end DESC NULLS LAST
),
website_counts AS (
  SELECT company_id, count(*)::bigint AS website_count FROM ariregister_source.websites WHERE status = 'active' GROUP BY company_id
),
domain_counts AS (
  SELECT company_id, count(*)::bigint AS domain_count FROM ariregister_source.domains WHERE status = 'active' GROUP BY company_id
),
contact_counts AS (
  SELECT company_id, count(*)::bigint AS contact_count FROM ariregister_source.contacts WHERE status = 'active' GROUP BY company_id
),
translation_counts AS (
  SELECT company_id, count(*)::bigint AS translation_missing_count FROM ariregister_source.v_missing_translations GROUP BY company_id
)
SELECT
  company.id AS company_id,
  company.registry_code,
  company.legal_name,
  coalesce(company.legal_form_label_en, company.legal_form_label) AS legal_form_label,
  company.lifecycle_status,
  company.registration_status,
  coalesce(company.registration_status_label_en, company.registration_status_label) AS registration_status_label,
  industry.primary_industry_code,
  industry.primary_industry_label,
  industry.primary_nace_code,
  industry.primary_nace_title,
  address.city_or_area,
  address.postal_code,
  address.normalized_full_address,
  coalesce(report.employee_count, company.employee_count) AS employee_count,
  report.latest_financial_year,
  coalesce(website_counts.website_count, 0) AS website_count,
  coalesce(domain_counts.domain_count, 0) AS domain_count,
  coalesce(contact_counts.contact_count, 0) AS contact_count,
  coalesce(translation_counts.translation_missing_count, 0) AS translation_missing_count,
  company.updated_at
FROM ariregister_source.companies company
LEFT JOIN primary_address address ON address.company_id = company.id
LEFT JOIN primary_industry industry ON industry.company_id = company.id
LEFT JOIN latest_report report ON report.company_id = company.id
LEFT JOIN website_counts ON website_counts.company_id = company.id
LEFT JOIN domain_counts ON domain_counts.company_id = company.id
LEFT JOIN contact_counts ON contact_counts.company_id = company.id
LEFT JOIN translation_counts ON translation_counts.company_id = company.id
WHERE company.row_status = 'active';

CREATE UNIQUE INDEX uq_ariregister_source_mv_company_explorer_company ON ariregister_source.mv_company_explorer(company_id);
CREATE INDEX idx_ariregister_source_mv_company_explorer_registry ON ariregister_source.mv_company_explorer(registry_code);
CREATE INDEX idx_ariregister_source_mv_company_explorer_name ON ariregister_source.mv_company_explorer(legal_name);
CREATE INDEX idx_ariregister_source_mv_company_explorer_translation ON ariregister_source.mv_company_explorer(translation_missing_count) WHERE translation_missing_count > 0;

CREATE OR REPLACE VIEW ariregister_source.v_company_detail AS
WITH company_names AS (
  SELECT company_id, jsonb_agg(to_jsonb(company_names) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS names FROM ariregister_source.company_names GROUP BY company_id
),
statuses AS (
  SELECT company_id, jsonb_agg(to_jsonb(company_statuses) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS statuses FROM ariregister_source.company_statuses GROUP BY company_id
),
legal_forms AS (
  SELECT company_id, jsonb_agg(to_jsonb(legal_forms) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS legal_forms FROM ariregister_source.legal_forms GROUP BY company_id
),
addresses AS (
  SELECT company_id, jsonb_agg(to_jsonb(addresses) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS addresses FROM ariregister_source.addresses GROUP BY company_id
),
contacts AS (
  SELECT company_id, jsonb_agg(to_jsonb(contacts) - 'company_id' ORDER BY is_primary DESC, contact_type, created_at DESC) AS contacts FROM ariregister_source.contacts GROUP BY company_id
),
websites AS (
  SELECT company_id, jsonb_agg(to_jsonb(websites) - 'company_id' ORDER BY is_primary DESC, confidence DESC, created_at DESC) AS websites FROM ariregister_source.websites GROUP BY company_id
),
domains AS (
  SELECT company_id, jsonb_agg(to_jsonb(domains) - 'company_id' ORDER BY is_primary DESC, confidence DESC, created_at DESC) AS domains FROM ariregister_source.domains GROUP BY company_id
),
industries AS (
  SELECT company_id, jsonb_agg(to_jsonb(industries) - 'company_id' ORDER BY is_primary DESC, position) AS industries FROM ariregister_source.industries GROUP BY company_id
),
capital AS (
  SELECT company_id, jsonb_agg(to_jsonb(capital) - 'company_id' ORDER BY introduced_on DESC NULLS LAST) AS capital FROM ariregister_source.capital GROUP BY company_id
),
annual_reports AS (
  SELECT company_id, jsonb_agg(to_jsonb(annual_reports) - 'company_id' ORDER BY fiscal_year DESC NULLS LAST) AS annual_reports FROM ariregister_source.annual_reports GROUP BY company_id
),
articles AS (
  SELECT company_id, jsonb_agg(to_jsonb(articles) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS articles FROM ariregister_source.articles GROUP BY company_id
),
registry_notes AS (
  SELECT company_id, jsonb_agg(to_jsonb(registry_notes) - 'company_id' ORDER BY started_on DESC NULLS LAST) AS registry_notes FROM ariregister_source.registry_notes GROUP BY company_id
)
SELECT
  company.*,
  coalesce(company_names.names, '[]'::jsonb) AS names,
  coalesce(statuses.statuses, '[]'::jsonb) AS statuses,
  coalesce(legal_forms.legal_forms, '[]'::jsonb) AS legal_forms,
  coalesce(addresses.addresses, '[]'::jsonb) AS addresses,
  coalesce(contacts.contacts, '[]'::jsonb) AS contacts,
  coalesce(websites.websites, '[]'::jsonb) AS websites,
  coalesce(domains.domains, '[]'::jsonb) AS domains,
  coalesce(industries.industries, '[]'::jsonb) AS industries,
  coalesce(capital.capital, '[]'::jsonb) AS capital,
  coalesce(annual_reports.annual_reports, '[]'::jsonb) AS annual_reports,
  coalesce(articles.articles, '[]'::jsonb) AS articles,
  coalesce(registry_notes.registry_notes, '[]'::jsonb) AS registry_notes
FROM ariregister_source.companies company
LEFT JOIN company_names ON company_names.company_id = company.id
LEFT JOIN statuses ON statuses.company_id = company.id
LEFT JOIN legal_forms ON legal_forms.company_id = company.id
LEFT JOIN addresses ON addresses.company_id = company.id
LEFT JOIN contacts ON contacts.company_id = company.id
LEFT JOIN websites ON websites.company_id = company.id
LEFT JOIN domains ON domains.company_id = company.id
LEFT JOIN industries ON industries.company_id = company.id
LEFT JOIN capital ON capital.company_id = company.id
LEFT JOIN annual_reports ON annual_reports.company_id = company.id
LEFT JOIN articles ON articles.company_id = company.id
LEFT JOIN registry_notes ON registry_notes.company_id = company.id
WHERE company.row_status = 'active';
```

- [ ] **Step 2: Create the down migration**

Create `database/migrations/000093_ariregister_source_profile_tables.down.sql`:

```sql
DROP SCHEMA IF EXISTS ariregister_source CASCADE;
```

- [ ] **Step 3: Run the migration contract test**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/db -run 'TestAriregisterSourceProfile' -count=1
```

Expected: PASS.

- [ ] **Step 4: Run migrations against local database**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose run --rm migrate
```

Expected: migration `93/u ariregister_source_profile_tables` applies successfully.

- [ ] **Step 5: Commit schema migration**

```bash
git add database/migrations/000093_ariregister_source_profile_tables.up.sql \
        database/migrations/000093_ariregister_source_profile_tables.down.sql
git commit -m "feat(ariregister): add source profile schema"
```

## Task 3: sqlc Read Queries

**Files:**
- Create: `database/queries/ariregister_source_profile.sql`
- Modify: `scheduler/internal/db/gen/` after `make sqlc-generate`

- [ ] **Step 1: Add source-profile queries**

Create `database/queries/ariregister_source_profile.sql`:

```sql
-- name: GetAriregisterSourceTranslationAssetState :one
SELECT
  'mv_company_translation_status'::text AS asset,
  count(*)::bigint AS raw_records_current,
  COALESCE(sum(translation_missing_count), 0)::bigint AS task_no_state,
  0::bigint AS task_pending,
  0::bigint AS task_running_active,
  0::bigint AS task_running_stale,
  0::bigint AS task_failed_retryable,
  0::bigint AS task_failed_terminal,
  0::bigint AS task_succeeded,
  0::bigint AS task_skipped,
  COALESCE(sum(translation_missing_count), 0)::bigint AS task_eligible_now,
  0::bigint AS artifact_succeeded,
  0::bigint AS artifact_skipped,
  0::bigint AS artifact_failed,
  COALESCE(sum(translation_missing_count), 0)::bigint AS artifact_missing
FROM ariregister_source.mv_company_explorer;

-- name: GetAriregisterSourceResultTableCounts :one
SELECT
  (SELECT count(*)::bigint FROM ariregister_source.companies WHERE row_status = 'active') AS companies,
  (SELECT count(*)::bigint FROM ariregister_source.addresses) AS addresses,
  (SELECT count(*)::bigint FROM ariregister_source.industries) AS industries,
  (SELECT count(*)::bigint FROM ariregister_source.contacts) AS contacts,
  (SELECT count(*)::bigint FROM ariregister_source.annual_reports) AS annual_reports;

-- name: CountAriregisterSourceEntries :one
SELECT count(*)::bigint
FROM ariregister_source.mv_company_explorer entry
WHERE (
    sqlc.narg('query')::text IS NULL
    OR entry.legal_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.registry_code ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.city_or_area ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (sqlc.narg('lifecycle_status')::text IS NULL OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text)
  AND (sqlc.narg('registration_status')::text IS NULL OR entry.registration_status = sqlc.narg('registration_status')::text)
  AND (
    sqlc.narg('translation_status')::text IS NULL
    OR (sqlc.narg('translation_status')::text = 'missing' AND entry.translation_missing_count > 0)
    OR (sqlc.narg('translation_status')::text = 'complete' AND entry.translation_missing_count = 0)
  );

-- name: ListAriregisterSourceEntries :many
SELECT
  entry.company_id,
  entry.registry_code,
  entry.legal_name,
  entry.legal_form_label,
  entry.lifecycle_status,
  entry.registration_status,
  entry.registration_status_label,
  entry.primary_industry_code,
  entry.primary_industry_label,
  entry.primary_nace_code,
  entry.primary_nace_title,
  entry.city_or_area,
  entry.postal_code,
  entry.normalized_full_address,
  entry.employee_count,
  entry.latest_financial_year,
  entry.website_count,
  entry.domain_count,
  entry.contact_count,
  entry.translation_missing_count,
  entry.updated_at
FROM ariregister_source.mv_company_explorer entry
WHERE (
    sqlc.narg('query')::text IS NULL
    OR entry.legal_name ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.registry_code ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.primary_industry_label ILIKE '%' || sqlc.narg('query')::text || '%'
    OR entry.city_or_area ILIKE '%' || sqlc.narg('query')::text || '%'
  )
  AND (sqlc.narg('lifecycle_status')::text IS NULL OR entry.lifecycle_status = sqlc.narg('lifecycle_status')::text)
  AND (sqlc.narg('registration_status')::text IS NULL OR entry.registration_status = sqlc.narg('registration_status')::text)
  AND (
    sqlc.narg('translation_status')::text IS NULL
    OR (sqlc.narg('translation_status')::text = 'missing' AND entry.translation_missing_count > 0)
    OR (sqlc.narg('translation_status')::text = 'complete' AND entry.translation_missing_count = 0)
  )
ORDER BY
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.legal_name END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'organization' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.legal_name END DESC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'updated_at' AND sqlc.arg('sort_dir')::text = 'asc' THEN entry.updated_at END ASC NULLS LAST,
  CASE WHEN sqlc.arg('sort_by')::text = 'updated_at' AND sqlc.arg('sort_dir')::text = 'desc' THEN entry.updated_at END DESC NULLS LAST,
  entry.updated_at DESC,
  entry.registry_code ASC
LIMIT GREATEST(sqlc.arg('limit')::integer, 1)
OFFSET GREATEST(sqlc.arg('offset')::integer, 0);

-- name: GetAriregisterSourceCompanyDetail :one
SELECT detail.*
FROM ariregister_source.v_company_detail detail
WHERE detail.id = sqlc.arg('company_id')::uuid;

-- name: GetAriregisterSourceCompanyExplorerRefreshSummary :one
SELECT
  count(*)::bigint AS source_entries,
  max(updated_at)::text AS latest_source_updated_at
FROM ariregister_source.mv_company_explorer;

-- name: RefreshAriregisterSourceCompanyExplorer :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY ariregister_source.mv_company_explorer;
```

- [ ] **Step 2: Run sqlc generation**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make sqlc-generate
```

Expected: no errors; generated methods appear in `scheduler/internal/db/gen/ariregister_source_profile.sql.go` and `scheduler/internal/db/gen/querier.go`.

- [ ] **Step 3: Commit queries and generated code**

```bash
git add database/queries/ariregister_source_profile.sql scheduler/internal/db/gen/
git commit -m "feat(ariregister): add source profile sqlc projections"
```

## Task 4: Parser Package

**Files:**
- Create: `scheduler/internal/ariregister/companydata/sourceprofile/source_profile.go`
- Create: `scheduler/internal/ariregister/companydata/sourceprofile/source_profile_test.go`

- [ ] **Step 1: Write parser tests first**

Create `scheduler/internal/ariregister/companydata/sourceprofile/source_profile_test.go` with tests for:

```go
package sourceprofile

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuildBatchMapsGeneralDataPayload(t *testing.T) {
	rawID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	payload := json.RawMessage(`{
		"ariregistri_kood": 16752073,
		"nimi": "007 Agent & Partners OÜ",
		"yldandmed": {
			"esmaregistreerimise_kpv": "05.06.2023",
			"staatus": "R",
			"staatus_tekstina": "Registrisse kantud",
			"piirkond": 5,
			"piirkond_tekstina": "Tartu",
			"piirkond_tekstina_pikk": "Tartu Maakohtu registriosakond",
			"oiguslik_vorm": "OÜ",
			"oiguslik_vorm_nr": 5,
			"oiguslik_vorm_tekstina": "Osaühing",
			"tegutseb_tekstina": "Jah",
			"aadressid": [{"kirje_id": 1, "riik": "EST", "riik_tekstina": "Eesti", "ehak": "0596", "ehak_nimetus": "Pirita linnaosa, Tallinn, Harju maakond", "tanav_maja_korter": "Regati pst 12", "postiindeks": "11911", "aadress_ads__ads_normaliseeritud_taisaadress": "Harju maakond, Tallinn, Pirita linnaosa, Regati pst 12"}],
			"sidevahendid": [{"kirje_id": 2, "liik": "EMAIL", "liik_tekstina": "Elektronposti aadress", "sisu": "info@example.ee"}, {"kirje_id": 3, "liik": "WWW", "liik_tekstina": "Interneti WWW aadress", "sisu": "https://example.ee/"}],
			"teatatud_tegevusalad": [{"kirje_id": 4, "emtak_kood": "73111", "emtak_tekstina": "Reklaamiagentuuride tegevus", "emtak_versioon": 3, "emtak_versioon_tekstina": "EMTAK 2025", "nace_kood": "73.11", "on_pohitegevusala": true}],
			"kapitalid": [{"kirje_id": 5, "kapitali_suurus": "0.01", "kapitali_valuuta": "EUR", "kapitali_valuuta_tekstina": "euro"}],
			"info_majandusaasta_aruannetest": [{"kirje_id": 6, "majandusaasta_perioodi_algus_kpv": "01.01.2024", "majandusaasta_perioodi_lopp_kpv": "31.12.2024", "tootajate_arv": "0", "tegevusala_emtak_kood": "73111", "tegevusala_emtak_tekstina": "Reklaamiagentuuride tegevus"}]
		}
	}`)

	batch, err := BuildBatch(Command{Trigger: "manual", Records: []RawRecord{{
		ID: rawID, RegistryCode: "16752073", LegalName: "007 Agent & Partners OÜ", CountryISO2: "EE", RawPayload: payload, PayloadHash: "hash",
	}}})

	require.NoError(t, err)
	require.Len(t, batch.Companies, 1)
	require.Equal(t, "16752073", batch.Companies[0].RegistryCode)
	require.Equal(t, "007 Agent & Partners OÜ", batch.Companies[0].LegalName)
	require.Equal(t, "Registrisse kantud", batch.Companies[0].RegistrationStatusLabel)
	require.Equal(t, "Osaühing", batch.Companies[0].LegalFormLabel)
	require.Len(t, batch.Addresses, 1)
	require.Equal(t, "Pirita linnaosa, Tallinn, Harju maakond", batch.Addresses[0].EHAKName)
	require.Len(t, batch.Contacts, 2)
	require.Len(t, batch.Websites, 1)
	require.Len(t, batch.Domains, 1)
	require.Len(t, batch.Industries, 1)
	require.True(t, batch.Industries[0].IsPrimary)
	require.Len(t, batch.Capital, 1)
	require.Len(t, batch.AnnualReports, 1)
}
```

- [ ] **Step 2: Run parser tests and verify they fail**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/companydata/sourceprofile -count=1
```

Expected: fails because package/functions do not exist.

- [ ] **Step 3: Implement source-profile row types and `BuildBatch`**

Create `scheduler/internal/ariregister/companydata/sourceprofile/source_profile.go`.

Required public API:

```go
type Command struct {
	Trigger string
	Records []RawRecord
}

type RawRecord struct {
	ID                 uuid.UUID
	SourceNativeID     string
	RegistryCode       string
	LegalName          string
	RegistrationStatus string
	LegalForm          string
	Website            string
	Email              string
	Phone              string
	CountryISO2        string
	SourceUpdatedAt    *time.Time
	RawPayload         json.RawMessage
	PayloadHash        string
}

type Batch struct {
	Companies              []CompanyRow
	CompanyNames           []CompanyNameRow
	CompanyStatuses        []CompanyStatusRow
	LegalForms             []LegalFormRow
	Addresses              []AddressRow
	Contacts               []ContactRow
	Websites               []WebsiteRow
	Domains                []DomainRow
	Industries             []IndustryRow
	Capital                []CapitalRow
	FinancialYearPeriods   []FinancialYearPeriodRow
	AnnualReports          []AnnualReportRow
	Articles               []ArticleRow
	RegistryNotes          []RegistryNoteRow
}

func BuildBatch(command Command) (Batch, error)
```

Required mapping:

- Top-level `ariregistri_kood` and raw record `registry_code` -> `companies.registry_code`.
- Top-level `nimi` and raw record `legal_name` -> `companies.legal_name`.
- `yldandmed.staatus`, `staatus_tekstina` -> company status fields.
- `yldandmed.oiguslik_vorm`, `oiguslik_vorm_nr`, `oiguslik_vorm_tekstina` -> legal form fields.
- `yldandmed.oigusliku_vormi_alaliik`, `oigusliku_vormi_alaliik_tekstina` -> legal form subtype fields.
- `yldandmed.piirkond`, `piirkond_tekstina`, `piirkond_tekstina_pikk` -> region fields.
- `yldandmed.tegutseb_tekstina` -> `active_label`; parse `Jah` as `true`, `Ei` as `false` when possible.
- `aadressid` -> `AddressRow`.
- `sidevahendid` -> `ContactRow`, plus `WebsiteRow` and `DomainRow` for `liik = WWW`.
- `teatatud_tegevusalad` -> `IndustryRow`.
- `kapitalid` -> `CapitalRow`.
- `majandusaastad` -> `FinancialYearPeriodRow`.
- `info_majandusaasta_aruannetest` -> `AnnualReportRow`.
- `pohikirjad` -> `ArticleRow`.
- `markused_kaardil` -> `RegistryNoteRow`.

Use direct concrete helpers in this package. Do not introduce parser interfaces.

- [ ] **Step 4: Run parser tests and commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/companydata/sourceprofile -count=1
```

Expected: PASS.

Commit:

```bash
git add scheduler/internal/ariregister/companydata/sourceprofile
git commit -m "feat(ariregister): parse general data into source profile rows"
```

## Task 5: COPY Merge Gateway

**Files:**
- Create: `scheduler/internal/ariregister/db/source_profile.go`
- Create: `scheduler/internal/ariregister/db/source_profile_copy.go`
- Create: `scheduler/internal/ariregister/db/source_profile_copy_sql.go`
- Create: `scheduler/internal/ariregister/db/source_profile_test.go`

- [ ] **Step 1: Write gateway tests**

Create tests that:

- Seed one current `ariregister_workflow.raw_records` row with the sample general JSON.
- Call `Gateway.NormalizeSourceProfilesWithCopy(ctx, NormalizeSourceProfilesCommand{Limit: 10, Trigger: "test"})`.
- Assert one active company exists in `ariregister_source.companies`.
- Assert one address, two contacts, one website, one domain, one industry, one capital row, and one annual report row exist.
- Update the raw payload hash and rerun; assert the previous active company row is superseded and the new row is active.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/db -run 'TestNormalizeSourceProfiles' -count=1
```

Expected: fails because gateway methods do not exist.

- [ ] **Step 2: Implement command/result types**

Create `scheduler/internal/ariregister/db/source_profile.go`:

```go
package ariregisterdb

import "strings"

const defaultSourceProfileCopyLimit int32 = 1000

type NormalizeSourceProfilesCommand struct {
	IDs     []string
	Filters map[string]string
	Limit   int32
	Trigger string
}

type NormalizeSourceProfilesResult struct {
	RecordsSeen                int32
	CompaniesUpserted          int32
	CompanyNamesUpserted       int32
	CompanyStatusesUpserted    int32
	LegalFormsUpserted         int32
	AddressesUpserted          int32
	ContactsUpserted           int32
	WebsitesUpserted           int32
	DomainsUpserted            int32
	IndustriesUpserted         int32
	CapitalUpserted            int32
	FinancialYearPeriodsUpserted int32
	AnnualReportsUpserted      int32
	ArticlesUpserted           int32
	RegistryNotesUpserted      int32
}

func textFilter(filters map[string]string, keys ...string) *string {
	if filters == nil {
		return nil
	}
	for _, key := range keys {
		value := strings.TrimSpace(filters[key])
		if value != "" {
			return &value
		}
	}
	return nil
}
```

If `textFilter` already exists in this package from another Ariregister DB file, reuse it instead of duplicating.

- [ ] **Step 3: Implement COPY selection and merge**

Use BRREG's `scheduler/internal/brreg/db/source_profile_copy.go` pattern, adapted to Ariregister:

```sql
SELECT
  rr.id,
  rr.source_native_id,
  rr.registry_code,
  rr.legal_name,
  rr.registration_status,
  rr.legal_form,
  rr.website,
  rr.email,
  rr.phone,
  rr.country_iso2,
  rr.source_updated_at,
  rr.raw_payload,
  rr.payload_hash
FROM ariregister_workflow.raw_records rr
LEFT JOIN ariregister_source.companies source_company
  ON source_company.registry_code = rr.registry_code
 AND source_company.row_status = 'active'
WHERE rr.is_current
  AND (
    COALESCE(cardinality($1::text[]), 0) = 0
    OR rr.id::text = ANY($1::text[])
    OR source_company.id::text = ANY($1::text[])
  )
  AND (
    $2::text IS NULL
    OR rr.legal_name ILIKE '%' || $2::text || '%'
    OR rr.registry_code ILIKE '%' || $2::text || '%'
  )
  AND (
    COALESCE(cardinality($1::text[]), 0) > 0
    OR source_company.id IS NULL
    OR source_company.payload_hash IS DISTINCT FROM rr.payload_hash
  )
ORDER BY rr.registry_code
LIMIT $3::integer
```

Merge behavior:

- Insert a new active `ariregister_source.companies` row when the registry code is new.
- If the active row exists and `payload_hash` changed, mark old row `row_status = 'superseded'`, set `superseded_at = now()`, then insert the new company and dependent rows.
- Insert dependent rows after company rows using `registry_code` joins from stage tables to active companies.
- Use `ON CONFLICT` for dependent row uniqueness where possible.
- Keep all merge work in one transaction.

- [ ] **Step 4: Run gateway tests and commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/db -run 'TestNormalizeSourceProfiles' -count=1
```

Expected: PASS.

Commit:

```bash
git add scheduler/internal/ariregister/db/source_profile.go \
        scheduler/internal/ariregister/db/source_profile_copy.go \
        scheduler/internal/ariregister/db/source_profile_copy_sql.go \
        scheduler/internal/ariregister/db/source_profile_test.go
git commit -m "feat(ariregister): merge source profile rows"
```

## Task 6: Temporal Workflow and Activity

**Files:**
- Create: `scheduler/internal/ariregister/actions/source_profile_actions.go`
- Create: `scheduler/internal/ariregister/workflow/source_profile.go`
- Create: `scheduler/internal/ariregister/workflow/source_profile_test.go`
- Create: `scheduler/internal/app/ariregister_source_profile_temporal.go`
- Modify: `scheduler/internal/app/temporal.go`
- Modify: `scheduler/internal/app/temporal_test.go`

- [ ] **Step 1: Write workflow tests first**

Mirror BRREG `source_profile_test.go` expectations:

- Default batch size is `5000`.
- Workflow calls `NormalizeAriregisterSourceProfilesWithCopyActivity` repeatedly until fewer rows are returned than requested.
- IDs mode executes one chunk.
- Refresh workflow calls `RefreshAriregisterSourceExplorerActivity`.

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/workflow -run 'TestNormalizeAriregisterSourceProfiles' -count=1
```

Expected: fails because workflow does not exist.

- [ ] **Step 2: Implement action and workflow types**

Use these names:

```go
const (
	NormalizeAriregisterSourceProfilesTaskQueue            = "ariregister-source-profile"
	NormalizeAriregisterSourceProfilesWithCopyWorkflowName = "NormalizeAriregisterSourceProfilesWithCopy"
	RefreshAriregisterSourceExplorerTaskQueue              = "ariregister-source-explorer-refresh"
	RefreshAriregisterSourceExplorerWorkflowName           = "RefreshAriregisterSourceExplorer"
)
```

Activity input/result should mirror BRREG but with Ariregister row families:

```go
type NormalizeAriregisterSourceProfilesActivityInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id,omitempty"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	Trigger            string            `json:"trigger,omitempty"`
}
```

Register Temporal workflows directly in `scheduler/internal/app/ariregister_source_profile_temporal.go` with `RegisterWorkflow` and `RegisterActivityWithOptions`. Do not add registry interfaces.

- [ ] **Step 3: Run workflow/app tests and commit**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/workflow ./internal/app -count=1
```

Expected: PASS.

Commit:

```bash
git add scheduler/internal/ariregister/actions/source_profile_actions.go \
        scheduler/internal/ariregister/workflow/source_profile.go \
        scheduler/internal/ariregister/workflow/source_profile_test.go \
        scheduler/internal/app/ariregister_source_profile_temporal.go \
        scheduler/internal/app/temporal.go \
        scheduler/internal/app/temporal_test.go
git commit -m "feat(ariregister): add source profile temporal workflow"
```

## Task 7: HTTP Trigger and UI Action

**Files:**
- Modify: `scheduler/internal/httpapi/handlers.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers.go`
- Modify: `scheduler/internal/httpapi/workflow_triggers_test.go`
- Modify: `ui/app/lib/api.ts`
- Create: `ui/app/components/app/AriregisterSourceProfileActionForm.tsx`
- Modify: `ui/app/components/app/AriregisterRawInputActionSheet.tsx`

- [ ] **Step 1: Add backend trigger tests**

Add tests equivalent to:

```go
func TestStartAriregisterSourceProfileWorkflowStartsTemporalWorkflow(t *testing.T) {
	tc := &temporalExecuteRecorder{}
	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, nil, nil, "", tc, ""))

	req := httptest.NewRequest(http.MethodPost, "/api/v1/workflows/ariregister/source-profile", bytes.NewBufferString(`{
		"limit": 1000,
		"batch_size": 500,
		"trigger": "manual"
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusAccepted, w.Code)
	require.Contains(t, w.Body.String(), `"status":"started"`)
	require.Contains(t, w.Body.String(), `"workflow":"NormalizeAriregisterSourceProfilesWithCopy"`)
	require.Equal(t, "ariregister-source-profile", tc.options.TaskQueue)
}
```

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/httpapi -run 'TestStartAriregisterSourceProfile' -count=1
```

Expected: fails because route does not exist.

- [ ] **Step 2: Implement HTTP trigger**

Add route:

```go
r.Post("/workflows/ariregister/source-profile", h.handleStartAriregisterSourceProfileWorkflow)
```

Request fields:

```go
type startAriregisterSourceProfileWorkflowRequest struct {
	IDs       []string          `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	BatchSize int               `json:"batch_size,omitempty"`
	Trigger   string            `json:"trigger,omitempty"`
}
```

Validation:

- `limit >= 0`
- `batch_size >= 0`
- `trigger` defaults to `manual` in workflow/action, not the handler.

- [ ] **Step 3: Add UI API method**

In `ui/app/lib/api.ts`, add:

```ts
type StartAriregisterSourceProfileRequest = {
  ids?: string[];
  filters?: Record<string, string>;
  limit?: number;
  batch_size?: number;
  trigger?: string;
};

loadAriregisterSourceProfiles: (
  body: StartAriregisterSourceProfileRequest = {},
) => post<StartWorkflowResponse>("/workflows/ariregister/source-profile", body),
```

- [ ] **Step 4: Add UI action form**

Create `ui/app/components/app/AriregisterSourceProfileActionForm.tsx` with the same mode/limit structure as `AriregisterBulkLoadActionForm`, but copy should say:

- Heading: `Build Ariregister source profile`
- Description: `Parse current Ariregister raw JSON rows into ariregister_source tables.`
- Submit button: `Start source profile build`

- [ ] **Step 5: Add action to the Ariregister raw-input action sheet**

Add an action key:

```ts
type AriregisterRawInputAction = "" | "load_bulk" | "build_source_profile";
```

Add action metadata:

```ts
{
  key: "build_source_profile",
  label: "Build source profile",
  description: "Parse current raw records into ariregister_source tables.",
}
```

Render `AriregisterSourceProfileActionForm` for `selectedAction === "build_source_profile"`.

- [ ] **Step 6: Run backend and UI tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/httpapi -run 'TestStartAriregisterSourceProfile|TestStartAriregisterBulk' -count=1
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: both commands pass.

- [ ] **Step 7: Commit trigger and UI action**

```bash
git add scheduler/internal/httpapi/handlers.go \
        scheduler/internal/httpapi/workflow_triggers.go \
        scheduler/internal/httpapi/workflow_triggers_test.go \
        ui/app/lib/api.ts \
        ui/app/components/app/AriregisterSourceProfileActionForm.tsx \
        ui/app/components/app/AriregisterRawInputActionSheet.tsx
git commit -m "feat(ariregister): add source profile build action"
```

## Task 8: Source Entries API/UI Projection

**Files:**
- Modify: `scheduler/internal/httpapi/source_read.go`
- Modify: `scheduler/internal/httpapi/sources_test.go`
- Modify: `ui/app/components/app/source-detail/sourceDetailUtils.ts`
- Modify: `ui/app/routes/sources_.$name.source_entries.tsx`

- [ ] **Step 1: Add tests for Ariregister source-entry support**

Add a source read test that asserts Ariregister exposes source entries once `ariregister_source.mv_company_explorer` exists:

```go
func TestGetAriregisterSourceExposesSourceEntries(t *testing.T) {
	q := &mockQuerier{}
	q.On("GetSourceByName", mock.Anything, "ariregister").Return(db.DataSource{
		ID:             uuid.New(),
		Name:           "ariregister",
		DisplayName:    "Ariregister (Estonia)",
		InputTableName: "ariregister_workflow.raw_records",
		SourceGroup:    "registry",
		SourceType:     "country_registry",
		Enabled:        true,
		Config:         json.RawMessage(`{"dataset_key":"general"}`),
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/ariregister", nil)
	w := httptest.NewRecorder()
	routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, "")).ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"source_entries_available":true`)
}
```

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/httpapi -run 'TestGetAriregisterSourceExposesSourceEntries' -count=1
```

Expected: fails until the source response adds the flag or capability.

- [ ] **Step 2: Reuse sqlc projection structs for API responses**

Do not add mirror DTO mappers for `ListAriregisterSourceEntriesRow`. Shape SQL to match the API response and return the generated projection struct directly or through a type alias, following the sqlc API boundary rule.

If the existing `sources_.$name.source_entries.tsx` route is BRREG-only, add a source switch:

- `brreg` -> existing BRREG endpoint/projection.
- `ariregister` -> new Ariregister endpoint/projection.

- [ ] **Step 3: Run source-entry tests and UI typecheck**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/httpapi -run 'SourceEntries|AriregisterSource' -count=1
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: PASS.

- [ ] **Step 4: Commit source-entry support**

```bash
git add scheduler/internal/httpapi/source_read.go \
        scheduler/internal/httpapi/sources_test.go \
        ui/app/components/app/source-detail/sourceDetailUtils.ts \
        ui/app/routes/sources_.$name.source_entries.tsx
git commit -m "feat(ariregister): expose source profile entries"
```

## Task 9: End-to-End Verification

**Files:**
- No new files unless fixing bugs found during verification.

- [ ] **Step 1: Run focused backend tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./internal/ariregister/... ./internal/app ./internal/httpapi ./internal/db -count=1
```

Expected: PASS. If unrelated BRREG tests fail in `./internal/db`, run the focused Ariregister migration tests and note the unrelated failure separately.

- [ ] **Step 2: Run UI typecheck**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: PASS.

- [ ] **Step 3: Apply migrations and rebuild**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose run --rm migrate
docker compose up -d --build scheduler ui
```

Expected: migration applies and both containers are healthy.

- [ ] **Step 4: Run a limited source-profile build from API**

```bash
curl -sS -X POST http://localhost:8092/api/v1/workflows/ariregister/source-profile \
  -H 'Content-Type: application/json' \
  -d '{"limit":1000,"batch_size":500,"trigger":"manual"}'
```

Expected: HTTP `202` JSON response with workflow `NormalizeAriregisterSourceProfilesWithCopy`.

- [ ] **Step 5: Verify parsed table counts**

```bash
psql "$DATABASE_URL" -c "
select
  (select count(*) from ariregister_source.companies where row_status = 'active') as companies,
  (select count(*) from ariregister_source.addresses) as addresses,
  (select count(*) from ariregister_source.industries) as industries,
  (select count(*) from ariregister_source.contacts) as contacts,
  (select count(*) from ariregister_source.annual_reports) as annual_reports;
"
```

Expected: `companies` is greater than zero after the workflow completes; dependent counts are nonzero for the current general-data sample.

- [ ] **Step 6: Browser smoke check**

Open:

```text
http://localhost:8094/sources/ariregister/raw_input
```

Verify:

- Action sheet shows `Build source profile`.
- Starting the action with a limited count returns a success toast.
- `http://localhost:8094/sources/ariregister/source_entries` shows parsed source entries after the workflow completes.

- [ ] **Step 7: Final full-suite check**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/scheduler
go test ./... -count=1
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/ui
pnpm typecheck
```

Expected: PASS, except for any already-known unrelated dirty-worktree failures explicitly documented before execution.

## Self-Review

- Spec coverage: The plan adds `ariregister_source`, parsed source tables, `_en` columns for translatable text, source-profile normalization, Temporal action, UI action, and explorer/detail projections.
- Scope: The first slice is limited to fields in the official general-data JSON. Roles/shareholders are deliberately deferred because they are not in the current bulk JSON sample.
- sqlc boundary: Read APIs should use sqlc projection structs from `ariregister_source_profile.sql`; parser/domain rows convert to COPY rows at the database boundary.
- Architecture constraints: No new interfaces are required. Temporal workflows and activities are registered directly in app wiring.

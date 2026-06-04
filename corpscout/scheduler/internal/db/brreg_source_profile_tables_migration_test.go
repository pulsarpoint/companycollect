package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregSourceProfileTablesMigrationDefinesSourceSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000074_brreg_source_profile_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS brreg_source")
	require.Contains(t, sql, "CREATE TABLE brreg_source.companies")
	require.Contains(t, sql, "CREATE TABLE brreg_source.addresses")
	require.Contains(t, sql, "CREATE TABLE brreg_source.industries")
	require.Contains(t, sql, "CREATE TABLE brreg_source.websites")
	require.Contains(t, sql, "CREATE TABLE brreg_source.domains")
	require.Contains(t, sql, "CREATE TABLE brreg_source.contacts")
	require.Contains(t, sql, "CREATE TABLE brreg_source.capital")
	require.Contains(t, sql, "CREATE TABLE brreg_source.financial_statements")
	require.Contains(t, sql, "CREATE TABLE brreg_source.action_tasks")
	require.Contains(t, sql, "action_type TEXT NOT NULL")
	require.Contains(t, sql, "source_fingerprint TEXT NOT NULL")
	require.Contains(t, sql, "target_key TEXT")
	require.NotContains(t, sql, "CREATE TABLE brreg_source.field_translation_tasks")

	require.Contains(t, sql, "latitude NUMERIC(10, 7)")
	require.Contains(t, sql, "longitude NUMERIC(10, 7)")
	require.Contains(t, sql, "geocode_provider TEXT")
	require.Contains(t, sql, "geocode_payload JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_source_addresses_coordinates")

	require.Contains(t, sql, "website_type IN ('official_site', 'social_profile', 'marketplace', 'directory_profile', 'contact_page', 'other', 'unknown')")
	require.Contains(t, sql, "domain_type IN ('official', 'related', 'email_domain', 'infrastructure', 'unknown')")
	require.Contains(t, sql, "share_count BIGINT")
	require.Contains(t, sql, "UNIQUE (action_type, source_table, source_row_id, target_key, source_fingerprint)")

	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_source.v_company_explorer AS")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_source.v_company_detail AS")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_source.v_missing_translations AS")
	require.Contains(t, sql, "GRANT SELECT ON ALL TABLES IN SCHEMA brreg_source TO corpscout_anon")
}

func TestBrregSourceProfileTablesDownMigrationDropsSourceSchemaObjects(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000074_brreg_source_profile_tables.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS brreg_source.v_missing_translations")
	require.Contains(t, sql, "DROP VIEW IF EXISTS brreg_source.v_company_detail")
	require.Contains(t, sql, "DROP VIEW IF EXISTS brreg_source.v_company_explorer")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.action_tasks")
	require.NotContains(t, sql, "DROP TABLE IF EXISTS brreg_source.field_translation_tasks")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.financial_statements")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.domains")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.websites")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.addresses")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_source.companies")
}

func TestBrregSourceProfileRepairMigrationUpgradesPreviouslyAppliedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000077_repair_brreg_source_action_tasks.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS brreg_source.action_tasks")
	require.Contains(t, sql, "to_regclass('brreg_source.field_translation_tasks')")
	require.Contains(t, sql, "'translate_field'")
	require.Contains(t, sql, "source_text_hash")
	require.Contains(t, sql, "CREATE VIEW brreg_source.v_company_explorer AS")
	require.Contains(t, sql, "translation_pending_count")
	require.Contains(t, sql, "domain_pending_count")
	require.Contains(t, sql, "CREATE VIEW brreg_source.v_company_detail AS")
	require.Contains(t, sql, "FROM brreg_source.action_tasks")
}

func TestBrregSourceProfileRepairDownMigrationKeepsSchemaObjects(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000077_repair_brreg_source_action_tasks.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.NotContains(t, sql, "DROP TABLE")
	require.NotContains(t, sql, "DROP VIEW")
}

func TestBrregSourceCompanyExplorerMaterializedViewMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000083_brreg_source_company_explorer_materialized_view.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE MATERIALIZED VIEW brreg_source.mv_company_explorer AS")
	require.Contains(t, sql, "FROM brreg_source.v_company_explorer")
	require.Contains(t, sql, "CREATE UNIQUE INDEX uq_brreg_source_mv_company_explorer_company")
	require.Contains(t, sql, "idx_brreg_source_mv_company_explorer_updated")
	require.Contains(t, sql, "idx_brreg_source_mv_company_explorer_org_name_trgm")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source.mv_company_explorer TO corpscout_anon")
}

func TestBrregSourceCompanyTranslationStatusMaterializedViewMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000085_brreg_source_company_translation_status_materialized_view.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE MATERIALIZED VIEW brreg_source.mv_company_translation_status AS")
	require.Contains(t, sql, "FROM brreg_source.v_missing_translations")
	require.Contains(t, sql, "CREATE UNIQUE INDEX uq_brreg_source_mv_company_translation_status_company")
	require.Contains(t, sql, "idx_brreg_source_mv_company_translation_status_missing")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source.mv_company_translation_status TO corpscout_anon")
}

func TestBrregSourceCompanyExplorerFinancialStatusMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000086_brreg_source_financial_status_explorer.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CASE")
	require.Contains(t, sql, "THEN 'success'")
	require.Contains(t, sql, "process_status.financial_status = 'skipped'")
	require.Contains(t, sql, "ELSE 'unknown'")
	require.Contains(t, sql, "AS financial_status")
	require.Contains(t, sql, "idx_brreg_source_mv_company_explorer_financial_status_updated")
	require.Contains(t, sql, "DROP VIEW IF EXISTS brreg_source.v_company_explorer")
	require.NotContains(t, sql, "CREATE OR REPLACE VIEW brreg_source.v_company_explorer")
}

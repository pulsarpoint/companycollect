package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregEnhancedSourceTablesMigrationDefinesWorkflowHandoff(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000051_brreg_enhanced_source_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_enhanced_raw_inputs")
	require.Contains(t, sql, "raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs(id) ON DELETE CASCADE")
	require.Contains(t, sql, "enhancement_version TEXT NOT NULL DEFAULT 'brreg.enhanced.v1'")
	require.Contains(t, sql, "orchestrator_run_id TEXT")
	require.Contains(t, sql, "asset_key TEXT")
	require.NotContains(t, sql, "dagster_run_id")
	require.NotContains(t, sql, "dagster_asset_key")
	require.Contains(t, sql, "status IN ('queued', 'running', 'succeeded', 'partial', 'failed', 'cancelled', 'superseded')")
	require.Contains(t, sql, "section_statuses JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "enhanced_payload JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "UNIQUE (raw_input_id, payload_hash, enhancement_version, attempt)")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_enhanced_latest_usable")
}

func TestBrregEnhancedSourceTablesMigrationRenamesLegacyDagsterColumns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000056_brreg_remove_dagster_schema_names.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "RENAME COLUMN dagster_run_id TO orchestrator_run_id")
	require.Contains(t, sql, "RENAME COLUMN dagster_asset_key TO asset_key")
	require.Contains(t, sql, "idx_brreg_enhanced_orchestrator_run")
	require.Contains(t, sql, "SET source = 'workflow'")
	require.Contains(t, sql, "ALTER COLUMN source SET DEFAULT 'workflow'")
	require.Contains(t, sql, "CHECK (source IN ('workflow', 'manual', 'corpscout'))")
}

func TestBrregSourceMetadataPointsToWorkflowRawRecords(t *testing.T) {
	seedBody, err := os.ReadFile("../../../database/migrations/000017_source_ingestion_mvp.up.sql")
	require.NoError(t, err)
	seedSQL := string(seedBody)
	require.Contains(t, seedSQL, "'brreg_workflow.raw_records'")
	require.Contains(t, seedSQL, "'brreg_bulk_ingest'")
	require.NotContains(t, seedSQL, "'brreg_company_raw_inputs',\n     'source_pull'")

	upgradeBody, err := os.ReadFile("../../../database/migrations/000057_brreg_source_metadata_workflow.up.sql")
	require.NoError(t, err)
	upgradeSQL := string(upgradeBody)
	require.Contains(t, upgradeSQL, "input_table_name = 'brreg_workflow.raw_records'")
	require.Contains(t, upgradeSQL, "pull_task_type = 'brreg_bulk_ingest'")
	require.Contains(t, upgradeSQL, "processor_task_type = NULL")
}

func TestBrregEnhancedSourceTablesMigrationDefinesNormalizedSourceTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000051_brreg_enhanced_source_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_source_companies")
	require.Contains(t, sql, "organization_form_description_en TEXT")
	require.Contains(t, sql, "activity_description_en TEXT")
	require.Contains(t, sql, "statutory_purpose_en TEXT")
	require.Contains(t, sql, "CREATE UNIQUE INDEX uq_brreg_source_companies_active_org")

	require.Contains(t, sql, "CREATE TABLE brreg_source_addresses")
	require.Contains(t, sql, "address_type IN ('business', 'postal')")
	require.Contains(t, sql, "street_lines TEXT[] NOT NULL DEFAULT '{}'::text[]")

	require.Contains(t, sql, "CREATE TABLE brreg_source_industries")
	require.Contains(t, sql, "description_en TEXT")
	require.Contains(t, sql, "classification_type IN ('industry', 'helper_unit', 'institutional_sector')")

	require.Contains(t, sql, "CREATE TABLE brreg_source_capital")
	require.Contains(t, sql, "capital_type_en TEXT")
	require.Contains(t, sql, "amount_usd_cents BIGINT")

	require.Contains(t, sql, "CREATE TABLE brreg_source_domains")
	require.Contains(t, sql, "source TEXT NOT NULL DEFAULT 'workflow'")
	require.Contains(t, sql, "source IN ('workflow', 'manual', 'corpscout')")
	require.Contains(t, sql, "UNIQUE (source_company_id, normalized_domain)")

	require.Contains(t, sql, "CREATE TABLE brreg_source_financials")
	require.Contains(t, sql, "is_consolidated BOOLEAN NOT NULL DEFAULT false")
	require.Contains(t, sql, "UNIQUE (source_company_id, fiscal_year, statement_type, is_consolidated)")
}

func TestBrregEnhancedSourceTablesMigrationGrantsReadAccess(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000051_brreg_enhanced_source_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "GRANT SELECT ON brreg_enhanced_raw_inputs TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source_companies TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source_addresses TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source_industries TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source_capital TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source_domains TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON brreg_source_financials TO corpscout_anon")
}

package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregLegacyPublicRawCleanupMigrationRetiresOldRawInputTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000064_retire_legacy_brreg_public_raw_inputs.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "legacy_brreg_company_raw_inputs")
	require.Contains(t, sql, "legacy_brreg_raw_input_actions")
	require.Contains(t, sql, "legacy_brreg_raw_input_action_events")
	require.Contains(t, sql, "legacy_brreg_raw_input_domains")
	require.Contains(t, sql, "DROP VIEW IF EXISTS v_brreg_raw_input_action_attributes")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_source_raw_inputs AS")
	require.Contains(t, sql, "'ai_company_profile_raw_inputs' AS source_input_table")
	require.Contains(t, sql, "'domain_discovery_raw_inputs' AS source_input_table")
	require.NotContains(t, sql, "'brreg_company_raw_inputs' AS source_input_table")
	require.NotContains(t, sql, "FROM brreg_company_raw_inputs")
}

func TestBrregLegacyPublicRawCleanupMigrationRebuildsSourceTablesOnWorkflowRecords(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000064_retire_legacy_brreg_public_raw_inputs.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_source_companies")
	require.Contains(t, sql, "raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE")
	require.Contains(t, sql, "enhanced_record_id UUID NOT NULL REFERENCES brreg_workflow.enhanced_records(id) ON DELETE CASCADE")
	require.NotContains(t, sql, "CREATE TABLE brreg_enhanced_raw_inputs")
	require.NotContains(t, sql, "raw_input_id UUID NOT NULL REFERENCES brreg_company_raw_inputs")
	require.Contains(t, sql, "ADD CONSTRAINT enhanced_records_corpscout_source_company_id_fkey")
	require.Contains(t, sql, "REFERENCES brreg_source_companies(id) ON DELETE SET NULL")
}

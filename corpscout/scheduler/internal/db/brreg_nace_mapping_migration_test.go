package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregNACEMappingMigrationDefinesSourceSpecificMappings(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000071_brreg_nace_mappings.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_workflow.nace_mappings")
	require.Contains(t, sql, "raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE")
	require.Contains(t, sql, "nace_code_id UUID NOT NULL REFERENCES nace_codes(id) ON DELETE RESTRICT")
	require.Contains(t, sql, "source_field IN ('naeringskode1', 'naeringskode2', 'naeringskode3', 'hjelpeenhetskode')")
	require.Contains(t, sql, "classification_type IN ('industry', 'helper_unit')")
	require.Contains(t, sql, "mapping_method IN ('sn_level_5_to_nace_class', 'nace_exact')")
	require.Contains(t, sql, "confidence BETWEEN 0 AND 1")
	require.Contains(t, sql, "UNIQUE (raw_record_id, source_field, source_code, nace_code_id)")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_workflow.v_nace_mappings")
	require.NotContains(t, sql, "company_industries")
	require.NotContains(t, sql, "suggestion_company_industries")
}

func TestBrregNACEMappingDownMigrationDropsSourceSpecificMappings(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000071_brreg_nace_mappings.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS brreg_workflow.v_nace_mappings")
	require.Contains(t, sql, "DROP TABLE IF EXISTS brreg_workflow.nace_mappings")
}

package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceIndustryNACEMappingActionMigrationShape(t *testing.T) {
	up, err := os.ReadFile("../../../database/migrations/000117_source_industry_nace_mapping_action.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("../../../database/migrations/000117_source_industry_nace_mapping_action.down.sql")
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"DROP CONSTRAINT IF EXISTS chk_data_source_actions_action",
		"DROP CONSTRAINT IF EXISTS chk_data_source_action_runs_action",
		"'map_industries_to_nace'",
		"'Map industries to NACE'",
		"'CompanySourceIndustryNACEMappingWorkflow'",
		"'finland/prhytj'",
		"ON CONFLICT (source_id, action) DO UPDATE",
	} {
		require.Contains(t, sql, needle)
	}
	require.Contains(t, string(down), "DELETE FROM data_source_action_runs")
	require.Contains(t, string(down), "DELETE FROM data_source_actions")
	require.Equal(t, 2, strings.Count(string(down), "action IN ('pull_source', 'import_clickhouse', 'refresh_explorer_cache')"))
}

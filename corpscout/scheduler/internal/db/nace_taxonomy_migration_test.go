package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNACETaxonomyMigrationDefinesCanonicalTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000070_nace_taxonomy.up.sql")
	require.NoError(t, err)
	sql := string(body)

	required := []string{
		"CREATE TABLE nace_classifications",
		"CREATE TABLE nace_codes",
		"CREATE TABLE nace_code_aliases",
		"CREATE OR REPLACE VIEW v_nace_taxonomy_state",
		"CREATE OR REPLACE VIEW v_nace_code_tree",
	}
	for _, item := range required {
		require.Contains(t, sql, item)
	}

	require.Contains(t, sql, "code_system TEXT NOT NULL DEFAULT 'NACE'")
	require.Contains(t, sql, "revision TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (code_system, revision)")
	require.Contains(t, sql, "UNIQUE (classification_id, code)")
	require.Contains(t, sql, "level_name IN ('section', 'division', 'group', 'class')")
	require.Contains(t, sql, "level BETWEEN 1 AND 4")
	require.NotContains(t, sql, "brreg_workflow.")
	require.NotContains(t, sql, "68.200")
	require.NotContains(t, sql, "SN 2007")
	require.NotContains(t, sql, "SN 2025")
}

func TestNACETaxonomyDownMigrationDropsCanonicalObjects(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000070_nace_taxonomy.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_nace_code_tree")
	require.Contains(t, sql, "DROP VIEW IF EXISTS v_nace_taxonomy_state")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_code_aliases")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_codes")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_classifications")
}

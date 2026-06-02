package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNACESourceFilesMigrationDefinesHashAndRunTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000072_nace_source_files.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE nace_source_files")
	require.Contains(t, sql, "CREATE TABLE nace_import_runs")
	require.Contains(t, sql, "content_sha256 TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (revision, source_url, content_sha256)")
	require.Contains(t, sql, "status IN ('downloaded', 'processing', 'processed', 'failed')")
	require.Contains(t, sql, "status IN ('running', 'skipped', 'succeeded', 'failed')")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_nace_source_file_imports")
	require.Contains(t, sql, "GRANT SELECT ON nace_source_files TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON v_nace_source_file_imports TO corpscout_anon")
}

func TestNACESourceFilesDownMigrationDropsHashAndRunTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000072_nace_source_files.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_nace_source_file_imports")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_import_runs")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_source_files")
}

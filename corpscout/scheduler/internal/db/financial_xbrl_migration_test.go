package db_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinancialXBRLFinlandPRHDownloadLedgerMigrationShape(t *testing.T) {
	up, err := os.ReadFile("../../../database/migrations/000118_financial_xbrl_finland_prh_download_ledger.up.sql")
	require.NoError(t, err)
	down, err := os.ReadFile("../../../database/migrations/000118_financial_xbrl_finland_prh_download_ledger.down.sql")
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"CREATE SCHEMA IF NOT EXISTS financial_xbrl",
		"CREATE TABLE financial_xbrl.finland_prh_xbrl_discovery_windows",
		"CREATE TABLE financial_xbrl.finland_prh_xbrl_statement_artifacts",
		"UNIQUE (source_id, registered_date_start, registered_date_end)",
		"UNIQUE (source_id, business_id, financial_date)",
		"download_status IN ('pending', 'downloading', 'succeeded', 'failed')",
		"'financial_statements'",
		"'source_manifest'",
	} {
		require.Contains(t, sql, needle)
	}
	require.NotContains(t, sql, "INSERT INTO data_sources")
	require.NotContains(t, sql, "INSERT INTO data_source_files")
	require.NotContains(t, sql, "INSERT INTO data_source_actions")
	require.Contains(t, sql, "source_group IN (")
	require.Contains(t, sql, "source_file_name IS NULL OR source_file_name IN ('source.ndjson', 'source.json', 'statements.ndjson')")
	require.Contains(t, sql, "kind IN ('source_snapshot', 'source_manifest', 'code_list', 'reference_data', 'archive')")

	downSQL := string(down)
	require.Contains(t, downSQL, "DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_statement_artifacts")
	require.Contains(t, downSQL, "DROP TABLE IF EXISTS financial_xbrl.finland_prh_xbrl_discovery_windows")
	require.Contains(t, downSQL, "DELETE FROM data_source_actions")
	require.Contains(t, downSQL, "DELETE FROM data_source_files")
	require.Contains(t, downSQL, "DELETE FROM data_sources")
	require.Contains(t, downSQL, "DROP SCHEMA IF EXISTS financial_xbrl")
	require.Contains(t, downSQL, "DROP CONSTRAINT IF EXISTS chk_data_sources_source_group")
	require.Contains(t, downSQL, "DROP CONSTRAINT IF EXISTS chk_data_source_files_kind")
}

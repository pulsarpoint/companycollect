package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExchangeRatesMigrationDefinesReferenceTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000078_exchange_rates.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE exchange_rate_source_files")
	require.Contains(t, sql, "CREATE TABLE exchange_rate_sheets")
	require.Contains(t, sql, "CREATE TABLE exchange_rates")
	require.Contains(t, sql, "CREATE TABLE exchange_rate_sync_runs")
	require.Contains(t, sql, "UNIQUE (provider, source_url, content_sha256)")
	require.Contains(t, sql, "UNIQUE (provider, rate_date)")
	require.Contains(t, sql, "UNIQUE (sheet_id, currency)")
	require.Contains(t, sql, "rate_per_base NUMERIC(24, 12) NOT NULL")
	require.Contains(t, sql, "status IN ('downloaded', 'processing', 'processed', 'failed')")
	require.Contains(t, sql, "status IN ('running', 'skipped', 'succeeded', 'failed')")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_exchange_rate_sync_state")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_exchange_rate_sync_runs")
	require.Contains(t, sql, "GRANT SELECT ON exchange_rate_sheets TO corpscout_anon")
	require.Contains(t, sql, "GRANT SELECT ON exchange_rates TO corpscout_anon")
}

func TestExchangeRatesDownMigrationDropsReferenceTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000078_exchange_rates.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_exchange_rate_sync_runs")
	require.Contains(t, sql, "DROP VIEW IF EXISTS v_exchange_rate_sync_state")
	require.Contains(t, sql, "DROP TABLE IF EXISTS exchange_rate_sync_runs")
	require.Contains(t, sql, "DROP TABLE IF EXISTS exchange_rates")
	require.Contains(t, sql, "DROP TABLE IF EXISTS exchange_rate_sheets")
	require.Contains(t, sql, "DROP TABLE IF EXISTS exchange_rate_source_files")
}

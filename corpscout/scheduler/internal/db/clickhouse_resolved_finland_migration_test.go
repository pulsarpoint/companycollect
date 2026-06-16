package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseResolvedFinlandMigrationCreatesResolvedTables(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000012_create_resolved_finland_tables.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000012_create_resolved_finland_tables.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	upSQL := string(up)
	downSQL := string(down)

	require.Contains(t, upSQL, "CREATE DATABASE IF NOT EXISTS `corpscout_resolved`")
	require.NotContains(t, downSQL, "DROP DATABASE IF EXISTS `corpscout_resolved`")

	for _, table := range []string{
		"fi_companies",
		"fi_websites",
		"fi_industries",
		"fi_addresses",
		"fi_registered_entries",
		"fi_legal_forms",
		"fi_financial_statements",
		"fi_financial_metrics",
	} {
		require.Contains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout_resolved`.`"+table+"`")
		require.Contains(t, downSQL, "DROP TABLE IF EXISTS `corpscout_resolved`.`"+table+"`")
	}

	for _, column := range []string{
		"`source_system` LowCardinality(String)",
		"`source_run_id` String",
		"`source_record_id` String",
		"`source_payload_hash` FixedString(64)",
		"`resolved_at` DateTime64(3, 'UTC')",
	} {
		require.GreaterOrEqual(t, strings.Count(upSQL, column), 8, "audit column must appear on every resolved table")
	}

	require.Contains(t, upSQL, "`description_original` Nullable(String)")
	require.Contains(t, upSQL, "`description_language` Nullable(String)")
	require.Contains(t, upSQL, "`description_en` Nullable(String)")
	require.Contains(t, upSQL, "`description_translated_at` Nullable(DateTime64(3, 'UTC'))")
	require.Contains(t, upSQL, "`amount_original` Nullable(Decimal(38, 6))")
	require.Contains(t, upSQL, "`currency_original` Nullable(String)")
	require.Contains(t, upSQL, "`amount_usd` Nullable(Decimal(38, 6))")
	require.Contains(t, upSQL, "`fx_rate_date` Nullable(Date)")
}

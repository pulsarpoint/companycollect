package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseFinlandPRHYTJExplorerCacheMigrationShape(t *testing.T) {
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000008_create_finland_prhytj_company_explorer_cache.up.sql"))
	require.NoError(t, err)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000008_create_finland_prhytj_company_explorer_cache.down.sql"))
	require.NoError(t, err)

	sql := string(up)
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	require.Contains(t, sql, "ENGINE = MergeTree")
	require.Contains(t, sql, "ORDER BY (`business_id`)")
	require.Contains(t, sql, "allow_nullable_key = 1")
	require.Contains(t, sql, "FROM `corpscout_sources`.`fi_prhytj_company_explorer`")
	require.Contains(t, sql, "now64(3, 'UTC') AS refreshed_at")
	require.Contains(t, string(down), "DROP TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache`")
}

func TestClickHouseFinlandPRHYTJExplorerCacheBackfillMigrationShape(t *testing.T) {
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000009_backfill_finland_prhytj_company_explorer_cache.up.sql"))
	require.NoError(t, err)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000009_backfill_finland_prhytj_company_explorer_cache.down.sql"))
	require.NoError(t, err)

	sql := string(up)
	require.Contains(t, sql, "TRUNCATE TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	require.Contains(t, sql, "INSERT INTO `corpscout_sources`.`fi_prhytj_company_explorer_cache`")
	require.Contains(t, sql, "FROM `corpscout_sources`.`fi_prhytj_company_explorer`")
	require.Contains(t, sql, "now64(3, 'UTC') AS `refreshed_at`")
	require.Contains(t, string(down), "TRUNCATE TABLE IF EXISTS `corpscout_sources`.`fi_prhytj_company_explorer_cache`")
}

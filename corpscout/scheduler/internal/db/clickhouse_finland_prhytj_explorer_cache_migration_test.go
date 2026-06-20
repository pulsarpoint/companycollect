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
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prhytj_company_explorer_cache`")
	require.Contains(t, sql, "ENGINE = MergeTree")
	require.Contains(t, sql, "ORDER BY (`business_id`)")
	require.Contains(t, sql, "allow_nullable_key = 1")
	require.Contains(t, sql, "FROM `corpscout`.`fi_prhytj_company_explorer`")
	require.Contains(t, sql, "now64(3, 'UTC') AS refreshed_at")
	require.Contains(t, string(down), "DROP TABLE IF EXISTS `corpscout`.`fi_prhytj_company_explorer_cache`")
}

func TestClickHouseFinlandPRHYTJExplorerCacheBackfillMigrationShape(t *testing.T) {
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000009_backfill_finland_prhytj_company_explorer_cache.up.sql"))
	require.NoError(t, err)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000009_backfill_finland_prhytj_company_explorer_cache.down.sql"))
	require.NoError(t, err)

	sql := string(up)
	require.Contains(t, sql, "TRUNCATE TABLE IF EXISTS `corpscout`.`fi_prhytj_company_explorer_cache`")
	require.Contains(t, sql, "INSERT INTO `corpscout`.`fi_prhytj_company_explorer_cache`")
	require.Contains(t, sql, "FROM `corpscout`.`fi_prhytj_company_explorer`")
	require.Contains(t, sql, "now64(3, 'UTC') AS `refreshed_at`")
	require.Contains(t, string(down), "TRUNCATE TABLE IF EXISTS `corpscout`.`fi_prhytj_company_explorer_cache`")
}

func TestClickHouseFinlandPRHYTJIndustryNACEMappingMigrationShape(t *testing.T) {
	up, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000010_finland_prhytj_industry_nace_mapping.up.sql"))
	require.NoError(t, err)
	down, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000010_finland_prhytj_industry_nace_mapping.down.sql"))
	require.NoError(t, err)

	sql := string(up)
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prhytj_industry_nace_mappings`",
		"`source_code_set` Nullable(String)",
		"`source_code` Nullable(String)",
		"`source_code_prefix4` Nullable(String)",
		"`source_code_dotted4` Nullable(String)",
		"`source_extra_digit` Nullable(String)",
		"`nace_revision` Nullable(String)",
		"`nace_code` Nullable(String)",
		"`nace_section_code` Nullable(String)",
		"`nace_division_code` Nullable(String)",
		"`nace_group_code` Nullable(String)",
		"`nace_class_code` Nullable(String)",
		"`mapping_status` LowCardinality(String)",
		"ENGINE = ReplacingMergeTree(`mapped_at`)",
		"ORDER BY (`source_code_set`, `source_code`)",
		"SETTINGS allow_nullable_key = 1",
		"ADD COLUMN IF NOT EXISTS `nace_revision` Nullable(String)",
		"ADD COLUMN IF NOT EXISTS `nace_mapping_status` Nullable(String)",
	} {
		require.Contains(t, sql, needle)
	}
	require.Contains(t, string(down), "DROP TABLE IF EXISTS `corpscout`.`fi_prhytj_industry_nace_mappings`")
	require.Contains(t, string(down), "DROP COLUMN IF EXISTS `nace_revision`")
	require.Contains(t, string(down), "DROP COLUMN IF EXISTS `nace_mapping_status`")
}

package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseFinlandPRHYTJNormalizedMigrationReplacesOldTables(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000004_replace_finland_prhytj_normalized_tables.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000004_replace_finland_prhytj_normalized_tables.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	upSQL := string(up)
	downSQL := string(down)
	require.Contains(t, upSQL, "DROP TABLE IF EXISTS `corpscout`.`fi_prhytj_raw_records`")
	require.Contains(t, upSQL, "DROP TABLE IF EXISTS `corpscout`.`fi_prhytj_companies`")
	require.NotContains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prhytj_raw_records`")
	require.NotContains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prhytj_companies`")

	for _, table := range []string{
		"fi_prhytj_identifiers",
		"fi_prhytj_statuses",
		"fi_prhytj_names",
		"fi_prhytj_business_lines",
		"fi_prhytj_business_line_descriptions",
		"fi_prhytj_websites",
		"fi_prhytj_company_forms",
		"fi_prhytj_company_form_descriptions",
		"fi_prhytj_company_situations",
		"fi_prhytj_company_situation_descriptions",
		"fi_prhytj_registered_entries",
		"fi_prhytj_registered_entry_descriptions",
		"fi_prhytj_addresses",
		"fi_prhytj_address_post_offices",
	} {
		require.Contains(t, upSQL, "CREATE TABLE IF NOT EXISTS `corpscout`.`"+table+"`")
		require.Contains(t, downSQL, "DROP TABLE IF EXISTS `corpscout`.`"+table+"`")
	}

	require.Equal(t, 14, strings.Count(upSQL, "CREATE TABLE IF NOT EXISTS `corpscout`.`fi_prhytj_"))
}

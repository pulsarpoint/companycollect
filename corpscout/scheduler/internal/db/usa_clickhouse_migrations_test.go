package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseUSASourceMigrationCreatesRawAndCompanyTables(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000003_create_united_states_source_tables.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000003_create_united_states_source_tables.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	for _, table := range []string{
		"us_coloradoentities_raw_records",
		"us_coloradoentities_companies",
		"us_irseobmf_raw_records",
		"us_irseobmf_companies",
		"us_secedgar_raw_records",
		"us_secedgar_companies",
	} {
		require.Contains(t, string(up), "CREATE TABLE IF NOT EXISTS `corpscout`.`"+table+"`")
		require.Contains(t, string(down), "DROP TABLE IF EXISTS `corpscout`.`"+table+"`;")
	}
	require.Contains(t, string(up), "`raw_payload_json` Nullable(String)")
	require.Contains(t, string(up), "`source_export_id` UUID")
}

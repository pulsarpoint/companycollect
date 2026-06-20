package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseNACEReferenceMigrationShape(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "clickhouse", "migrations", "000007_create_nace_reference_tables.up.sql"))
	require.NoError(t, err)

	sql := string(body)
	require.Contains(t, sql, "CREATE DATABASE IF NOT EXISTS `corpscout`")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout`.`nace_classifications`")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout`.`nace_codes`")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS `corpscout`.`nace_code_aliases`")
	require.Contains(t, sql, "section_code")
	require.Contains(t, sql, "division_code")
	require.Contains(t, sql, "ReplacingMergeTree")
}

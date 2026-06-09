package db

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClickHouseInitialMigrationOnlyCreatesSourcesDatabase(t *testing.T) {
	upPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000001_create_databases.up.sql")
	downPath := filepath.Join("..", "..", "..", "clickhouse", "migrations", "000001_create_databases.down.sql")

	up, err := os.ReadFile(upPath)
	require.NoError(t, err)
	down, err := os.ReadFile(downPath)
	require.NoError(t, err)

	require.Contains(t, string(up), "CREATE DATABASE IF NOT EXISTS corpscout_sources")
	require.NotContains(t, string(up), "corpscout_projection")
	require.Equal(t, 1, strings.Count(string(up), "CREATE DATABASE"))
	require.Contains(t, string(down), "DROP DATABASE IF EXISTS corpscout_sources")
	require.NotContains(t, string(down), "corpscout_projection")
}

package db

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNACEClickHouseExportQueriesExist(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "..", "database", "queries", "nace_taxonomy.sql"))
	require.NoError(t, err)

	sql := string(body)
	require.Contains(t, sql, "-- name: ListNACEClassificationsForClickHouse :many")
	require.Contains(t, sql, "-- name: ListNACECodesForClickHouse :many")
	require.Contains(t, sql, "-- name: ListNACECodeAliasesForClickHouse :many")
	require.Contains(t, sql, "JOIN nace_classifications")
	require.Contains(t, sql, "FROM nace_code_aliases")
}

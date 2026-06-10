package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceActionRunQueriesExist(t *testing.T) {
	source, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(source)

	for _, queryName := range []string{
		"-- name: GetSourceActionByName :one",
		"-- name: CreateSourceActionRun :one",
		"-- name: GetSourceActionRun :one",
		"-- name: GetLatestSuccessfulSourceActionRun :one",
		"-- name: FinishSourceActionRun :one",
	} {
		require.True(t, strings.Contains(sql, queryName), "missing %s", queryName)
	}
}

func TestCreateSourceActionRunDerivesActionIdentity(t *testing.T) {
	source, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(source)

	createQuery := sql[strings.Index(sql, "-- name: CreateSourceActionRun :one"):]
	createQuery = createQuery[:strings.Index(createQuery, "-- name: GetSourceActionRun :one")]

	require.Contains(t, createQuery, "sqlc.arg(id)")
	require.Contains(t, createQuery, "sqlc.arg(temporal_workflow_id)")
	require.Contains(t, createQuery, "sqlc.arg(temporal_run_id)")
	require.Contains(t, createQuery, "FROM data_source_actions a")
	require.Contains(t, createQuery, "WHERE a.id = sqlc.arg(action_id)")
	require.NotContains(t, createQuery, "$1, $2, $3, 'running'")
}

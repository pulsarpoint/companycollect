package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceFileQueriesExist(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(body)

	required := []string{
		"-- name: UpsertDataSourceFileFromCatalog :exec",
		"-- name: DisableDataSourceFilesNotInCatalog :exec",
		"-- name: ListSourceFilesWithLatestRun :many",
		"-- name: GetSourceFileBySourceNameAndKey :one",
		"-- name: CreateSourceFileRun :one",
		"-- name: UpdateSourceFileRunTemporalRunID :exec",
		"-- name: FinishSourceFileRun :one",
		"-- name: ListSourceFileRuns :many",
		"-- name: ListSuccessfulSourceFileRunsForAction :many",
		"-- name: ListLatestSuccessfulRequiredSourceFileRuns :many",
		"-- name: GetSourceFileRunWithDefinition :one",
	}
	for _, needle := range required {
		require.Contains(t, sql, needle)
	}
}

func TestCreateSourceActionRunAcceptsDeterministicID(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/sources.sql")
	require.NoError(t, err)
	sql := string(body)

	start := strings.Index(sql, "-- name: CreateSourceActionRun :one")
	require.NotEqual(t, -1, start)
	createQuery := sql[start:]
	end := strings.Index(createQuery, "-- name: GetSourceActionRun :one")
	require.NotEqual(t, -1, end)
	createQuery = createQuery[:end]

	require.Contains(t, createQuery, "sqlc.arg(id)")
	require.Contains(t, createQuery, "sqlc.arg(temporal_workflow_id)")
	require.Contains(t, createQuery, "sqlc.arg(temporal_run_id)")
}

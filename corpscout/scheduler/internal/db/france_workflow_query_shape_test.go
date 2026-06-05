package db_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFranceWorkflowQueriesDefineBulkIngestContract(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/france_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	for _, expected := range []string{
		"-- name: BeginFranceWorkflowRun :one",
		"-- name: FinishFranceWorkflowRunWithStats :one",
		"-- name: FailFranceWorkflowRunByOrchestrator :exec",
		"-- name: CreateFranceBulkSnapshot :one",
		"-- name: MarkFranceBulkSnapshotParsed :exec",
		"-- name: RecordFranceSourceFile :one",
		"-- name: GetCurrentFranceWorkflowRawLegalUnit :one",
		"-- name: SupersedeCurrentFranceWorkflowRawLegalUnit :exec",
		"-- name: UpsertFranceWorkflowRawLegalUnit :one",
		"-- name: GetCurrentFranceWorkflowRawEstablishment :one",
		"-- name: SupersedeCurrentFranceWorkflowRawEstablishment :exec",
		"-- name: UpsertFranceWorkflowRawEstablishment :one",
		"ON CONFLICT (siren, payload_hash) DO UPDATE",
		"ON CONFLICT (siret, payload_hash) DO UPDATE",
		"UNIQUE (bulk_snapshot_id, dataset_key)",
	} {
		require.Contains(t, sql, expected)
	}
}

package db

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregWorkflowQueriesUseCorpscoutWorkflowSchema(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "brreg_workflow.raw_records")
	require.Contains(t, sql, "brreg_workflow.raw_record_task_states")
	require.Contains(t, sql, "brreg_workflow.task_attempts")
	require.Contains(t, sql, "brreg_workflow.translation_results")
	require.Contains(t, sql, "brreg_workflow.domain_results")
	require.Contains(t, sql, "brreg_workflow.financial_results")
	require.Contains(t, sql, "brreg_workflow.v_enhanced_asset_state")
	require.Contains(t, sql, "brreg_workflow.v_enhanced_ready_records")
	require.NotContains(t, sql, "dagster_brreg")
	require.NotContains(t, sql, "dagster_run_id")
}

func TestBrregWorkflowQueriesExposeActionBoundary(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	requiredQueries := []string{
		"-- name: BeginBrregWorkflowRun :one",
		"-- name: FinishBrregWorkflowRun :one",
		"-- name: FinishBrregWorkflowRunWithStats :one",
		"-- name: FailRunningBrregWorkflowTasksForRun :one",
		"-- name: RecoverStaleBrregWorkflowRuns :one",
		"-- name: UpsertBrregWorkflowRawRecord :one",
		"-- name: CreateBrregWorkflowTaskSelection :one",
		"-- name: ClaimBrregWorkflowTaskSelectionBatch :many",
		"-- name: FinishBrregWorkflowTaskAttempt :exec",
		"-- name: InsertBrregWorkflowTranslationResult :exec",
		"-- name: InsertBrregWorkflowDomainResult :exec",
		"-- name: InsertBrregWorkflowFinancialResult :exec",
		"-- name: GetBrregWorkflowTranslationAssetState :one",
		"-- name: GetBrregWorkflowDomainAssetState :one",
		"-- name: GetBrregWorkflowFinancialAssetState :one",
		"-- name: GetBrregWorkflowEnhancedAssetState :one",
		"-- name: ListBrregWorkflowEnhancedReadyRecords :many",
	}
	for _, query := range requiredQueries {
		require.Contains(t, sql, query)
	}
}

func TestCreateBrregWorkflowTaskSelectionReturnsInsertedSelection(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "records_selected")
	require.Contains(t, sql, "(SELECT count(*)::integer FROM eligible_records)")
	require.Contains(t, sql, "RETURNING id, selection_hash, records_selected")
	require.NotContains(t, sql, "UPDATE brreg_workflow.task_selections ts\n  SET records_selected")
}

func TestClaimBrregWorkflowTaskSelectionBatchReturnsRowsItClaims(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	selectionQuery := querySection(sql, "-- name: ClaimBrregWorkflowTaskSelectionBatch :many", "-- name: FinishBrregWorkflowTaskAttempt :exec")

	require.Contains(t, selectionQuery, "claimed_task_ids AS")
	require.Contains(t, selectionQuery, "JOIN claimed_task_ids cti ON cti.id = attempts.raw_record_id")
	require.NotContains(t, selectionQuery, "state_attempts AS")
}

func TestBrregWorkflowQueriesDoNotExposeUnselectedTaskClaims(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	require.NotContains(t, sql, "-- name: ClaimBrregWorkflowTaskBatch :many")
	require.NotContains(t, sql, "sqlc.arg('include_new_records')")
}

func TestRecoverStaleBrregWorkflowRunsClosesAuditOnlyRuns(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_workflow.sql")
	require.NoError(t, err)
	sql := string(body)

	recoveryQuery := querySection(sql, "-- name: RecoverStaleBrregWorkflowRuns :one", "-- name: UpsertBrregWorkflowRawRecord :one")

	require.Contains(t, recoveryQuery, "audit_only_stale_runs AS")
	require.Contains(t, recoveryQuery, "NOT EXISTS")
	require.Contains(t, recoveryQuery, "FROM brreg_workflow.task_attempts ta")
	require.Contains(t, recoveryQuery, "UNION")
}

func querySection(sql, startMarker, endMarker string) string {
	start := strings.Index(sql, startMarker)
	if start < 0 {
		return ""
	}
	end := strings.Index(sql[start+len(startMarker):], endMarker)
	if end < 0 {
		return sql[start:]
	}
	return sql[start : start+len(startMarker)+end]
}

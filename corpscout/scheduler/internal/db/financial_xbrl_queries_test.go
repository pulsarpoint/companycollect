package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinancialXBRLQueriesShape(t *testing.T) {
	sqlBytes, err := os.ReadFile("../../../database/queries/financial_xbrl.sql")
	require.NoError(t, err)
	sql := string(sqlBytes)

	for _, needle := range []string{
		"-- name: UpsertFinlandPRHXBRLDiscoveryWindow :one",
		"-- name: UpdateFinlandPRHXBRLDiscoveryProgress :one",
		"-- name: CompleteFinlandPRHXBRLDiscoveryWindow :one",
		"-- name: UpsertFinlandPRHXBRLStatementArtifact :one",
		"-- name: ListFinlandPRHXBRLStatementArtifactsToDownload :many",
		"-- name: MarkFinlandPRHXBRLStatementArtifactDownloading :one",
		"-- name: MarkFinlandPRHXBRLStatementArtifactSucceeded :one",
		"-- name: MarkFinlandPRHXBRLStatementArtifactFailed :one",
		"financial_xbrl.finland_prh_xbrl_discovery_windows",
		"financial_xbrl.finland_prh_xbrl_statement_artifacts",
		"ON CONFLICT (source_id, registered_date_start, registered_date_end) DO UPDATE",
		"ON CONFLICT (source_id, business_id, financial_date) DO UPDATE",
	} {
		require.Contains(t, sql, needle)
	}

	upsertWindow := querySection(t, sql, "UpsertFinlandPRHXBRLDiscoveryWindow")
	for _, needle := range []string{
		"ON CONFLICT (source_id, registered_date_start, registered_date_end) DO UPDATE SET",
		"action_run_id = EXCLUDED.action_run_id",
		"temporal_workflow_id = EXCLUDED.temporal_workflow_id",
		"temporal_run_id = EXCLUDED.temporal_run_id",
		"total_results = 0",
		"pages_discovered = 0",
		"statements_discovered = 0",
		"last_completed_page = 0",
		"completed_at = NULL",
	} {
		require.Contains(t, upsertWindow, needle)
	}

	listToDownload := querySection(t, sql, "ListFinlandPRHXBRLStatementArtifactsToDownload")
	for _, needle := range []string{
		"registration_date >= sqlc.arg(registered_date_start)",
		"registration_date <= sqlc.arg(registered_date_end)",
		"download_status = 'pending'",
		"download_status = 'failed' AND sqlc.arg(retry_failed)::boolean",
		"download_status = 'downloading' AND latest_action_run_id = sqlc.narg(latest_action_run_id)",
	} {
		require.Contains(t, listToDownload, needle)
	}

	markDownloading := querySection(t, sql, "MarkFinlandPRHXBRLStatementArtifactDownloading")
	for _, needle := range []string{
		"-- name: MarkFinlandPRHXBRLStatementArtifactDownloading :one",
		"WHERE id = sqlc.arg(id)",
		"AND source_id = sqlc.arg(source_id)",
		"registration_date >= sqlc.arg(registered_date_start)",
		"registration_date <= sqlc.arg(registered_date_end)",
		"download_status = 'pending'",
		"download_status = 'failed' AND sqlc.arg(retry_failed)::boolean",
		"download_status = 'downloading' AND latest_action_run_id = sqlc.narg(latest_action_run_id)",
	} {
		require.Contains(t, markDownloading, needle)
	}

	markSucceeded := querySection(t, sql, "MarkFinlandPRHXBRLStatementArtifactSucceeded")
	for _, needle := range []string{
		"xml_path = sqlc.arg(xml_path)::text",
		"xml_sha256 = sqlc.arg(xml_sha256)::text",
		"xml_size_bytes = sqlc.arg(xml_size_bytes)::bigint",
	} {
		require.Contains(t, markSucceeded, needle)
	}

	generatedBytes, err := os.ReadFile("gen/financial_xbrl.sql.go")
	require.NoError(t, err)
	generated := string(generatedBytes)

	downloadingParams := structSection(t, generated, "MarkFinlandPRHXBRLStatementArtifactDownloadingParams")
	require.Contains(t, downloadingParams, "SourceID            uuid.UUID")
	require.Contains(t, downloadingParams, "RegisteredDateStart pgtype.Date")
	require.Contains(t, downloadingParams, "RegisteredDateEnd   pgtype.Date")
	require.Contains(t, downloadingParams, "RetryFailed         bool")

	failedParams := structSection(t, generated, "MarkFinlandPRHXBRLStatementArtifactFailedParams")
	require.Contains(t, failedParams, "SourceID          uuid.UUID")

	succeededParams := structSection(t, generated, "MarkFinlandPRHXBRLStatementArtifactSucceededParams")
	require.Contains(t, succeededParams, "XmlPath           string")
	require.Contains(t, succeededParams, "XmlSha256         string")
	require.Contains(t, succeededParams, "XmlSizeBytes      int64")
	require.NotContains(t, succeededParams, "XmlPath           *string")
	require.NotContains(t, succeededParams, "XmlSha256         *string")
	require.NotContains(t, succeededParams, "XmlSizeBytes      *int64")
}

func querySection(t *testing.T, sql string, name string) string {
	t.Helper()

	marker := "-- name: " + name + " "
	start := strings.Index(sql, marker)
	require.NotEqual(t, -1, start)

	end := strings.Index(sql[start+len(marker):], "\n-- name: ")
	if end == -1 {
		return sql[start:]
	}
	return sql[start : start+len(marker)+end]
}

func structSection(t *testing.T, goSource string, name string) string {
	t.Helper()

	marker := "type " + name + " struct {"
	start := strings.Index(goSource, marker)
	require.NotEqual(t, -1, start)

	end := strings.Index(goSource[start:], "\n}\n")
	require.NotEqual(t, -1, end)
	return goSource[start : start+end+3]
}

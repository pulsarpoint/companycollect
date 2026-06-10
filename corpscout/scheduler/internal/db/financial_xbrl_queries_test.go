package db_test

import (
	"os"
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
}

package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetBrregSourceTranslationAssetStateUsesMaterializedStatus(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_source_profile.sql")
	require.NoError(t, err)
	sql := string(body)

	query := querySection(
		sql,
		"-- name: GetBrregSourceTranslationAssetState :one",
		"-- name: CountBrregSourceEntries :one",
	)

	require.Contains(t, query, "brreg_source.mv_company_translation_status")
	require.NotContains(t, query, "brreg_source.v_missing_translations")
}

func TestBrregSourceProfileQueriesDoNotUseLegacyTranslationActionTasks(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_source_profile.sql")
	require.NoError(t, err)
	sql := string(body)

	require.NotContains(t, sql, "brreg_source.action_tasks")
	require.NotContains(t, sql, "PrepareBrregSourceTranslationTasks")
	require.NotContains(t, sql, "ClaimBrregSourceTranslationBatch")
	require.NotContains(t, sql, "CompleteBrregSourceTranslationTask")
	require.NotContains(t, sql, "FailRunningBrregSourceTranslationTasksForRun")
}

func TestBrregSourceProfileQueriesExposeSourceTaskStateReads(t *testing.T) {
	body, err := os.ReadFile("../../../../database/queries/brreg_source_profile.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "-- name: GetBrregSourceDomainAssetState :one")
	require.Contains(t, sql, "-- name: GetBrregSourceFinancialAssetState :one")
	require.Contains(t, sql, "-- name: GetBrregSourceResultTableCounts :one")

	domainQuery := querySection(
		sql,
		"-- name: GetBrregSourceDomainAssetState :one",
		"-- name: GetBrregSourceFinancialAssetState :one",
	)
	require.Contains(t, domainQuery, "brreg_source.domains")
	require.NotContains(t, domainQuery, "brreg_workflow.domain_results")

	financialQuery := querySection(
		sql,
		"-- name: GetBrregSourceFinancialAssetState :one",
		"-- name: GetBrregSourceResultTableCounts :one",
	)
	require.Contains(t, financialQuery, "brreg_source.company_process_status")
	require.Contains(t, financialQuery, "brreg_source.financial_statements")
	require.NotContains(t, financialQuery, "brreg_workflow.financial_results")
}

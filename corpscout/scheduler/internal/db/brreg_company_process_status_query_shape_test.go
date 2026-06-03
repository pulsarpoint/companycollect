package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregCompanyProcessStatusQueriesExposeCompanyLevelOperations(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/brreg_company_process_status.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "-- name: EnsureBrregCompanyProcessStatuses")
	require.Contains(t, sql, "-- name: GetBrregCompanyProcessStatus")
	require.Contains(t, sql, "-- name: MarkBrregCompanyTranslationDirty")
	require.Contains(t, sql, "-- name: ClaimBrregCompanyTranslationBatch")
	require.Contains(t, sql, "-- name: MarkBrregCompanyTranslationSucceeded")
	require.Contains(t, sql, "-- name: MarkBrregCompanyTranslationFailed")
	require.Contains(t, sql, "-- name: MarkBrregCompanyCurrencyDirty")
	require.Contains(t, sql, "-- name: ClaimBrregCompanyCurrencyBatch")
	require.Contains(t, sql, "-- name: MarkBrregCompanyCurrencySucceeded")
	require.Contains(t, sql, "-- name: MarkBrregCompanyCurrencyFailed")
	require.Contains(t, sql, "-- name: MarkBrregCompanyFinancialDirty")
	require.Contains(t, sql, "-- name: ClaimBrregCompanyFinancialBatch")
	require.Contains(t, sql, "-- name: MarkBrregCompanyFinancialSucceeded")
	require.Contains(t, sql, "-- name: MarkBrregCompanyFinancialFailed")
	require.Contains(t, sql, "FOR UPDATE OF status_row SKIP LOCKED")
	require.Contains(t, sql, "translation_status IN ('pending', 'dirty', 'failed_retryable')")
	require.Contains(t, sql, "currency_status IN ('pending', 'dirty', 'failed_retryable')")
	require.Contains(t, sql, "financial_status IN ('pending', 'dirty', 'failed_retryable')")
}

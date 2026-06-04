package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregCompanyProcessStatusMigrationDefinesCompanyLevelStatus(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000081_brreg_company_process_status.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_source.company_process_status")
	require.Contains(t, sql, "company_id UUID PRIMARY KEY REFERENCES brreg_source.companies(id) ON DELETE CASCADE")
	require.Contains(t, sql, "translation_status TEXT NOT NULL DEFAULT 'pending'")
	require.Contains(t, sql, "currency_status TEXT NOT NULL DEFAULT 'pending'")
	require.Contains(t, sql, "financial_status TEXT NOT NULL DEFAULT 'pending'")
	require.Contains(t, sql, "status IN ('pending', 'dirty', 'running', 'succeeded', 'failed_retryable', 'failed_terminal', 'skipped')")
	require.Contains(t, sql, "translation_attempt_count INTEGER NOT NULL DEFAULT 0")
	require.Contains(t, sql, "currency_attempt_count INTEGER NOT NULL DEFAULT 0")
	require.Contains(t, sql, "financial_attempt_count INTEGER NOT NULL DEFAULT 0")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_company_process_status_translation_queue")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_company_process_status_currency_queue")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_company_process_status_financial_queue")
}

func TestBrregCompanyProcessStatusDownMigrationDropsTable(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000081_brreg_company_process_status.down.sql")
	require.NoError(t, err)
	require.Contains(t, string(body), "DROP TABLE IF EXISTS brreg_source.company_process_status")
}

func TestBrregCompanyFinancialClaimIndexesMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000088_brreg_source_financial_claim_indexes.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE INDEX IF NOT EXISTS idx_brreg_company_process_status_financial_running_lease")
	require.Contains(t, sql, "ON brreg_source.company_process_status(financial_lease_until, company_id)")
	require.Contains(t, sql, "WHERE financial_status = 'running'")
	require.Contains(t, sql, "CREATE INDEX IF NOT EXISTS idx_brreg_company_process_status_financial_ready_order")
	require.Contains(t, sql, "ON brreg_source.company_process_status(updated_at, company_id)")
	require.Contains(t, sql, "INCLUDE (financial_status, financial_attempt_count)")
	require.Contains(t, sql, "WHERE financial_status IN ('pending', 'dirty', 'failed_retryable')")
	require.NotContains(t, sql, "CONCURRENTLY")
}

func TestBrregCompanyFinancialClaimIndexesDownMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000088_brreg_source_financial_claim_indexes.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP INDEX IF EXISTS brreg_source.idx_brreg_company_process_status_financial_running_lease")
	require.Contains(t, sql, "DROP INDEX IF EXISTS brreg_source.idx_brreg_company_process_status_financial_ready_order")
	require.NotContains(t, sql, "CONCURRENTLY")
}

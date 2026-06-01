package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReclassifyBrregDomainProcessingResultsMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000065_reclassify_brreg_domain_processing_results.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "related_only_domain_results")
	require.Contains(t, sql, "SET status = 'not_found'")
	require.Contains(t, sql, "site_analysis_failed")
	require.Contains(t, sql, "status = 'failed'")
	require.Contains(t, sql, "ELSE 'failed_retryable'")
	require.Contains(t, sql, "retry_strategy = 'retry_with_backoff'")
	require.Contains(t, sql, "ts.task_type = 'discover_domains'")
}

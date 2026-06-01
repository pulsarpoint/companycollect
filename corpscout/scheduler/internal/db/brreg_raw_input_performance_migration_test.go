package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregRawInputPerformanceMigrationAddsQueueIndexes(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000049_brreg_raw_input_performance.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_inputs_created_at")
	require.Contains(t, sql, "ON brreg_company_raw_inputs (created_at DESC)")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_actions_enhance_lookup")
	require.Contains(t, sql, "WHERE action_type = 'enhance'")
}

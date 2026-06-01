package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregRawInputActionFilterIndexesMigrationAddsActionTypeLookup(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000050_brreg_raw_input_action_filter_indexes.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_actions_type_raw_payload_attempt")
	require.Contains(t, sql, "ON brreg_raw_input_actions (action_type, raw_input_id, payload_hash, attempt DESC, created_at DESC)")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_raw_input_action_events_action_status_latest")
	require.Contains(t, sql, "ON brreg_raw_input_action_events (action_id, status, created_at DESC)")
}

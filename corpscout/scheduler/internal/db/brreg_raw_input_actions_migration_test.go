package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregRawInputActionsMigrationDefinesLifecycleAndEvents(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000047_brreg_raw_input_state.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "state IN (")
	require.Contains(t, sql, "'input'")
	require.Contains(t, sql, "'suggestion_submitted'")
	require.NotContains(t, sql, "state = 'translating'")
	require.NotContains(t, sql, "state = 'translated'")
	require.NotContains(t, sql, "THEN 'translating'")
	require.NotContains(t, sql, "THEN 'translated'")
	require.Contains(t, sql, "CREATE TABLE brreg_raw_input_actions")
	require.Contains(t, sql, "CREATE TABLE brreg_raw_input_action_events")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW v_brreg_raw_input_action_attributes")
	require.Contains(t, sql, "has_successful_translation")
	require.Contains(t, sql, "latest_translation_action_status")
	require.Contains(t, sql, "has_successful_enhancement")
	require.Contains(t, sql, "latest_submission_action_status")
}

func TestBrregRawInputActionsMigrationBackfillsLegacyColumns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000047_brreg_raw_input_state.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "translation_status IN ('translated', 'failed')")
	require.Contains(t, sql, "translation_status WHEN 'translated'")
	require.Contains(t, sql, "processing_status IN ('processed', 'failed')")
	require.Contains(t, sql, "processing_status = 'failed'")
	require.Contains(t, sql, "status, message")
	require.Contains(t, sql, "'succeeded'")
	require.Contains(t, sql, "'failed'")
}

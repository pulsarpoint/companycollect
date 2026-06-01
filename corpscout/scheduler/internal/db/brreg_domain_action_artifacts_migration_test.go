package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregDomainActionArtifactsMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000066_brreg_domain_action_artifacts.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_workflow.domain_action_attempts")
	require.Contains(t, sql, "CREATE TABLE brreg_workflow.domain_action_artifacts")
	require.Contains(t, sql, "action_type IN (")
	require.Contains(t, sql, "'search_page_fetch'")
	require.Contains(t, sql, "'candidate_site_analysis'")
	require.Contains(t, sql, "status IN ('running', 'succeeded', 'failed', 'skipped')")
	require.Contains(t, sql, "artifact_type IN (")
	require.Contains(t, sql, "'site_analysis'")
	require.Contains(t, sql, "raw_record_id UUID NOT NULL REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE")
	require.Contains(t, sql, "task_attempt_id UUID REFERENCES brreg_workflow.task_attempts(id) ON DELETE SET NULL")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_domain_action_attempts_raw_action")
	require.Contains(t, sql, "CREATE INDEX idx_brreg_domain_action_artifacts_raw_type")
}

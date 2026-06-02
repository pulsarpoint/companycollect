package db

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBrregWorkflowStoreMigrationDefinesCorpscoutOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000053_brreg_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS brreg_workflow")
	require.NotContains(t, sql, "dagster_brreg")
	require.NotContains(t, sql, "dagster_run_id")
	require.NotContains(t, sql, "brreg_company_raw_inputs")
	require.NotContains(t, sql, "corpscout_raw_input_id")

	requiredTables := []string{
		"CREATE TABLE brreg_workflow.workflow_runs",
		"CREATE TABLE brreg_workflow.bulk_snapshots",
		"CREATE TABLE brreg_workflow.raw_records",
		"CREATE TABLE brreg_workflow.raw_record_task_states",
		"CREATE TABLE brreg_workflow.task_attempts",
		"CREATE TABLE brreg_workflow.translation_results",
		"CREATE TABLE brreg_workflow.domain_results",
		"CREATE TABLE brreg_workflow.financial_results",
		"CREATE TABLE brreg_workflow.enhanced_records",
	}
	for _, table := range requiredTables {
		require.Contains(t, sql, table)
	}
}

func TestBrregWorkflowRawRecordCompatibilityCleanupMigrationDropsLegacyLink(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000063_drop_brreg_workflow_raw_record_legacy_link.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_workflow.v_enhanced_ready_records")
	require.Contains(t, sql, "ALTER TABLE brreg_workflow.raw_records")
	require.Contains(t, sql, "DROP COLUMN IF EXISTS corpscout_raw_input_id")
	require.NotContains(t, sql, "brreg_company_raw_inputs")
}

func TestBrregWorkflowStoreMigrationDefinesTaskContract(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000053_brreg_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	requiredTasks := []string{
		"'translate'",
		"'discover_domains'",
		"'convert_financials'",
		"'build_enhanced'",
		"'publish'",
	}
	for _, task := range requiredTasks {
		require.Contains(t, sql, task)
	}

	taskStateSection := migrationSection(sql, "CREATE TABLE brreg_workflow.raw_record_task_states", "CREATE TABLE brreg_workflow.translation_results")
	requiredStatuses := []string{
		"'pending'",
		"'running'",
		"'failed_retryable'",
		"'failed_terminal'",
		"'cancelled'",
	}
	for _, status := range requiredStatuses {
		require.Contains(t, taskStateSection, status)
	}
	require.NotContains(t, taskStateSection, "'succeeded'")
	require.NotContains(t, taskStateSection, "'skipped'")

	require.Contains(t, sql, "lease_until TIMESTAMPTZ")
	require.Contains(t, sql, "next_retry_at TIMESTAMPTZ")
	require.Contains(t, sql, "error_category TEXT")
	require.Contains(t, sql, "retry_strategy TEXT")
}

func TestBrregWorkflowStoreMigrationDefinesLiveViews(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000053_brreg_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	requiredViews := []string{
		"CREATE OR REPLACE VIEW brreg_workflow.v_translation_asset_state",
		"CREATE OR REPLACE VIEW brreg_workflow.v_domain_asset_state",
		"CREATE OR REPLACE VIEW brreg_workflow.v_financial_asset_state",
		"CREATE OR REPLACE VIEW brreg_workflow.v_enhanced_asset_state",
		"CREATE OR REPLACE VIEW brreg_workflow.v_enhanced_ready_records",
	}
	for _, view := range requiredViews {
		require.Contains(t, sql, view)
	}

	require.NotContains(t, sql, "SELECT *\n  FROM brreg_workflow.raw_records")
	require.Contains(t, sql, "artifact_missing")
	require.Contains(t, sql, "task_running_active")
	require.Contains(t, sql, "task_failed_terminal")
	require.Contains(t, sql, "0::bigint AS task_succeeded")
	require.Contains(t, sql, "0::bigint AS task_skipped")
}

func TestBrregWorkflowRawRecordReadModelMigrationDefinesListAndDetailViews(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000054_brreg_workflow_raw_record_read_models.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_list")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW brreg_workflow.v_raw_record_detail")
	require.Contains(t, sql, "latest_translation")
	require.Contains(t, sql, "latest_domain")
	require.Contains(t, sql, "latest_financial")
	require.Contains(t, sql, "latest_enhanced")
	require.Contains(t, sql, "task_statuses")
	require.Contains(t, sql, "task_errors")
	require.NotContains(t, sql, "brreg_company_raw_inputs")
}

func TestBrregWorkflowRawRecordSourceSyncMigrationDefinesSyncStatus(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000076_brreg_raw_record_source_sync_status.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "LEFT JOIN brreg_source.companies source_company")
	require.Contains(t, sql, "source_company.payload_hash = rr.payload_hash")
	require.Contains(t, sql, "rr.last_seen_at <= source_company.updated_at")
	require.Contains(t, sql, "'needs_update'")
	require.Contains(t, sql, "'synced'")
	require.Contains(t, sql, "'not_synced'")
	require.Contains(t, sql, "AS sync_status")
	require.Contains(t, sql, "END::boolean AS synced")
	require.Contains(t, sql, "rr.last_seen_at AS updated_at")
}

func TestBrregSourceProfileNormalizationSupportsUnlimitedBatch(t *testing.T) {
	body, err := os.ReadFile("../../../database/queries/brreg_source_profile.sql")
	require.NoError(t, err)
	sql := string(body)
	require.Contains(t, sql, "LIMIT NULLIF(sqlc.arg('limit')::integer, 0)")
	require.NotContains(t, sql, "ORDER BY rr.organization_number\n  LIMIT GREATEST(sqlc.arg('limit')::integer, 1)")
}

func TestBrregWorkflowTaskSelectionMigrationDefinesRunScopedSelections(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000055_brreg_workflow_task_selections.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE brreg_workflow.task_selections")
	require.Contains(t, sql, "CREATE TABLE brreg_workflow.task_selection_records")
	require.Contains(t, sql, "selection_hash TEXT NOT NULL UNIQUE")
	require.Contains(t, sql, "records_selected INTEGER NOT NULL DEFAULT 0")
	require.Contains(t, sql, "PRIMARY KEY (selection_id, raw_record_id)")
	require.Contains(t, sql, "REFERENCES brreg_workflow.workflow_runs(id) ON DELETE CASCADE")
	require.Contains(t, sql, "REFERENCES brreg_workflow.raw_records(id) ON DELETE CASCADE")
}

func TestBrregWorkflowStoreDownMigrationDropsOwnedObjects(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000053_brreg_workflow_store.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP SCHEMA IF EXISTS brreg_workflow CASCADE")
	require.False(t, strings.Contains(sql, "dagster_brreg"))
}

func migrationSection(sql, startMarker, endMarker string) string {
	start := strings.Index(sql, startMarker)
	if start < 0 {
		return ""
	}
	end := strings.Index(sql[start+len(startMarker):], endMarker)
	if end < 0 {
		return sql[start:]
	}
	return sql[start : start+len(startMarker)+end]
}

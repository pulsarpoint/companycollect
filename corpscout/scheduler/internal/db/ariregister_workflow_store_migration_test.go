package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAriregisterWorkflowStoreMigrationDefinesCorpscoutOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000087_ariregister_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS ariregister_workflow")
	require.NotContains(t, sql, "brreg_workflow")
	require.NotContains(t, sql, "ariregister_company_raw_inputs")

	requiredTables := []string{
		"CREATE TABLE ariregister_workflow.workflow_runs",
		"CREATE TABLE ariregister_workflow.bulk_snapshots",
		"CREATE TABLE ariregister_workflow.source_files",
		"CREATE TABLE ariregister_workflow.raw_records",
		"CREATE TABLE ariregister_workflow.raw_record_task_states",
		"CREATE TABLE ariregister_workflow.task_attempts",
	}
	for _, table := range requiredTables {
		require.Contains(t, sql, table)
	}
}

func TestAriregisterWorkflowStoreMigrationDefinesRawRecordContract(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000087_ariregister_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "registry_code TEXT NOT NULL")
	require.Contains(t, sql, "legal_name TEXT")
	require.Contains(t, sql, "country_iso2 TEXT NOT NULL DEFAULT 'EE'")
	require.Contains(t, sql, "raw_payload JSONB NOT NULL")
	require.Contains(t, sql, "UNIQUE (registry_code, payload_hash)")
	require.Contains(t, sql, "idx_ariregister_workflow_raw_records_current_registry_code")
	require.Contains(t, sql, "WHERE is_current")
}

func TestAriregisterWorkflowStoreDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000087_ariregister_workflow_store.down.sql")
	require.NoError(t, err)

	require.Contains(t, string(body), "DROP SCHEMA IF EXISTS ariregister_workflow CASCADE")
}

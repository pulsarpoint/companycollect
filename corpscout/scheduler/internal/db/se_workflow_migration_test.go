package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSEWorkflowMigrationDefinesRawDumpSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000096_se_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS se_workflow")
	require.Contains(t, sql, "CREATE TABLE se_workflow.workflow_runs")
	require.Contains(t, sql, "CREATE TABLE se_workflow.bulk_snapshots")
	require.Contains(t, sql, "CREATE TABLE se_workflow.source_files")
	require.Contains(t, sql, "CREATE TABLE se_workflow.raw_records")
	require.Contains(t, sql, "organization_number TEXT NOT NULL")
	require.Contains(t, sql, "organization_name TEXT")
	require.Contains(t, sql, "legal_form TEXT")
	require.Contains(t, sql, "registration_status TEXT")
	require.Contains(t, sql, "business_description TEXT")
	require.Contains(t, sql, "sni_codes JSONB NOT NULL DEFAULT '[]'::jsonb")
	require.Contains(t, sql, "postal_address JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "raw_payload JSONB NOT NULL")
	require.Contains(t, sql, "UNIQUE (organization_number, payload_hash)")
	require.Contains(t, sql, "idx_se_workflow_raw_records_current_org")
	require.Contains(t, sql, "'se'")
	require.Contains(t, sql, "'se_workflow.raw_records'")
	require.Contains(t, sql, "SE_HVD_DATASETS_JSON")
}

func TestSEWorkflowDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000096_se_workflow_store.down.sql")
	require.NoError(t, err)

	require.Contains(t, string(body), "DROP SCHEMA IF EXISTS se_workflow CASCADE")
}

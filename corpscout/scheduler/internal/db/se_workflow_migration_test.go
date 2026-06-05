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
	require.Contains(t, sql, "https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip")
	require.Contains(t, sql, "https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip")
	require.Contains(t, sql, "'format', 'zip'")
}

func TestSEWorkflowSplitRawTablesMigrationStoresRealHVDFileSchemas(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000100_se_hvd_split_raw_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS se_workflow.bolagsverket_raw_records")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS se_workflow.scb_raw_records")
	require.Contains(t, sql, "organisationsidentitet TEXT NOT NULL")
	require.Contains(t, sql, "namnskyddslopnummer TEXT")
	require.Contains(t, sql, "pagande_avvecklings_eller_omstruktureringsforfarande TEXT")
	require.Contains(t, sql, "pe_org_nr TEXT NOT NULL")
	require.Contains(t, sql, "for_andr_typ TEXT")
	require.Contains(t, sql, "jur_form TEXT")
	require.Contains(t, sql, "ng1 TEXT")
	require.Contains(t, sql, "mask_columns JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "idx_se_workflow_bolagsverket_raw_current_key")
	require.Contains(t, sql, "idx_se_workflow_scb_raw_current_key")
}

func TestSEWorkflowDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000096_se_workflow_store.down.sql")
	require.NoError(t, err)

	require.Contains(t, string(body), "DROP SCHEMA IF EXISTS se_workflow CASCADE")
}

func TestSESourceFileDuplicateStatusMigrationTracksProcessedFileHashes(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000097_se_source_file_duplicate_status.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "skipped_duplicate")
	require.Contains(t, sql, "idx_se_workflow_source_files_hash_status")
	require.Contains(t, sql, "dataset_key, payload_hash, status")
}

func TestSEHVDSourceConfigMigrationStoresEditableDefaultDatasetURL(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000098_se_hvd_source_config.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "UPDATE data_sources")
	require.Contains(t, sql, "WHERE name = 'se'")
	require.Contains(t, sql, "'datasets'")
	require.Contains(t, sql, "'url', 'https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip'")
	require.Contains(t, sql, "'url', 'https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip'")
	require.Contains(t, sql, "'format', 'zip'")
	require.Contains(t, sql, "se_workflow.bolagsverket_raw_records")
	require.Contains(t, sql, "se_workflow.scb_raw_records")
}

func TestSEHVDDirectZipConfigRepairMigrationUpdatesPreviouslyMigratedDatabases(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000101_se_hvd_direct_zip_config_repair.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "UPDATE data_sources")
	require.Contains(t, sql, "WHERE name = 'se'")
	require.Contains(t, sql, "'dataset', 'bolagsverket'")
	require.Contains(t, sql, "'dataset', 'scb'")
	require.Contains(t, sql, "'url', 'https://vardefulla-datamangder.bolagsverket.se/bolagsverket/bolagsverket_bulkfil.zip'")
	require.Contains(t, sql, "'url', 'https://vardefulla-datamangder.bolagsverket.se/scb/scb_bulkfil.zip'")
	require.Contains(t, sql, "'format', 'zip'")
	require.Contains(t, sql, "se_workflow.bolagsverket_raw_records")
	require.Contains(t, sql, "se_workflow.scb_raw_records")
}

package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFranceWorkflowMigrationDefinesRawDumpSchemas(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000094_france_workflow_store.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS france_workflow")
	require.Contains(t, sql, "CREATE TABLE france_workflow.workflow_runs")
	require.Contains(t, sql, "CREATE TABLE france_workflow.bulk_snapshots")
	require.Contains(t, sql, "CREATE TABLE france_workflow.source_files")
	require.Contains(t, sql, "CREATE TABLE france_workflow.raw_legal_units")
	require.Contains(t, sql, "CREATE TABLE france_workflow.raw_establishments")
	require.Contains(t, sql, "UNIQUE (siren, payload_hash)")
	require.Contains(t, sql, "UNIQUE (siret, payload_hash)")
	require.Contains(t, sql, "idx_france_workflow_raw_legal_units_current_siren")
	require.Contains(t, sql, "idx_france_workflow_raw_establishments_current_siret")
	require.Contains(t, sql, "StockUniteLegale")
	require.Contains(t, sql, "StockEtablissement")
	require.Contains(t, sql, "'france'")
	require.Contains(t, sql, "'france_workflow.raw_legal_units'")
	require.Contains(t, sql, "350182c9-148a-46e0-8389-76c2ec1374a3")
	require.Contains(t, sql, "a29c1297-1f92-4e2a-8f6b-8c902ce96c5f")
}

func TestFranceSourceMigrationDefinesNormalizedReadModel(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000095_france_source_profile_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS france_source")
	require.Contains(t, sql, "CREATE TABLE france_source.companies")
	require.Contains(t, sql, "CREATE TABLE france_source.establishments")
	require.Contains(t, sql, "CREATE TABLE france_source.addresses")
	require.Contains(t, sql, "CREATE TABLE france_source.industries")
	require.Contains(t, sql, "CREATE TABLE france_source.websites")
	require.Contains(t, sql, "CREATE TABLE france_source.domains")
	require.Contains(t, sql, "CREATE TABLE france_source.contacts")
	require.Contains(t, sql, "CREATE TABLE france_source.action_tasks")
	require.Contains(t, sql, "CREATE TABLE france_source.translation_terms")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW france_source.v_missing_translations")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW france_source.v_company_explorer")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW france_source.v_company_detail")
	require.Contains(t, sql, "source_lang TEXT NOT NULL DEFAULT 'fr'")
	require.Contains(t, sql, "REFERENCES france_workflow.raw_legal_units")
	require.Contains(t, sql, "REFERENCES france_workflow.raw_establishments")
}

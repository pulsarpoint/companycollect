package db_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAriregisterSourceProfileMigrationDefinesSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000093_ariregister_source_profile_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE SCHEMA IF NOT EXISTS ariregister_source")
	for _, table := range []string{
		"companies",
		"company_names",
		"company_statuses",
		"legal_forms",
		"addresses",
		"contacts",
		"websites",
		"domains",
		"industries",
		"capital",
		"financial_year_periods",
		"annual_reports",
		"articles",
		"registry_notes",
		"action_tasks",
	} {
		require.Contains(t, sql, "CREATE TABLE ariregister_source."+table, table)
	}
	require.Contains(t, sql, "REFERENCES ariregister_workflow.raw_records")
	require.Contains(t, sql, "CREATE MATERIALIZED VIEW ariregister_source.mv_company_explorer")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW ariregister_source.v_company_detail")
	require.Contains(t, sql, "CREATE OR REPLACE VIEW ariregister_source.v_missing_translations")
}

func TestAriregisterSourceProfileMigrationDefinesTranslationColumns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000093_ariregister_source_profile_tables.up.sql")
	require.NoError(t, err)
	sql := string(body)

	for _, column := range []string{
		"legal_name_en",
		"registration_status_label_en",
		"legal_form_label_en",
		"legal_form_subtype_label_en",
		"region_label_en",
		"region_label_long_en",
		"active_label_en",
		"status_label_en",
		"country_label_en",
		"ehak_name_en",
		"street_text_en",
		"normalized_full_address_en",
		"contact_type_label_en",
		"emtak_label_en",
		"emtak_version_label_en",
		"capital_currency_label_en",
		"report_address_en",
		"activity_label_en",
		"activity_version_label_en",
		"explanation_en",
		"note_type_label_en",
		"note_text_en",
	} {
		require.True(t, strings.Contains(sql, column+" TEXT") || strings.Contains(sql, column+" TEXT,"),
			"missing translatable column %s", column)
	}
}

func TestAriregisterSourceTranslationTermsMigrationDefinesCacheAndStatus(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000099_ariregister_source_translation_terms.up.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "CREATE TABLE ariregister_source.translation_terms")
	require.Contains(t, sql, "CONSTRAINT chk_ariregister_source_translation_terms_source CHECK (source = 'ariregister')")
	require.Contains(t, sql, "CREATE MATERIALIZED VIEW ariregister_source.mv_company_translation_status AS")
	require.Contains(t, sql, "FROM ariregister_source.v_missing_translations")
}

func TestAriregisterSourceProfileDownMigrationDropsOwnedSchema(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000093_ariregister_source_profile_tables.down.sql")
	require.NoError(t, err)

	require.Contains(t, string(body), "DROP SCHEMA IF EXISTS ariregister_source CASCADE")
}

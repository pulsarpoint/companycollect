package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestSourceMetadataActionsMigrationDefinesMetadataOnlySources(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000111_source_metadata_actions.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"ADD COLUMN IF NOT EXISTS country TEXT",
		"ADD COLUMN IF NOT EXISTS source TEXT",
		"ADD COLUMN IF NOT EXISTS registry_key TEXT",
		"ADD COLUMN IF NOT EXISTS auth_required BOOLEAN NOT NULL DEFAULT false",
		"ADD COLUMN IF NOT EXISTS storage_kind TEXT NOT NULL DEFAULT 'clickhouse'",
		"ADD COLUMN IF NOT EXISTS clickhouse_database TEXT",
		"ADD COLUMN IF NOT EXISTS clickhouse_table_prefix TEXT",
		"CREATE TABLE data_source_actions",
		"CREATE TABLE data_source_action_runs",
		"'finland_prhytj'",
		"'united_states_coloradoentities'",
		"'united_states_irseobmf'",
		"'united_states_secedgar'",
		"'pull_source'",
		"'import_clickhouse'",
		"description = EXCLUDED.description",
		"auth_required = EXCLUDED.auth_required",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}

	retired := []string{
		"ADD COLUMN IF NOT EXISTS schedule_enabled",
		"ADD COLUMN IF NOT EXISTS schedule_kind",
		"ADD COLUMN IF NOT EXISTS schedule_expression",
		"ADD COLUMN IF NOT EXISTS last_started_at",
		"ADD COLUMN IF NOT EXISTS last_success_at",
		"ADD COLUMN IF NOT EXISTS last_failed_at",
		"ADD COLUMN IF NOT EXISTS last_error",
	}
	for _, needle := range retired {
		if strings.Contains(sql, needle) {
			t.Fatalf("source metadata migration should not add retired state column %q", needle)
		}
	}
}

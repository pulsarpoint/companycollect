package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestSourceFileStatusMigrationDefinesFileCatalogAndRuns(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000115_source_file_status.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"CREATE TABLE data_source_files",
		"CREATE TABLE data_source_file_runs",
		"UNIQUE (source_id, file_key)",
		"kind IN ('source_snapshot', 'code_list', 'reference_data', 'archive')",
		"status IN ('running', 'succeeded', 'failed', 'missing', 'skipped', 'cancelled')",
		"idx_data_source_file_runs_file_started",
		"idx_data_source_file_runs_source_status",
		"idx_data_source_file_runs_parent_action",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}
}

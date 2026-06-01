package db

import (
	"os"
	"strings"
	"testing"
)

func TestDropDataSourceTaskMetadataMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000060_drop_data_source_task_metadata.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)
	for _, needle := range []string{
		"ALTER TABLE data_sources",
		"DROP COLUMN pull_task_type",
		"DROP COLUMN processor_task_type",
	} {
		if !strings.Contains(sql, needle) {
			t.Fatalf("source task metadata migration missing %q", needle)
		}
	}
}

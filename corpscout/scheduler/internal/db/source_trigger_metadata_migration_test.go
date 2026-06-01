package db

import (
	"os"
	"strings"
	"testing"
)

func TestCleanupSourceTriggerMetadataMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000059_cleanup_source_trigger_metadata.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	for _, needle := range []string{
		"pull_task_type = 'temporal_download'",
		"WHERE name IN ('companies_house', 'gleif', 'cvr', 'ariregister')",
		"pull_task_type = 'brreg_bulk_ingest'",
		"input_table_name = 'brreg_workflow.raw_records'",
		"pull_task_type = 'unregistered'",
		"WHERE name IN ('wikidata', 'opencorporates')",
		"schedule_enabled = false",
		"schedule_kind = 'manual'",
		"schedule_expression = NULL",
	} {
		if !strings.Contains(sql, needle) {
			t.Fatalf("source trigger metadata migration missing %q", needle)
		}
	}
}

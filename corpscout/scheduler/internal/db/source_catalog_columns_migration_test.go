package db_test

import (
	"os"
	"strings"
	"testing"
)

func TestSourceCatalogColumnsMigrationAddsTypedSourceMetadata(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000113_source_catalog_columns.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)

	required := []string{
		"ADD COLUMN IF NOT EXISTS source_url TEXT",
		"ADD COLUMN IF NOT EXISTS docs_url TEXT",
		"ADD COLUMN IF NOT EXISTS raw_source_retention TEXT",
		"ADD COLUMN IF NOT EXISTS source_file_name TEXT",
		"ADD COLUMN IF NOT EXISTS user_agent_required BOOLEAN NOT NULL DEFAULT false",
		"chk_data_sources_source_file_name",
	}
	for _, needle := range required {
		if !strings.Contains(sql, needle) {
			t.Fatalf("migration missing %q", needle)
		}
	}

	forbidden := []string{
		"UPDATE data_sources",
		"https://avoindata.prh.fi",
		"https://data.colorado.gov",
		"https://www.irs.gov",
		"https://www.sec.gov",
	}
	for _, needle := range forbidden {
		if strings.Contains(sql, needle) {
			t.Fatalf("migration must not seed source catalog value %q", needle)
		}
	}
}

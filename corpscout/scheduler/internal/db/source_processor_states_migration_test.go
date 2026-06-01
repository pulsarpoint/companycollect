package db

import (
	"os"
	"strings"
	"testing"
)

func TestDropSourceProcessorStatesMigration(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000061_drop_source_processor_states.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	sql := string(body)
	if !strings.Contains(sql, "DROP TABLE IF EXISTS source_processor_states") {
		t.Fatal("source processor state migration should drop the unused compatibility table")
	}
}

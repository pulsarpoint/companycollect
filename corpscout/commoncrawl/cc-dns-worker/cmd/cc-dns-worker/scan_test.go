package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestCleanResolvers(t *testing.T) {
	got := cleanResolvers([]string{" 1.1.1.1:53 ", "", "   ", "8.8.8.8:53", "\t"})
	want := []string{"1.1.1.1:53", "8.8.8.8:53"}
	if len(got) != len(want) {
		t.Fatalf("cleanResolvers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cleanResolvers = %v, want %v", got, want)
		}
	}
}

func TestCleanResolversAllBlank(t *testing.T) {
	got := cleanResolvers([]string{"", "  ", " "})
	if len(got) != 0 {
		t.Fatalf("cleanResolvers(all blank) = %v, want empty", got)
	}
}

// TestRunScanRejectsEmptyResolvers proves --resolvers being empty/whitespace-only after the comma
// split fails fast with a clear error instead of silently making every domain's discovery fail one
// at a time later. This must return before any ClickHouse/SQLite I/O is attempted, so the test
// needs no external services.
func TestRunScanRejectsEmptyResolvers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scan.db")
	err := runScan([]string{"-resolvers= , ,  ", "-db", dbPath})
	if err == nil {
		t.Fatal("runScan with blank --resolvers: want error, got nil")
	}
	if !strings.Contains(err.Error(), "--resolvers is empty") {
		t.Fatalf("runScan error = %q, want to contain %q", err.Error(), "--resolvers is empty")
	}
}

package main

import "testing"

func TestParseArgsSyncSource(t *testing.T) {
	cfg, err := parseArgs([]string{"sync-source", "--source", "prhytj", "--data-dir", "/data", "--max-pages", "2"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "sync-source" || cfg.source != "prhytj" || cfg.dataDir != "/data" || cfg.maxPages != 2 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsBuildExport(t *testing.T) {
	cfg, err := parseArgs([]string{"build-export", "--data-dir", "/data", "--run-id", "final-run-1"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "build-export" || cfg.runID != "final-run-1" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsRejectsUnknownSource(t *testing.T) {
	_, err := parseArgs([]string{"sync-source", "--source", "unknown"})
	if err == nil {
		t.Fatal("parse args returned nil error")
	}
}

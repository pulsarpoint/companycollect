package main

import "testing"

func TestParseArgsRunCommand(t *testing.T) {
	cfg, err := parseArgs([]string{"run", "--env", ".env", "--data-dir", "/tmp/irs", "--max-files", "2", "--chunk-size", "500", "--limit", "100"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if cfg.command != "run" {
		t.Fatalf("command = %q, want run", cfg.command)
	}
	if cfg.envPath != ".env" {
		t.Fatalf("envPath = %q, want .env", cfg.envPath)
	}
	if cfg.dataDir != "/tmp/irs" {
		t.Fatalf("dataDir = %q, want /tmp/irs", cfg.dataDir)
	}
	if cfg.maxFiles != 2 {
		t.Fatalf("maxFiles = %d, want 2", cfg.maxFiles)
	}
	if cfg.chunkSize != 500 {
		t.Fatalf("chunkSize = %d, want 500", cfg.chunkSize)
	}
	if cfg.limit != 100 {
		t.Fatalf("limit = %d, want 100", cfg.limit)
	}
}

func TestParseArgsRejectsUnknownCommand(t *testing.T) {
	_, err := parseArgs([]string{"unknown"})
	if err == nil {
		t.Fatal("parseArgs returned nil error, want error")
	}
}

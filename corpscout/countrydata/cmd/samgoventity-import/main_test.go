package main

import "testing"

func TestParseArgsRunCommand(t *testing.T) {
	cfg, err := parseArgs([]string{"run", "--env", ".env", "--data-dir", "/tmp/sam", "--max-pages", "3", "--chunk-size", "500", "--limit", "100"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if cfg.command != "run" {
		t.Fatalf("command = %q, want run", cfg.command)
	}
	if cfg.envPath != ".env" {
		t.Fatalf("envPath = %q, want .env", cfg.envPath)
	}
	if cfg.dataDir != "/tmp/sam" {
		t.Fatalf("dataDir = %q, want /tmp/sam", cfg.dataDir)
	}
	if cfg.maxPages != 3 {
		t.Fatalf("maxPages = %d, want 3", cfg.maxPages)
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

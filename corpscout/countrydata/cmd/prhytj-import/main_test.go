package main

import "testing"

func TestParseArgsRunCommand(t *testing.T) {
	cfg, err := parseArgs([]string{"run", "--env", ".env", "--data-dir", "/tmp/prh", "--max-pages", "2", "--chunk-size", "25"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if cfg.command != "run" {
		t.Fatalf("command = %q, want run", cfg.command)
	}
	if cfg.envPath != ".env" {
		t.Fatalf("envPath = %q, want .env", cfg.envPath)
	}
	if cfg.dataDir != "/tmp/prh" {
		t.Fatalf("dataDir = %q, want /tmp/prh", cfg.dataDir)
	}
	if cfg.maxPages != 2 {
		t.Fatalf("maxPages = %d, want 2", cfg.maxPages)
	}
	if cfg.chunkSize != 25 {
		t.Fatalf("chunkSize = %d, want 25", cfg.chunkSize)
	}
}

func TestParseArgsRejectsUnknownCommand(t *testing.T) {
	_, err := parseArgs([]string{"unknown"})
	if err == nil {
		t.Fatal("parseArgs returned nil error, want error")
	}
}

package main

import "testing"

func TestParseArgsRunCommand(t *testing.T) {
	cfg, err := parseArgs([]string{"run", "--env", ".env", "--data-dir", "/tmp/sec", "--chunk-size", "250", "--limit", "100"})
	if err != nil {
		t.Fatalf("parseArgs returned error: %v", err)
	}

	if cfg.command != "run" {
		t.Fatalf("command = %q, want run", cfg.command)
	}
	if cfg.envPath != ".env" {
		t.Fatalf("envPath = %q, want .env", cfg.envPath)
	}
	if cfg.dataDir != "/tmp/sec" {
		t.Fatalf("dataDir = %q, want /tmp/sec", cfg.dataDir)
	}
	if cfg.chunkSize != 250 {
		t.Fatalf("chunkSize = %d, want 250", cfg.chunkSize)
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

package main

import (
	"testing"
	"time"
)

func TestParseConfigReadsUSSECEDGARSyncFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--env", "../.env.test",
		"--database-url", "postgres://example",
		"--download-url", "https://example.test/company_tickers.json",
		"--data-dir", "/tmp/sec",
		"--user-agent", "corpscout/1.0 (test@example.com)",
		"--chunk-size", "25",
		"--limit", "10",
		"--request-timeout", "2s",
		"--timeout", "1m",
	})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}

	if cfg.DatabaseURL != "postgres://example" {
		t.Fatalf("DatabaseURL = %q, want postgres://example", cfg.DatabaseURL)
	}
	if cfg.DownloadURL != "https://example.test/company_tickers.json" {
		t.Fatalf("DownloadURL = %q", cfg.DownloadURL)
	}
	if cfg.DataDir != "/tmp/sec" {
		t.Fatalf("DataDir = %q, want /tmp/sec", cfg.DataDir)
	}
	if cfg.UserAgent != "corpscout/1.0 (test@example.com)" {
		t.Fatalf("UserAgent = %q", cfg.UserAgent)
	}
	if cfg.ChunkSize != 25 || cfg.Limit != 10 {
		t.Fatalf("chunk/limit config = %d/%d, want 25/10", cfg.ChunkSize, cfg.Limit)
	}
	if cfg.RequestTimeout != 2*time.Second {
		t.Fatalf("RequestTimeout = %s, want 2s", cfg.RequestTimeout)
	}
	if cfg.Timeout != time.Minute {
		t.Fatalf("Timeout = %s, want 1m", cfg.Timeout)
	}
}

func TestConfigWithEnvDefaultsResolvesUSSECEDGARSettings(t *testing.T) {
	values := map[string]string{
		"CORPSCOUT_DATABASE_URL":            "postgres://env",
		"SEC_EDGAR_DOWNLOAD_URL":            "https://env.test/company_tickers.json",
		"SEC_EDGAR_DATA_DIR":                "/tmp/env-sec",
		"SEC_EDGAR_USER_AGENT":              "env-agent (env@example.com)",
		"SEC_EDGAR_REQUEST_TIMEOUT_SECONDS": "7",
	}
	cfg := config{}.withEnvDefaults(func(key string) string {
		return values[key]
	})

	if cfg.DatabaseURL != "postgres://env" {
		t.Fatalf("DatabaseURL = %q, want postgres://env", cfg.DatabaseURL)
	}
	if cfg.DownloadURL != "https://env.test/company_tickers.json" {
		t.Fatalf("DownloadURL = %q, want https://env.test/company_tickers.json", cfg.DownloadURL)
	}
	if cfg.DataDir != "/tmp/env-sec" {
		t.Fatalf("DataDir = %q, want /tmp/env-sec", cfg.DataDir)
	}
	if cfg.UserAgent != "env-agent (env@example.com)" {
		t.Fatalf("UserAgent = %q, want env-agent (env@example.com)", cfg.UserAgent)
	}
	if cfg.RequestTimeout != 7*time.Second {
		t.Fatalf("RequestTimeout = %s, want 7s", cfg.RequestTimeout)
	}
}

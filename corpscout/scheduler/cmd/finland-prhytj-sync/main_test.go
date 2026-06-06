package main

import (
	"testing"
	"time"
)

func TestParseConfigReadsFinlandPRHYTJSyncFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--env", "../.env.test",
		"--database-url", "postgres://example",
		"--base-url", "https://example.test/companies",
		"--data-dir", "/tmp/prh",
		"--user-agent", "corpscout-test",
		"--max-pages", "2",
		"--chunk-size", "25",
		"--page-delay", "5ms",
		"--request-timeout", "2s",
		"--timeout", "1m",
	})
	if err != nil {
		t.Fatalf("parseConfig returned error: %v", err)
	}

	if cfg.EnvFile != "../.env.test" {
		t.Fatalf("EnvFile = %q, want ../.env.test", cfg.EnvFile)
	}
	if cfg.DatabaseURL != "postgres://example" {
		t.Fatalf("DatabaseURL = %q, want postgres://example", cfg.DatabaseURL)
	}
	if cfg.BaseURL != "https://example.test/companies" {
		t.Fatalf("BaseURL = %q, want https://example.test/companies", cfg.BaseURL)
	}
	if cfg.DataDir != "/tmp/prh" {
		t.Fatalf("DataDir = %q, want /tmp/prh", cfg.DataDir)
	}
	if cfg.UserAgent != "corpscout-test" {
		t.Fatalf("UserAgent = %q, want corpscout-test", cfg.UserAgent)
	}
	if cfg.MaxPages != 2 || cfg.ChunkSize != 25 {
		t.Fatalf("page/chunk config = %d/%d, want 2/25", cfg.MaxPages, cfg.ChunkSize)
	}
	if cfg.PageDelay != 5*time.Millisecond {
		t.Fatalf("PageDelay = %s, want 5ms", cfg.PageDelay)
	}
	if cfg.RequestTimeout != 2*time.Second {
		t.Fatalf("RequestTimeout = %s, want 2s", cfg.RequestTimeout)
	}
	if cfg.Timeout != time.Minute {
		t.Fatalf("Timeout = %s, want 1m", cfg.Timeout)
	}
}

func TestConfigWithEnvDefaultsResolvesDatabaseAndSourceSettings(t *testing.T) {
	values := map[string]string{
		"CORPSCOUT_DATABASE_URL":          "postgres://env",
		"PRH_YTJ_BASE_URL":                "https://env.test/companies",
		"PRH_YTJ_DATA_DIR":                "/tmp/env-prh",
		"PRH_YTJ_USER_AGENT":              "env-agent",
		"PRH_YTJ_PAGE_DELAY_MS":           "250",
		"PRH_YTJ_REQUEST_TIMEOUT_SECONDS": "7",
	}
	cfg := config{}.withEnvDefaults(func(key string) string {
		return values[key]
	})

	if cfg.DatabaseURL != "postgres://env" {
		t.Fatalf("DatabaseURL = %q, want postgres://env", cfg.DatabaseURL)
	}
	if cfg.BaseURL != "https://env.test/companies" {
		t.Fatalf("BaseURL = %q, want https://env.test/companies", cfg.BaseURL)
	}
	if cfg.DataDir != "/tmp/env-prh" {
		t.Fatalf("DataDir = %q, want /tmp/env-prh", cfg.DataDir)
	}
	if cfg.UserAgent != "env-agent" {
		t.Fatalf("UserAgent = %q, want env-agent", cfg.UserAgent)
	}
	if cfg.PageDelay != 250*time.Millisecond {
		t.Fatalf("PageDelay = %s, want 250ms", cfg.PageDelay)
	}
	if cfg.RequestTimeout != 7*time.Second {
		t.Fatalf("RequestTimeout = %s, want 7s", cfg.RequestTimeout)
	}
}

package main

import (
	"testing"
	"time"
)

func TestParseConfigReadsUSColoradoEntitiesSyncFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--env", "../.env.test",
		"--database-url", "postgres://example",
		"--base-url", "https://example.test/resource.json",
		"--app-token", "tok",
		"--data-dir", "/tmp/co",
		"--user-agent", "corpscout-test",
		"--page-size", "500",
		"--max-pages", "2",
		"--chunk-size", "25",
		"--limit", "10",
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
	if cfg.BaseURL != "https://example.test/resource.json" {
		t.Fatalf("BaseURL = %q, want https://example.test/resource.json", cfg.BaseURL)
	}
	if cfg.AppToken != "tok" {
		t.Fatalf("AppToken = %q, want tok", cfg.AppToken)
	}
	if cfg.DataDir != "/tmp/co" {
		t.Fatalf("DataDir = %q, want /tmp/co", cfg.DataDir)
	}
	if cfg.UserAgent != "corpscout-test" {
		t.Fatalf("UserAgent = %q, want corpscout-test", cfg.UserAgent)
	}
	if cfg.PageSize != 500 || cfg.MaxPages != 2 || cfg.ChunkSize != 25 || cfg.Limit != 10 {
		t.Fatalf("page/chunk/limit config = %d/%d/%d/%d, want 500/2/25/10", cfg.PageSize, cfg.MaxPages, cfg.ChunkSize, cfg.Limit)
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

func TestConfigWithEnvDefaultsResolvesUSColoradoEntitiesSettings(t *testing.T) {
	values := map[string]string{
		"CORPSCOUT_DATABASE_URL":                             "postgres://env",
		"COLORADO_BUSINESS_ENTITIES_BASE_URL":                "https://env.test/resource.json",
		"COLORADO_BUSINESS_ENTITIES_APP_TOKEN":               "env-token",
		"COLORADO_BUSINESS_ENTITIES_DATA_DIR":                "/tmp/env-co",
		"COLORADO_BUSINESS_ENTITIES_USER_AGENT":              "env-agent",
		"COLORADO_BUSINESS_ENTITIES_PAGE_SIZE":               "750",
		"COLORADO_BUSINESS_ENTITIES_PAGE_DELAY_MS":           "250",
		"COLORADO_BUSINESS_ENTITIES_REQUEST_TIMEOUT_SECONDS": "7",
	}
	cfg := config{}.withEnvDefaults(func(key string) string {
		return values[key]
	})

	if cfg.DatabaseURL != "postgres://env" {
		t.Fatalf("DatabaseURL = %q, want postgres://env", cfg.DatabaseURL)
	}
	if cfg.BaseURL != "https://env.test/resource.json" {
		t.Fatalf("BaseURL = %q, want https://env.test/resource.json", cfg.BaseURL)
	}
	if cfg.AppToken != "env-token" {
		t.Fatalf("AppToken = %q, want env-token", cfg.AppToken)
	}
	if cfg.DataDir != "/tmp/env-co" {
		t.Fatalf("DataDir = %q, want /tmp/env-co", cfg.DataDir)
	}
	if cfg.UserAgent != "env-agent" {
		t.Fatalf("UserAgent = %q, want env-agent", cfg.UserAgent)
	}
	if cfg.PageSize != 750 {
		t.Fatalf("PageSize = %d, want 750", cfg.PageSize)
	}
	if cfg.PageDelay != 250*time.Millisecond {
		t.Fatalf("PageDelay = %s, want 250ms", cfg.PageDelay)
	}
	if cfg.RequestTimeout != 7*time.Second {
		t.Fatalf("RequestTimeout = %s, want 7s", cfg.RequestTimeout)
	}
}

package main

import (
	"testing"
	"time"
)

func TestParseConfigReadsUSSamGovEntitySyncFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--env", "../.env.test",
		"--database-url", "postgres://example",
		"--base-url", "https://example.test/entities",
		"--sam-registered", "Yes",
		"--data-dir", "/tmp/sam",
		"--user-agent", "corpscout-test",
		"--page-size", "10",
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

	if cfg.DatabaseURL != "postgres://example" {
		t.Fatalf("DatabaseURL = %q, want postgres://example", cfg.DatabaseURL)
	}
	if cfg.BaseURL != "https://example.test/entities" {
		t.Fatalf("BaseURL = %q, want https://example.test/entities", cfg.BaseURL)
	}
	if cfg.SamRegistered != "Yes" {
		t.Fatalf("SamRegistered = %q, want Yes", cfg.SamRegistered)
	}
	if cfg.DataDir != "/tmp/sam" {
		t.Fatalf("DataDir = %q, want /tmp/sam", cfg.DataDir)
	}
	if cfg.PageSize != 10 || cfg.MaxPages != 2 || cfg.ChunkSize != 25 || cfg.Limit != 10 {
		t.Fatalf("page/chunk/limit config = %d/%d/%d/%d, want 10/2/25/10", cfg.PageSize, cfg.MaxPages, cfg.ChunkSize, cfg.Limit)
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

func TestConfigWithEnvDefaultsResolvesUSSamGovEntitySettings(t *testing.T) {
	values := map[string]string{
		"CORPSCOUT_DATABASE_URL":                 "postgres://env",
		"SAM_GOV_ENTITY_BASE_URL":                "https://env.test/entities",
		"SAM_GOV_ENTITY_SAM_REGISTERED":          "Yes",
		"SAM_GOV_ENTITY_DATA_DIR":                "/tmp/env-sam",
		"SAM_GOV_ENTITY_USER_AGENT":              "env-agent",
		"SAM_GOV_ENTITY_PAGE_SIZE":               "10",
		"SAM_GOV_ENTITY_PAGE_DELAY_MS":           "250",
		"SAM_GOV_ENTITY_REQUEST_TIMEOUT_SECONDS": "7",
	}
	cfg := config{}.withEnvDefaults(func(key string) string {
		return values[key]
	})

	if cfg.DatabaseURL != "postgres://env" {
		t.Fatalf("DatabaseURL = %q, want postgres://env", cfg.DatabaseURL)
	}
	if cfg.BaseURL != "https://env.test/entities" {
		t.Fatalf("BaseURL = %q, want https://env.test/entities", cfg.BaseURL)
	}
	if cfg.SamRegistered != "Yes" {
		t.Fatalf("SamRegistered = %q, want Yes", cfg.SamRegistered)
	}
	if cfg.DataDir != "/tmp/env-sam" {
		t.Fatalf("DataDir = %q, want /tmp/env-sam", cfg.DataDir)
	}
	if cfg.UserAgent != "env-agent" {
		t.Fatalf("UserAgent = %q, want env-agent", cfg.UserAgent)
	}
	if cfg.PageSize != 10 {
		t.Fatalf("PageSize = %d, want 10", cfg.PageSize)
	}
	if cfg.PageDelay != 250*time.Millisecond {
		t.Fatalf("PageDelay = %s, want 250ms", cfg.PageDelay)
	}
	if cfg.RequestTimeout != 7*time.Second {
		t.Fatalf("RequestTimeout = %s, want 7s", cfg.RequestTimeout)
	}
}

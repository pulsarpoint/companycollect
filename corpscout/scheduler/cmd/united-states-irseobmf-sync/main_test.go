package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseConfigReadsUSIRSEoBmfSyncFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"--env", "../.env.test",
		"--database-url", "postgres://example",
		"--download-urls", "https://example.test/eo1.csv,https://example.test/eo2.csv",
		"--data-dir", "/tmp/irs",
		"--user-agent", "corpscout-test",
		"--max-files", "2",
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
	if cfg.DownloadURLs != "https://example.test/eo1.csv,https://example.test/eo2.csv" {
		t.Fatalf("DownloadURLs = %q", cfg.DownloadURLs)
	}
	if cfg.DataDir != "/tmp/irs" {
		t.Fatalf("DataDir = %q, want /tmp/irs", cfg.DataDir)
	}
	if cfg.UserAgent != "corpscout-test" {
		t.Fatalf("UserAgent = %q, want corpscout-test", cfg.UserAgent)
	}
	if cfg.MaxFiles != 2 || cfg.ChunkSize != 25 || cfg.Limit != 10 {
		t.Fatalf("files/chunk/limit config = %d/%d/%d, want 2/25/10", cfg.MaxFiles, cfg.ChunkSize, cfg.Limit)
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

func TestSplitURLsParsesCommaSeparatedList(t *testing.T) {
	got := splitURLs(" https://a.test/eo1.csv , https://a.test/eo2.csv ,, ")
	want := []string{"https://a.test/eo1.csv", "https://a.test/eo2.csv"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("splitURLs = %#v, want %#v", got, want)
	}
	if splitURLs("   ") != nil {
		t.Fatalf("splitURLs(blank) = %#v, want nil", splitURLs("   "))
	}
}

func TestConfigWithEnvDefaultsResolvesUSIRSEoBmfSettings(t *testing.T) {
	values := map[string]string{
		"CORPSCOUT_DATABASE_URL":             "postgres://env",
		"IRS_EO_BMF_DOWNLOAD_URLS":           "https://env.test/eo1.csv",
		"IRS_EO_BMF_DATA_DIR":                "/tmp/env-irs",
		"IRS_EO_BMF_USER_AGENT":              "env-agent",
		"IRS_EO_BMF_PAGE_DELAY_MS":           "250",
		"IRS_EO_BMF_REQUEST_TIMEOUT_SECONDS": "7",
	}
	cfg := config{}.withEnvDefaults(func(key string) string {
		return values[key]
	})

	if cfg.DatabaseURL != "postgres://env" {
		t.Fatalf("DatabaseURL = %q, want postgres://env", cfg.DatabaseURL)
	}
	if cfg.DownloadURLs != "https://env.test/eo1.csv" {
		t.Fatalf("DownloadURLs = %q, want https://env.test/eo1.csv", cfg.DownloadURLs)
	}
	if cfg.DataDir != "/tmp/env-irs" {
		t.Fatalf("DataDir = %q, want /tmp/env-irs", cfg.DataDir)
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

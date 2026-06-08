package secedgar

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestConfigFromEnvUsesDefaults(t *testing.T) {
	t.Setenv("USA_SEC_EDGAR_DATA_DIR", "")
	t.Setenv("USA_SEC_EDGAR_DOWNLOAD_URL", "")
	t.Setenv("USA_SEC_EDGAR_USER_AGENT", "")
	t.Setenv("USA_SEC_EDGAR_REQUEST_TIMEOUT", "")

	cfg := ConfigFromEnv()

	wantDataDir := filepath.Join("..", "data", "united_states", "countrydata", "sources", "secedgar")
	if cfg.DataDir != wantDataDir {
		t.Fatalf("DataDir = %q, want %q", cfg.DataDir, wantDataDir)
	}
	if cfg.DownloadURL != DefaultDownloadURL {
		t.Fatalf("DownloadURL = %q, want %q", cfg.DownloadURL, DefaultDownloadURL)
	}
	if cfg.UserAgent != DefaultUserAgent {
		t.Fatalf("UserAgent = %q, want %q", cfg.UserAgent, DefaultUserAgent)
	}
	if cfg.RequestTimeout != countryimport.DefaultRequestTimeout {
		t.Fatalf("RequestTimeout = %s, want %s", cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	}
	if cfg.SourceSlug != SourceSlug {
		t.Fatalf("SourceSlug = %q, want %q", cfg.SourceSlug, SourceSlug)
	}
}

func TestConfigFromEnvUsesOverrides(t *testing.T) {
	t.Setenv("USA_SEC_EDGAR_DATA_DIR", "/tmp/sec")
	t.Setenv("USA_SEC_EDGAR_DOWNLOAD_URL", "https://example.test/company_tickers.json")
	t.Setenv("USA_SEC_EDGAR_USER_AGENT", "test-agent")
	t.Setenv("USA_SEC_EDGAR_REQUEST_TIMEOUT", "15s")

	cfg := ConfigFromEnv()

	if cfg.DataDir != "/tmp/sec" {
		t.Fatalf("DataDir = %q, want override", cfg.DataDir)
	}
	if cfg.DownloadURL != "https://example.test/company_tickers.json" {
		t.Fatalf("DownloadURL = %q, want override", cfg.DownloadURL)
	}
	if cfg.UserAgent != "test-agent" {
		t.Fatalf("UserAgent = %q, want override", cfg.UserAgent)
	}
	if cfg.RequestTimeout != 15*time.Second {
		t.Fatalf("RequestTimeout = %s, want 15s", cfg.RequestTimeout)
	}
}

func TestNewSourceAppliesDefaults(t *testing.T) {
	client := &http.Client{}
	source := NewSource(Config{HTTPClient: client})

	if source.cfg.SourceSlug != SourceSlug {
		t.Fatalf("source slug = %q, want %q", source.cfg.SourceSlug, SourceSlug)
	}
	if source.httpClient != client {
		t.Fatal("NewSource did not keep configured HTTP client")
	}
	if source.metadataStore == nil {
		t.Fatal("metadataStore is nil")
	}
}

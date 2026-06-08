package coloradoentities

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestConfigFromEnvUsesDefaults(t *testing.T) {
	t.Setenv("COLORADO_BUSINESS_ENTITIES_BASE_URL", "")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_DATA_DIR", "")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_PAGE_SIZE", "")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_USER_AGENT", "")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_APP_TOKEN", "")

	cfg := ConfigFromEnv()

	wantDataDir := filepath.Join("..", "data", "united_states", "countrydata", "sources", "coloradoentities")
	if cfg.DataDir != wantDataDir {
		t.Fatalf("DataDir = %q, want %q", cfg.DataDir, wantDataDir)
	}
	if cfg.BaseURL != DefaultBaseURL {
		t.Fatalf("BaseURL = %q, want %q", cfg.BaseURL, DefaultBaseURL)
	}
	if cfg.PageSize != DefaultPageSize {
		t.Fatalf("PageSize = %d, want %d", cfg.PageSize, DefaultPageSize)
	}
	if cfg.UserAgent != countryimport.DefaultUserAgent {
		t.Fatalf("UserAgent = %q, want %q", cfg.UserAgent, countryimport.DefaultUserAgent)
	}
	if cfg.AppToken != "" {
		t.Fatalf("AppToken = %q, want empty by default", cfg.AppToken)
	}
}

func TestConfigFromEnvUsesOverrides(t *testing.T) {
	t.Setenv("COLORADO_BUSINESS_ENTITIES_BASE_URL", "https://example.test/resource.json")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_DATA_DIR", "/tmp/co")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_PAGE_SIZE", "250")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_USER_AGENT", "test-agent")
	t.Setenv("COLORADO_BUSINESS_ENTITIES_APP_TOKEN", "secret-token")

	cfg := ConfigFromEnv()

	if cfg.BaseURL != "https://example.test/resource.json" {
		t.Fatalf("BaseURL = %q, want override", cfg.BaseURL)
	}
	if cfg.DataDir != "/tmp/co" {
		t.Fatalf("DataDir = %q, want override", cfg.DataDir)
	}
	if cfg.PageSize != 250 {
		t.Fatalf("PageSize = %d, want 250", cfg.PageSize)
	}
	if cfg.UserAgent != "test-agent" {
		t.Fatalf("UserAgent = %q, want override", cfg.UserAgent)
	}
	if cfg.AppToken != "secret-token" {
		t.Fatalf("AppToken = %q, want override", cfg.AppToken)
	}
}

func TestNewSourceAppliesDefaults(t *testing.T) {
	client := &http.Client{}
	source := NewSource(Config{HTTPClient: client})

	if source.cfg.BaseURL != DefaultBaseURL {
		t.Fatalf("BaseURL = %q, want %q", source.cfg.BaseURL, DefaultBaseURL)
	}
	if source.cfg.PageSize != DefaultPageSize {
		t.Fatalf("PageSize = %d, want %d", source.cfg.PageSize, DefaultPageSize)
	}
	if source.cfg.PageDelay != countryimport.DefaultPageDelay {
		t.Fatalf("PageDelay = %s, want %s", source.cfg.PageDelay, countryimport.DefaultPageDelay)
	}
	if source.httpClient != client {
		t.Fatal("NewSource did not keep configured HTTP client")
	}
	if source.metadataStore == nil {
		t.Fatal("metadataStore is nil")
	}
}

func TestConfigDefaultTimeouts(t *testing.T) {
	source := NewSource(Config{})
	if source.cfg.RequestTimeout != countryimport.DefaultRequestTimeout {
		t.Fatalf("RequestTimeout = %s, want %s", source.cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	}
	_ = time.Second
}

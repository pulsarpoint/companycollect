package irseobmf

import (
	"net/http"
	"path/filepath"
	"testing"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func TestConfigFromEnvUsesDefaults(t *testing.T) {
	t.Setenv("IRS_EO_BMF_BASE_URL", "")
	t.Setenv("IRS_EO_BMF_FILES", "")
	t.Setenv("IRS_EO_BMF_DATA_DIR", "")
	t.Setenv("IRS_EO_BMF_USER_AGENT", "")
	t.Setenv("IRS_EO_BMF_REQUEST_TIMEOUT", "")

	cfg := ConfigFromEnv()

	wantDataDir := filepath.Join("..", "data", "united_states", "countrydata", "sources", "irseobmf")
	if cfg.DataDir != wantDataDir {
		t.Fatalf("DataDir = %q, want %q", cfg.DataDir, wantDataDir)
	}
	if cfg.BaseURL != DefaultBaseURL {
		t.Fatalf("BaseURL = %q, want %q", cfg.BaseURL, DefaultBaseURL)
	}
	if len(cfg.Files) != len(DefaultFiles) {
		t.Fatalf("Files = %#v, want %#v", cfg.Files, DefaultFiles)
	}
	if cfg.UserAgent != countryimport.DefaultUserAgent {
		t.Fatalf("UserAgent = %q, want %q", cfg.UserAgent, countryimport.DefaultUserAgent)
	}
	if cfg.RequestTimeout != countryimport.DefaultRequestTimeout {
		t.Fatalf("RequestTimeout = %s, want %s", cfg.RequestTimeout, countryimport.DefaultRequestTimeout)
	}
}

func TestConfigFromEnvUsesOverrides(t *testing.T) {
	t.Setenv("IRS_EO_BMF_BASE_URL", "https://example.test/irs/")
	t.Setenv("IRS_EO_BMF_FILES", "eo1.csv, eo2.csv")
	t.Setenv("IRS_EO_BMF_DATA_DIR", "/tmp/irs")
	t.Setenv("IRS_EO_BMF_USER_AGENT", "test-agent")
	t.Setenv("IRS_EO_BMF_REQUEST_TIMEOUT", "15s")

	cfg := ConfigFromEnv()

	if cfg.BaseURL != "https://example.test/irs/" {
		t.Fatalf("BaseURL = %q, want override", cfg.BaseURL)
	}
	if len(cfg.Files) != 2 || cfg.Files[0] != "eo1.csv" || cfg.Files[1] != "eo2.csv" {
		t.Fatalf("Files = %#v, want [eo1.csv eo2.csv]", cfg.Files)
	}
	if cfg.DataDir != "/tmp/irs" {
		t.Fatalf("DataDir = %q, want override", cfg.DataDir)
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

	if source.cfg.BaseURL != DefaultBaseURL {
		t.Fatalf("BaseURL = %q, want %q", source.cfg.BaseURL, DefaultBaseURL)
	}
	if len(source.cfg.Files) != len(DefaultFiles) {
		t.Fatalf("Files = %#v, want defaults", source.cfg.Files)
	}
	if source.httpClient != client {
		t.Fatal("NewSource did not keep configured HTTP client")
	}
	if source.metadataStore == nil {
		t.Fatal("metadataStore is nil")
	}
}

package secedgar

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

const defaultDataDir = "./data/countrydata/united_states/secedgar"

// Config configures the SEC EDGAR ticker-map source. SEC rejects requests
// without a descriptive User-Agent that includes a contact email (HTTP 403), so
// set SEC_EDGAR_USER_AGENT to something like
// "corpscout/1.0 (contact@example.com)" before any live download.
type Config struct {
	DownloadURL    string
	DataDir        string
	RequestTimeout time.Duration
	UserAgent      string
	HTTPClient     *http.Client
	MetadataStore  countryimport.MetadataStore
}

func ConfigFromEnv() Config {
	return Config{
		DownloadURL:    envOr("SEC_EDGAR_DOWNLOAD_URL", DefaultDownloadURL),
		DataDir:        envOr("SEC_EDGAR_DATA_DIR", defaultDataDir),
		RequestTimeout: envDurationSeconds("SEC_EDGAR_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("SEC_EDGAR_USER_AGENT", countryimport.DefaultUserAgent),
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Second
}

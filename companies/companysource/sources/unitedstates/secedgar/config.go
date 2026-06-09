package secedgar

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type Config struct {
	DownloadURL    string
	UserAgent      string
	RequestTimeout time.Duration
	SourceSlug     string
	HTTPClient     *http.Client
}

func ConfigFromEnv() Config {
	return Config{
		DownloadURL:    envOr("USA_SEC_EDGAR_DOWNLOAD_URL", DefaultDownloadURL),
		UserAgent:      envOr("USA_SEC_EDGAR_USER_AGENT", DefaultUserAgent),
		RequestTimeout: envDuration("USA_SEC_EDGAR_REQUEST_TIMEOUT", countryimport.DefaultRequestTimeout),
		SourceSlug:     SourceSlug,
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}

	duration, err := time.ParseDuration(value)
	if err == nil && duration > 0 {
		return duration
	}

	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}

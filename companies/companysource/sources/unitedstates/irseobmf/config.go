package irseobmf

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
)

// Config holds IRS EO BMF source configuration. No credentials are required;
// the EO BMF extracts are public U.S. Government works.
type Config struct {
	BaseURL        string
	Files          []string
	RequestTimeout time.Duration
	UserAgent      string
	HTTPClient     *http.Client
}

// ConfigFromEnv builds a Config from IRS_EO_BMF_* environment variables, falling
// back to public defaults.
func ConfigFromEnv() Config {
	return Config{
		BaseURL:        envOr("IRS_EO_BMF_BASE_URL", DefaultBaseURL),
		Files:          envFiles("IRS_EO_BMF_FILES", DefaultFiles),
		RequestTimeout: envDuration("IRS_EO_BMF_REQUEST_TIMEOUT", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("IRS_EO_BMF_USER_AGENT", countryimport.DefaultUserAgent),
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envFiles(key string, fallback []string) []string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parts := strings.Split(value, ",")
	files := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			files = append(files, trimmed)
		}
	}
	if len(files) == 0 {
		return fallback
	}
	return files
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

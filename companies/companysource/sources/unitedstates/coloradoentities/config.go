package coloradoentities

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/companysource/common/countryimport"
)

// Config holds Colorado Business Entities source configuration. The Socrata app
// token is optional (it raises rate limits) and is never required.
type Config struct {
	BaseURL        string
	PageSize       int
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	AppToken       string
	HTTPClient     *http.Client
}

// ConfigFromEnv builds a Config from COLORADO_BUSINESS_ENTITIES_* environment
// variables, falling back to public defaults.
func ConfigFromEnv() Config {
	return Config{
		BaseURL:        envOr("COLORADO_BUSINESS_ENTITIES_BASE_URL", DefaultBaseURL),
		PageSize:       envInt("COLORADO_BUSINESS_ENTITIES_PAGE_SIZE", DefaultPageSize),
		PageDelay:      envDurationMillis("COLORADO_BUSINESS_ENTITIES_PAGE_DELAY_MS", countryimport.DefaultPageDelay),
		RequestTimeout: envDurationSeconds("COLORADO_BUSINESS_ENTITIES_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("COLORADO_BUSINESS_ENTITIES_USER_AGENT", countryimport.DefaultUserAgent),
		AppToken:       strings.TrimSpace(os.Getenv("COLORADO_BUSINESS_ENTITIES_APP_TOKEN")),
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envDurationMillis(key string, fallback time.Duration) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Millisecond
}

func envDurationSeconds(key string, fallback time.Duration) time.Duration {
	value, err := strconv.Atoi(strings.TrimSpace(os.Getenv(key)))
	if err != nil || value <= 0 {
		return fallback
	}
	return time.Duration(value) * time.Second
}

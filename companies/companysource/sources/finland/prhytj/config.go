package prhytj

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

type Config struct {
	BaseURL        string
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	HTTPClient     *http.Client
}

func ConfigFromEnv() Config {
	return Config{
		BaseURL:        envOr("PRH_YTJ_BASE_URL", DefaultBaseURL),
		PageDelay:      envDurationMillis("PRH_YTJ_PAGE_DELAY_MS", countryimport.DefaultPageDelay),
		RequestTimeout: envDurationSeconds("PRH_YTJ_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("PRH_YTJ_USER_AGENT", countryimport.DefaultUserAgent),
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
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

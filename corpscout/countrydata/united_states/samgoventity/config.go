package samgoventity

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

const defaultDataDir = "./data/countrydata/united_states/samgoventity"

// Config configures the SAM.gov Entity source. APIKey is REQUIRED and is sent
// via the X-Api-Key header (never placed in a URL, error, or metadata). Use only
// a "Read Public" key: this importer consumes the FOIA-releasable Public extract
// and must never request FOUO/Sensitive sensitivity levels.
type Config struct {
	BaseURL        string
	APIKey         string
	SamRegistered  string
	DataDir        string
	PageSize       int
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	HTTPClient     *http.Client
	MetadataStore  countryimport.MetadataStore
}

func ConfigFromEnv() Config {
	return Config{
		BaseURL:        envOr("SAM_GOV_ENTITY_BASE_URL", DefaultBaseURL),
		APIKey:         strings.TrimSpace(os.Getenv("SAM_GOV_ENTITY_API_KEY")),
		SamRegistered:  envOr("SAM_GOV_ENTITY_SAM_REGISTERED", DefaultSamRegistered),
		DataDir:        envOr("SAM_GOV_ENTITY_DATA_DIR", defaultDataDir),
		PageSize:       envInt("SAM_GOV_ENTITY_PAGE_SIZE", DefaultPageSize),
		PageDelay:      envDurationMillis("SAM_GOV_ENTITY_PAGE_DELAY_MS", countryimport.DefaultPageDelay),
		RequestTimeout: envDurationSeconds("SAM_GOV_ENTITY_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("SAM_GOV_ENTITY_USER_AGENT", countryimport.DefaultUserAgent),
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

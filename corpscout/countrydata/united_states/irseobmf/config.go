package irseobmf

import (
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

const defaultDataDir = "./data/countrydata/united_states/irseobmf"

// Config configures the IRS EO BMF source. The extract is anonymous public-domain
// CSV, so no credentials are needed; PageDelay paces requests between the regional
// files.
type Config struct {
	DownloadURLs   []string
	DataDir        string
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	HTTPClient     *http.Client
	MetadataStore  countryimport.MetadataStore
}

func ConfigFromEnv() Config {
	return Config{
		DownloadURLs:   envURLs("IRS_EO_BMF_DOWNLOAD_URLS", DefaultDownloadURLs),
		DataDir:        envOr("IRS_EO_BMF_DATA_DIR", defaultDataDir),
		PageDelay:      envDurationMillis("IRS_EO_BMF_PAGE_DELAY_MS", countryimport.DefaultPageDelay),
		RequestTimeout: envDurationSeconds("IRS_EO_BMF_REQUEST_TIMEOUT_SECONDS", countryimport.DefaultRequestTimeout),
		UserAgent:      envOr("IRS_EO_BMF_USER_AGENT", countryimport.DefaultUserAgent),
	}
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

// envURLs parses a comma-separated list of URLs, falling back to defaults when
// unset. A copy of the fallback is returned so callers never mutate the package
// default slice.
func envURLs(key string, fallback []string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return append([]string(nil), fallback...)
	}
	urls := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			urls = append(urls, trimmed)
		}
	}
	if len(urls) == 0 {
		return append([]string(nil), fallback...)
	}
	return urls
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

package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

const defaultNATSRequestTimeout = 180 * time.Second
const defaultFXECBDailyURL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
const defaultAriregisterBulkSourceURL = "https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip"
const defaultCVRDistributionSourceURL = "https://distribution.virk.dk/cvr-permanent/virksomhed/_search"
const defaultCVRDistributionScrollURL = "https://distribution.virk.dk/_search/scroll"
const defaultCVRDistributionScroll = "1m"
const defaultFranceLegalUnitsSourceURL = "https://www.data.gouv.fr/api/1/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3"
const defaultFranceEstablishmentsSourceURL = "https://www.data.gouv.fr/api/1/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f"

type Config struct {
	DatabaseURL             string
	ListenAddr              string
	PostgRESTURL            string
	S3Endpoint              string
	S3AccessKey             string
	S3SecretKey             string
	S3Bucket                string
	CrawlServiceURL         string
	NATSURL                 string
	NATSRequestTimeout      time.Duration
	TemporalHost            string
	TemporalUIURL           string
	LogLevel                string
	LLMProviderKey          string
	NACESourceURL           string
	BRREGBulkSourceURL      string
	BRREGFinancialURL       string
	AriregisterSourceURL    string
	CVRSourceURL            string
	CVRScrollURL            string
	CVRScroll               string
	CVRUsername             string
	CVRPassword             string
	CVRBearerToken          string
	CVRAPIKey               string
	FranceLegalUnitsURL     string
	FranceEstablishmentsURL string
	SEHVDDatasetsJSON       string
	FXECBSourceURL          string
}

func Load() (Config, error) {
	databaseURL, err := requireEnv("DATABASE_URL", "CORPSCOUT_DATABASE_URL")
	if err != nil {
		return Config{}, err
	}
	s3AccessKey, err := requireEnv("CORPSCOUT_S3_ACCESS_KEY")
	if err != nil {
		return Config{}, err
	}
	s3SecretKey, err := requireEnv("CORPSCOUT_S3_SECRET_KEY")
	if err != nil {
		return Config{}, err
	}
	logLevel, err := parseLogLevel(getEnv("CORPSCOUT_LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}
	natsRequestTimeout, err := parseSecondsEnv("CORPSCOUT_NATS_REQUEST_TIMEOUT_SECONDS", defaultNATSRequestTimeout)
	if err != nil {
		return Config{}, err
	}
	return Config{
		DatabaseURL:             databaseURL,
		ListenAddr:              getEnv("CORPSCOUT_LISTEN_ADDR", ":8090"),
		PostgRESTURL:            getEnv("CORPSCOUT_POSTGREST_URL", "http://localhost:3000"),
		S3Endpoint:              getEnv("CORPSCOUT_S3_ENDPOINT", "http://localhost:9000"),
		S3AccessKey:             s3AccessKey,
		S3SecretKey:             s3SecretKey,
		S3Bucket:                getEnv("CORPSCOUT_S3_BUCKET", "crawls"),
		CrawlServiceURL:         getEnv("CORPSCOUT_CRAWL_SERVICE_URL", "http://localhost:8096"),
		NATSURL:                 getEnv("CORPSCOUT_NATS_URL", ""),
		NATSRequestTimeout:      natsRequestTimeout,
		TemporalHost:            getEnv("CORPSCOUT_TEMPORAL_HOST", "localhost:7233"),
		TemporalUIURL:           getEnv("CORPSCOUT_TEMPORAL_UI_URL", "http://localhost:8089"),
		LogLevel:                logLevel,
		LLMProviderKey:          getEnv("CORPSCOUT_LLM_PROVIDER_KEY_ENCRYPTION_KEY", ""),
		NACESourceURL:           getEnv("CORPSCOUT_NACE_REV21_SOURCE_URL", ""),
		BRREGBulkSourceURL:      getEnv("CORPSCOUT_BRREG_BULK_SOURCE_URL", "https://data.brreg.no/enhetsregisteret/api/enheter/lastned"),
		BRREGFinancialURL:       getEnv("CORPSCOUT_BRREG_FINANCIAL_BASE_URL", "https://data.brreg.no"),
		AriregisterSourceURL:    getEnv("CORPSCOUT_ARIREGISTER_BULK_SOURCE_URL", defaultAriregisterBulkSourceURL),
		CVRSourceURL:            getEnv("CORPSCOUT_CVR_DISTRIBUTION_SOURCE_URL", defaultCVRDistributionSourceURL),
		CVRScrollURL:            getEnv("CORPSCOUT_CVR_DISTRIBUTION_SCROLL_URL", defaultCVRDistributionScrollURL),
		CVRScroll:               getEnv("CORPSCOUT_CVR_DISTRIBUTION_SCROLL", defaultCVRDistributionScroll),
		CVRUsername:             getEnv("CORPSCOUT_CVR_DISTRIBUTION_USERNAME", ""),
		CVRPassword:             getEnv("CORPSCOUT_CVR_DISTRIBUTION_PASSWORD", ""),
		CVRBearerToken:          getEnv("CORPSCOUT_CVR_DISTRIBUTION_BEARER_TOKEN", ""),
		CVRAPIKey:               getEnv("CORPSCOUT_CVR_DISTRIBUTION_API_KEY", ""),
		FranceLegalUnitsURL:     getEnv("CORPSCOUT_FRANCE_LEGAL_UNITS_SOURCE_URL", defaultFranceLegalUnitsSourceURL),
		FranceEstablishmentsURL: getEnv("CORPSCOUT_FRANCE_ESTABLISHMENTS_SOURCE_URL", defaultFranceEstablishmentsSourceURL),
		SEHVDDatasetsJSON:       getEnv("CORPSCOUT_SE_HVD_DATASETS_JSON", os.Getenv("SE_HVD_DATASETS_JSON")),
		FXECBSourceURL:          getEnv("CORPSCOUT_FX_ECB_DAILY_URL", defaultFXECBDailyURL),
	}, nil
}

func parseLogLevel(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "debug", "info", "warn", "error":
		return normalized, nil
	default:
		return "", errors.New("CORPSCOUT_LOG_LEVEL must be one of debug, info, warn, error")
	}
}

func requireEnv(keys ...string) (string, error) {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v, nil
		}
	}
	return "", errors.Newf("required env var not set: %s", keys[0])
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseSecondsEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 0, errors.Newf("%s must be a positive number of seconds", key)
	}
	return time.Duration(seconds) * time.Second, nil
}

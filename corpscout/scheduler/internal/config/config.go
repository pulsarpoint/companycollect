package config

import (
	"os"
	"strings"

	"github.com/cockroachdb/errors"
)

type Config struct {
	DatabaseURL     string
	ListenAddr      string
	PostgRESTURL    string
	S3Endpoint      string
	S3AccessKey     string
	S3SecretKey     string
	S3Bucket        string
	CrawlServiceURL string
	NATSURL         string
	TemporalHost    string
	TemporalUIURL   string
	LogLevel        string
	LLMProviderKey  string
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
	return Config{
		DatabaseURL:     databaseURL,
		ListenAddr:      getEnv("CORPSCOUT_LISTEN_ADDR", ":8090"),
		PostgRESTURL:    getEnv("CORPSCOUT_POSTGREST_URL", "http://localhost:3000"),
		S3Endpoint:      getEnv("CORPSCOUT_S3_ENDPOINT", "http://localhost:9000"),
		S3AccessKey:     s3AccessKey,
		S3SecretKey:     s3SecretKey,
		S3Bucket:        getEnv("CORPSCOUT_S3_BUCKET", "crawls"),
		CrawlServiceURL: getEnv("CORPSCOUT_CRAWL_SERVICE_URL", "http://localhost:8096"),
		NATSURL:         getEnv("CORPSCOUT_NATS_URL", ""),
		TemporalHost:    getEnv("CORPSCOUT_TEMPORAL_HOST", "localhost:7233"),
		TemporalUIURL:   getEnv("CORPSCOUT_TEMPORAL_UI_URL", "http://localhost:8089"),
		LogLevel:        logLevel,
		LLMProviderKey:  getEnv("CORPSCOUT_LLM_PROVIDER_KEY_ENCRYPTION_KEY", ""),
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

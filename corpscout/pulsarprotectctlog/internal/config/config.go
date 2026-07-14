// Package config loads the CT log ingestion service configuration from the
// environment, following the same caarlos0/env convention used elsewhere in
// the PulsarPoint monorepo.
package config

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config holds all runtime configuration for the ingestion service.
type Config struct {
	// Multi-source config.
	SourcesFile  string `env:"CTLOG_SOURCES_FILE" envDefault:"./sources.json"`
	ShardListURL string `env:"CTLOG_SHARD_LIST_URL" envDefault:"https://www.gstatic.com/ct/log_list/v3/all_logs_list.json"`

	// Fetch tuning.
	BatchSize     int           `env:"CTLOG_BATCH_SIZE" envDefault:"256"`
	FetchParallel int           `env:"CTLOG_FETCH_PARALLEL" envDefault:"4"`
	HTTPTimeout   time.Duration `env:"CTLOG_HTTP_TIMEOUT" envDefault:"30s"`
	MaxRetries    int           `env:"CTLOG_MAX_RETRIES" envDefault:"5"`

	// ClickHouse (data plane).
	ClickHouseAddr     string `env:"CTLOG_CLICKHOUSE_ADDR" envDefault:"localhost:9000"`
	ClickHouseDatabase string `env:"CTLOG_CLICKHOUSE_DATABASE" envDefault:"ctlog"`
	ClickHouseUser     string `env:"CTLOG_CLICKHOUSE_USER" envDefault:"default"`
	ClickHousePassword string `env:"CTLOG_CLICKHOUSE_PASSWORD"`
	ClickHouseStorage  string `env:"CTLOG_CLICKHOUSE_STORAGE_POLICY"` // optional storage policy for disk isolation

	// Control plane (SQLite cursor/work-unit store).
	ControlDBPath string `env:"CTLOG_CONTROL_DB_PATH" envDefault:"./data/ctlog-control.db"`

	// Writer batching.
	WriteBatchSize int `env:"CTLOG_WRITE_BATCH_SIZE" envDefault:"5000"`
}

// Load parses configuration from the environment, after loading a .env file in
// the working directory if present. Existing environment variables take
// precedence over .env values.
func Load() (*Config, error) {
	if err := loadDotEnv(".env"); err != nil {
		return nil, err
	}
	var c Config
	if err := env.Parse(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &c, nil
}

// loadDotEnv reads KEY=VALUE lines from path and sets any that are not already
// present in the environment. A missing file is not an error.
func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, val); err != nil {
				return fmt.Errorf("set %s: %w", key, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	return nil
}

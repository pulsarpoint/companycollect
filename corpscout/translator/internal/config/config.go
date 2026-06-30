package config

import (
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
)

const (
	DefaultConfigPath = "config/translator.json"

	ConfigFileEnv = "TRANSLATOR_CONFIG_FILE"
	APIAddrEnv    = "TRANSLATOR_API_ADDR"
)

type Config struct {
	Server     ServerConfig              `json:"server"`
	ClickHouse ClickHouseConfig          `json:"clickhouse"`
	Temporal   TemporalConfig            `json:"temporal"`
	Endpoints  map[string]EndpointConfig `json:"endpoints"`
	Sources    map[string]SourceConfig   `json:"sources"`
}

type ServerConfig struct {
	ListenAddress string `json:"listen_address"`
}

type ClickHouseConfig struct {
	Host       string `json:"host"`
	NativePort int    `json:"native_port"`
	User       string `json:"user"`
	Database   string `json:"database"`
	Secure     bool   `json:"secure"`

	Password  string `json:"-"`
	NativeURL string `json:"-"`
}

type TemporalConfig struct {
	Address          string `json:"address"`
	Namespace        string `json:"namespace"`
	BatchSize        int    `json:"batch_size"`
	TimeoutSeconds   int    `json:"timeout_seconds"`
	MaxBatchesPerRun int    `json:"max_batches_per_run"`
}

type EndpointConfig struct {
	Model         string           `json:"model"`
	ModelEnv      string           `json:"model_env"`
	BaseURL       string           `json:"base_url"`
	BaseURLEnv    string           `json:"base_url_env"`
	APIKeyEnv     string           `json:"api_key_env"`
	APIKeyDefault string           `json:"api_key_default"`
	MaxTokens     int              `json:"max_tokens"`
	ExtraBody     map[string]any   `json:"extra_body"`
	PromptData    PromptDataConfig `json:"prompt_data"`

	APIKey string `json:"-"`
}

type PromptDataConfig struct {
	SourceLanguage string `json:"source_language"`
	TargetLanguage string `json:"target_language"`
}

type SourceConfig struct {
	QueuePath  string `json:"queue_path"`
	EndpointID string `json:"endpoint_id"`
}

func LoadFromEnvironment() (Config, string, error) {
	path := os.Getenv(ConfigFileEnv)
	if path == "" {
		path = DefaultConfigPath
	}

	cfg, err := Load(path)
	if err != nil {
		return Config{}, path, err
	}
	return cfg, path, nil
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read translator config %q: %w", path, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse translator config %q: %w", path, err)
	}

	applyDefaults(&cfg)
	applyEnvironment(&cfg)

	return cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.ListenAddress == "" {
		cfg.Server.ListenAddress = ":8080"
	}
	if cfg.Temporal.Address == "" {
		cfg.Temporal.Address = "localhost:7233"
	}
	if cfg.Temporal.Namespace == "" {
		cfg.Temporal.Namespace = "default"
	}
	if cfg.Temporal.BatchSize <= 0 {
		cfg.Temporal.BatchSize = 50
	}
	if cfg.Temporal.TimeoutSeconds <= 0 {
		cfg.Temporal.TimeoutSeconds = 120
	}
	if cfg.Temporal.MaxBatchesPerRun <= 0 {
		cfg.Temporal.MaxBatchesPerRun = 500
	}
	if cfg.ClickHouse.Host == "" {
		cfg.ClickHouse.Host = "localhost"
	}
	if cfg.ClickHouse.NativePort <= 0 {
		cfg.ClickHouse.NativePort = 9000
	}
	if cfg.ClickHouse.User == "" {
		cfg.ClickHouse.User = "default"
	}
	if cfg.ClickHouse.Database == "" {
		cfg.ClickHouse.Database = "corpscout"
	}
	if cfg.Endpoints == nil {
		cfg.Endpoints = make(map[string]EndpointConfig)
	}
	if cfg.Sources == nil {
		cfg.Sources = make(map[string]SourceConfig)
	}
}

func applyEnvironment(cfg *Config) {
	if listenAddress := os.Getenv(APIAddrEnv); listenAddress != "" {
		cfg.Server.ListenAddress = listenAddress
	}

	applyClickHouseEnvironment(cfg)

	if temporalAddress := os.Getenv("TEMPORAL_ADDRESS"); temporalAddress != "" {
		cfg.Temporal.Address = temporalAddress
	}
	if maxBatches := os.Getenv("TRANSLATOR_MAX_BATCHES_PER_RUN"); maxBatches != "" {
		value, err := strconv.Atoi(maxBatches)
		if err == nil && value > 0 {
			cfg.Temporal.MaxBatchesPerRun = value
		}
	}

	for name, endpoint := range cfg.Endpoints {
		if endpoint.ModelEnv != "" {
			if model := os.Getenv(endpoint.ModelEnv); model != "" {
				endpoint.Model = model
			}
		}
		if endpoint.BaseURLEnv != "" {
			if baseURL := os.Getenv(endpoint.BaseURLEnv); baseURL != "" {
				endpoint.BaseURL = baseURL
			}
		}
		if endpoint.APIKeyEnv != "" {
			endpoint.APIKey = os.Getenv(endpoint.APIKeyEnv)
		}
		if endpoint.APIKey == "" {
			endpoint.APIKey = endpoint.APIKeyDefault
		}
		cfg.Endpoints[name] = endpoint
	}
}

func applyClickHouseEnvironment(cfg *Config) {
	if host := os.Getenv("CLICKHOUSE_HOST"); host != "" {
		cfg.ClickHouse.Host = host
	}
	if nativePort := os.Getenv("CLICKHOUSE_NATIVE_PORT"); nativePort != "" {
		value, err := strconv.Atoi(nativePort)
		if err == nil && value > 0 {
			cfg.ClickHouse.NativePort = value
		}
	}
	if user := os.Getenv("CLICKHOUSE_USER"); user != "" {
		cfg.ClickHouse.User = user
	}
	cfg.ClickHouse.Password = os.Getenv("CLICKHOUSE_PASSWORD")
	if database := os.Getenv("CLICKHOUSE_DATABASE"); database != "" {
		cfg.ClickHouse.Database = database
	}
	if secure := os.Getenv("CLICKHOUSE_SECURE"); secure != "" {
		value, err := strconv.ParseBool(secure)
		if err == nil {
			cfg.ClickHouse.Secure = value
		}
	}
	cfg.ClickHouse.NativeURL = buildClickHouseNativeURL(cfg.ClickHouse)
}

func buildClickHouseNativeURL(cfg ClickHouseConfig) string {
	query := url.Values{}
	query.Set("database", cfg.Database)
	query.Set("password", cfg.Password)
	query.Set("secure", strconv.FormatBool(cfg.Secure))
	query.Set("username", cfg.User)

	return (&url.URL{
		Scheme:   "clickhouse",
		Host:     net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.NativePort)),
		RawQuery: query.Encode(),
	}).String()
}

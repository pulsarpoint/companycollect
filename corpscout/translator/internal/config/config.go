package config

import (
	"encoding/json"
	"fmt"
	"os"
)

const (
	DefaultConfigPath = "config/translator.json"

	ConfigFileEnv = "TRANSLATOR_CONFIG_FILE"
	APIAddrEnv    = "TRANSLATOR_API_ADDR"
)

type Config struct {
	Server     ServerConfig              `json:"server"`
	ClickHouse ClickHouseConfig          `json:"clickhouse"`
	Endpoints  map[string]EndpointConfig `json:"endpoints"`
	Sources    map[string]SourceConfig   `json:"sources"`
}

type ServerConfig struct {
	ListenAddress string `json:"listen_address"`
}

type ClickHouseConfig struct {
	NativeURL    string `json:"native_url"`
	NativeURLEnv string `json:"native_url_env"`
}

type EndpointConfig struct {
	Model         string         `json:"model"`
	ModelEnv      string         `json:"model_env"`
	BaseURL       string         `json:"base_url"`
	BaseURLEnv    string         `json:"base_url_env"`
	APIKeyEnv     string         `json:"api_key_env"`
	APIKeyDefault string         `json:"api_key_default"`
	MaxTokens     int            `json:"max_tokens"`
	ExtraBody     map[string]any `json:"extra_body"`

	APIKey string `json:"-"`
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

	if cfg.ClickHouse.NativeURLEnv != "" {
		if nativeURL := os.Getenv(cfg.ClickHouse.NativeURLEnv); nativeURL != "" {
			cfg.ClickHouse.NativeURL = nativeURL
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

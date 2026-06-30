package config

import (
	"encoding/json"
	"fmt"
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
	NativeURL    string `json:"native_url"`
	NativeURLEnv string `json:"native_url_env"`
}

type TemporalConfig struct {
	HostPort          string `json:"host_port"`
	HostPortEnv       string `json:"host_port_env"`
	Namespace         string `json:"namespace"`
	NamespaceEnv      string `json:"namespace_env"`
	TaskQueue         string `json:"task_queue"`
	TaskQueueEnv      string `json:"task_queue_env"`
	BatchSize         int    `json:"batch_size"`
	BatchSizeEnv      string `json:"batch_size_env"`
	TimeoutSeconds    int    `json:"timeout_seconds"`
	TimeoutSecondsEnv string `json:"timeout_seconds_env"`
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
	if cfg.Temporal.HostPort == "" {
		cfg.Temporal.HostPort = "localhost:7233"
	}
	if cfg.Temporal.Namespace == "" {
		cfg.Temporal.Namespace = "default"
	}
	if cfg.Temporal.TaskQueue == "" {
		cfg.Temporal.TaskQueue = "translator"
	}
	if cfg.Temporal.BatchSize <= 0 {
		cfg.Temporal.BatchSize = 50
	}
	if cfg.Temporal.TimeoutSeconds <= 0 {
		cfg.Temporal.TimeoutSeconds = 120
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
	if cfg.Temporal.HostPortEnv != "" {
		if hostPort := os.Getenv(cfg.Temporal.HostPortEnv); hostPort != "" {
			cfg.Temporal.HostPort = hostPort
		}
	}
	if cfg.Temporal.NamespaceEnv != "" {
		if namespace := os.Getenv(cfg.Temporal.NamespaceEnv); namespace != "" {
			cfg.Temporal.Namespace = namespace
		}
	}
	if cfg.Temporal.TaskQueueEnv != "" {
		if taskQueue := os.Getenv(cfg.Temporal.TaskQueueEnv); taskQueue != "" {
			cfg.Temporal.TaskQueue = taskQueue
		}
	}
	if cfg.Temporal.BatchSizeEnv != "" {
		if batchSize := os.Getenv(cfg.Temporal.BatchSizeEnv); batchSize != "" {
			value, err := strconv.Atoi(batchSize)
			if err == nil && value > 0 {
				cfg.Temporal.BatchSize = value
			}
		}
	}
	if cfg.Temporal.TimeoutSecondsEnv != "" {
		if timeoutSeconds := os.Getenv(cfg.Temporal.TimeoutSecondsEnv); timeoutSeconds != "" {
			value, err := strconv.Atoi(timeoutSeconds)
			if err == nil && value > 0 {
				cfg.Temporal.TimeoutSeconds = value
			}
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

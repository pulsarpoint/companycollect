package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesClickHouseNativeURLEnv(t *testing.T) {
	t.Setenv("CLICKHOUSE_NATIVE_URL", "clickhouse://clickhouse.test:9000?username=test&password=secret&database=corpscout")

	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{
  "clickhouse": {
    "native_url_env": "CLICKHOUSE_NATIVE_URL"
  }
}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ClickHouse.NativeURL != "clickhouse://clickhouse.test:9000?username=test&password=secret&database=corpscout" {
		t.Fatalf("unexpected ClickHouse native URL: %q", cfg.ClickHouse.NativeURL)
	}
}

func TestLoadEndpointPromptLanguages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{
  "endpoints": {
    "local_llm": {
      "model": "qwen3:6b",
      "base_url": "http://127.0.0.1:8888/v1",
      "api_key_default": "not-needed",
      "prompt_data": {
        "source_language": "Norwegian",
        "target_language": "English"
      }
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	endpoint := cfg.Endpoints["local_llm"]
	if endpoint.PromptData.SourceLanguage != "Norwegian" {
		t.Fatalf("unexpected source language: %q", endpoint.PromptData.SourceLanguage)
	}
	if endpoint.PromptData.TargetLanguage != "English" {
		t.Fatalf("unexpected target language: %q", endpoint.PromptData.TargetLanguage)
	}
}

func TestLoadAppliesTemporalDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Temporal.HostPort != "localhost:7233" {
		t.Fatalf("unexpected Temporal host: %q", cfg.Temporal.HostPort)
	}
	if cfg.Temporal.Namespace != "default" {
		t.Fatalf("unexpected Temporal namespace: %q", cfg.Temporal.Namespace)
	}
	if cfg.Temporal.TaskQueue != "translator" {
		t.Fatalf("unexpected Temporal task queue: %q", cfg.Temporal.TaskQueue)
	}
	if cfg.Temporal.BatchSize != 50 {
		t.Fatalf("unexpected Temporal batch size: %d", cfg.Temporal.BatchSize)
	}
	if cfg.Temporal.TimeoutSeconds != 120 {
		t.Fatalf("unexpected Temporal timeout seconds: %d", cfg.Temporal.TimeoutSeconds)
	}
}

func TestLoadAppliesTemporalEnv(t *testing.T) {
	t.Setenv("TRANSLATOR_TEMPORAL_HOST", "temporal.test:7233")
	t.Setenv("TRANSLATOR_TEMPORAL_TASK_QUEUE", "translator-test")
	t.Setenv("TRANSLATOR_BATCH_SIZE", "13")
	t.Setenv("TRANSLATOR_TIMEOUT_SECONDS", "44")

	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{
  "temporal": {
    "host_port_env": "TRANSLATOR_TEMPORAL_HOST",
    "task_queue_env": "TRANSLATOR_TEMPORAL_TASK_QUEUE",
    "batch_size_env": "TRANSLATOR_BATCH_SIZE",
    "timeout_seconds_env": "TRANSLATOR_TIMEOUT_SECONDS"
  }
}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Temporal.HostPort != "temporal.test:7233" {
		t.Fatalf("unexpected Temporal host: %q", cfg.Temporal.HostPort)
	}
	if cfg.Temporal.TaskQueue != "translator-test" {
		t.Fatalf("unexpected Temporal task queue: %q", cfg.Temporal.TaskQueue)
	}
	if cfg.Temporal.BatchSize != 13 {
		t.Fatalf("unexpected Temporal batch size: %d", cfg.Temporal.BatchSize)
	}
	if cfg.Temporal.TimeoutSeconds != 44 {
		t.Fatalf("unexpected Temporal timeout seconds: %d", cfg.Temporal.TimeoutSeconds)
	}
}

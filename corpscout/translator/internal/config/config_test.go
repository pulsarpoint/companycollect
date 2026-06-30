package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadBuildsClickHouseNativeURLFromDagsterEnv(t *testing.T) {
	t.Setenv("CLICKHOUSE_HOST", "clickhouse.test")
	t.Setenv("CLICKHOUSE_NATIVE_PORT", "9440")
	t.Setenv("CLICKHOUSE_USER", "test-user")
	t.Setenv("CLICKHOUSE_PASSWORD", "secret")
	t.Setenv("CLICKHOUSE_DATABASE", "corpscout_test")
	t.Setenv("CLICKHOUSE_SECURE", "true")

	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	expected := "clickhouse://clickhouse.test:9440?database=corpscout_test&password=secret&secure=true&username=test-user"
	if cfg.ClickHouse.NativeURL != expected {
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

	if cfg.Temporal.Address != "localhost:7233" {
		t.Fatalf("unexpected Temporal address: %q", cfg.Temporal.Address)
	}
	if cfg.Temporal.Namespace != "default" {
		t.Fatalf("unexpected Temporal namespace: %q", cfg.Temporal.Namespace)
	}
	if cfg.Temporal.BatchSize != 50 {
		t.Fatalf("unexpected Temporal batch size: %d", cfg.Temporal.BatchSize)
	}
	if cfg.Temporal.TimeoutSeconds != 120 {
		t.Fatalf("unexpected Temporal timeout seconds: %d", cfg.Temporal.TimeoutSeconds)
	}
}

func TestLoadAppliesTemporalEnv(t *testing.T) {
	t.Setenv("TEMPORAL_ADDRESS", "temporal.test:7233")

	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{
  "temporal": {
    "batch_size": 13,
    "timeout_seconds": 44
  }
}`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Temporal.Address != "temporal.test:7233" {
		t.Fatalf("unexpected Temporal address: %q", cfg.Temporal.Address)
	}
	if cfg.Temporal.BatchSize != 13 {
		t.Fatalf("unexpected Temporal batch size: %d", cfg.Temporal.BatchSize)
	}
	if cfg.Temporal.TimeoutSeconds != 44 {
		t.Fatalf("unexpected Temporal timeout seconds: %d", cfg.Temporal.TimeoutSeconds)
	}
}

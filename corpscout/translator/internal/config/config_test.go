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

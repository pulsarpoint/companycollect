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

func TestLoadParsesQueueConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	content := `{
  "queue": {"path": "data/translator/custom.sqlite", "flush_every_batches": 5},
  "endpoint_id": "local_llm"
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Queue.Path != "data/translator/custom.sqlite" {
		t.Fatalf("expected queue path, got %q", cfg.Queue.Path)
	}
	if cfg.Queue.FlushEveryBatches != 5 {
		t.Fatalf("expected flush_every_batches 5, got %d", cfg.Queue.FlushEveryBatches)
	}
	if cfg.EndpointID != "local_llm" {
		t.Fatalf("expected endpoint_id local_llm, got %q", cfg.EndpointID)
	}
}

func TestLoadAppliesQueueDefaultsAndSoleEndpointFallback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translator.json")
	content := `{
  "endpoints": {"local_llm": {"model": "qwen3:6b", "base_url": "http://x/v1"}}
}`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Queue.Path != "data/translator/queue.sqlite" {
		t.Fatalf("expected default queue path, got %q", cfg.Queue.Path)
	}
	if cfg.Queue.FlushEveryBatches != 10 {
		t.Fatalf("expected default flush_every_batches 10, got %d", cfg.Queue.FlushEveryBatches)
	}
	if cfg.EndpointID != "local_llm" {
		t.Fatalf("expected sole-endpoint fallback, got %q", cfg.EndpointID)
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
	if cfg.Temporal.BatchesPerRun != 500 {
		t.Fatalf("unexpected Temporal batches per run: %d", cfg.Temporal.BatchesPerRun)
	}
}

func TestLoadAppliesTemporalEnv(t *testing.T) {
	t.Setenv("TEMPORAL_ADDRESS", "temporal.test:7233")
	t.Setenv("TRANSLATOR_BATCHES_PER_RUN", "17")

	path := filepath.Join(t.TempDir(), "translator.json")
	if err := os.WriteFile(path, []byte(`{
    "temporal": {
    "batch_size": 13,
    "timeout_seconds": 44,
    "batches_per_run": 22
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
	if cfg.Temporal.BatchesPerRun != 17 {
		t.Fatalf("unexpected Temporal batches per run: %d", cfg.Temporal.BatchesPerRun)
	}
}

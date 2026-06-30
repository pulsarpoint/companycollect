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

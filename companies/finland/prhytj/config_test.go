package prhytj

import (
	"path/filepath"
	"testing"
)

func TestConfigFromEnvUsesCompaniesDataDefault(t *testing.T) {
	t.Setenv("PRH_YTJ_DATA_DIR", "")

	cfg := ConfigFromEnv()
	want := filepath.Join("..", "data", "finland", "countrydata", "sources", "prhytj")
	if cfg.DataDir != want {
		t.Fatalf("DataDir = %q, want %q", cfg.DataDir, want)
	}
}

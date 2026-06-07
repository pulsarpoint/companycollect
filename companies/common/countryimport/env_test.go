package countryimport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadEnvFileSetsUnsetValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("PRH_YTJ_PAGE_DELAY_MS=250\nPRH_YTJ_USER_AGENT=test-agent\n"), 0o600); err != nil {
		t.Fatalf("write env file: %v", err)
	}

	t.Setenv("PRH_YTJ_USER_AGENT", "existing")

	if err := LoadEnvFile(path); err != nil {
		t.Fatalf("LoadEnvFile returned error: %v", err)
	}

	if got := os.Getenv("PRH_YTJ_PAGE_DELAY_MS"); got != "250" {
		t.Fatalf("expected page delay to be set, got %q", got)
	}

	if got := os.Getenv("PRH_YTJ_USER_AGENT"); got != "existing" {
		t.Fatalf("expected user agent to remain existing, got %q", got)
	}
}

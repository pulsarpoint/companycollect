package input

import (
	"strings"
	"testing"
)

func TestApplyLimit(t *testing.T) {
	got := applyLimit("SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains", 100)
	want := "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains LIMIT 100"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
	if applyLimit("SELECT 1", 0) != "SELECT 1" {
		t.Errorf("limit 0 should be a no-op")
	}
	if applyLimit("SELECT 1", -5) != "SELECT 1" {
		t.Errorf("negative limit should be a no-op")
	}
}

// DefaultQuery must be deterministically ordered so a --limit run returns the same domains every
// time (resume-by-rescan depends on it). The ORDER BY must precede the appended LIMIT.
func TestDefaultQueryIsDeterministic(t *testing.T) {
	if !strings.Contains(DefaultQuery, "ORDER BY") {
		t.Fatalf("DefaultQuery must be ordered for reproducible --limit runs: %q", DefaultQuery)
	}
	limited := applyLimit(DefaultQuery, 20)
	oi := strings.Index(limited, "ORDER BY")
	li := strings.Index(limited, "LIMIT")
	if oi < 0 || li < 0 || oi > li {
		t.Errorf("ORDER BY must come before LIMIT: %q", limited)
	}
}

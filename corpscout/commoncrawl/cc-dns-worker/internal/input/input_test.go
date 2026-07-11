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

// DefaultQuery must be deterministically ordered so a --max-domains run returns the same domains every
// time (resume-by-rescan depends on it). The ORDER BY must precede the appended LIMIT.
func TestDefaultQueryIsDeterministic(t *testing.T) {
	if !strings.Contains(DefaultQuery, "ORDER BY") {
		t.Fatalf("DefaultQuery must be ordered for reproducible --max-domains runs: %q", DefaultQuery)
	}
	limited := applyLimit(DefaultQuery, 20)
	oi := strings.Index(limited, "ORDER BY")
	li := strings.Index(limited, "LIMIT")
	if oi < 0 || li < 0 || oi > li {
		t.Errorf("ORDER BY must come before LIMIT: %q", limited)
	}
}

func TestPageQueryUsesKeysetAndBoundParameters(t *testing.T) {
	for _, fragment := range []string{
		"FROM corpscout.commoncrawl_domains",
		"root_domain != ''",
		"root_domain > ?",
		"GROUP BY root_domain",
		"ORDER BY root_domain",
		"LIMIT ?",
	} {
		if !strings.Contains(pageQuery, fragment) {
			t.Errorf("page query missing %q: %s", fragment, pageQuery)
		}
	}
	if strings.Contains(pageQuery, "OFFSET") {
		t.Errorf("domain pagination must be keyset based: %s", pageQuery)
	}
}

func TestDrainRows(t *testing.T) {
	// Fake row source: yields the slice one at a time; empty strings must be dropped.
	src := []string{"a", "", "b", "c", "d"}
	run := func(batchSize int) [][]string {
		i := -1
		var got [][]string
		_ = drainRows(
			func() bool { i++; return i < len(src) },
			func(p *string) error { *p = src[i]; return nil },
			func() error { return nil },
			batchSize,
			func(b []string) error { got = append(got, append([]string(nil), b...)); return nil },
		)
		return got
	}
	// batchSize 2 over [a,b,c,d] (empty dropped) -> [a,b],[c,d]
	if got := run(2); len(got) != 2 || got[0][0] != "a" || got[0][1] != "b" || got[1][0] != "c" || got[1][1] != "d" {
		t.Fatalf("batchSize 2 -> %v", got)
	}
	// batchSize 3 -> [a,b,c],[d]  (final partial batch flushed)
	if got := run(3); len(got) != 2 || len(got[0]) != 3 || len(got[1]) != 1 || got[1][0] != "d" {
		t.Fatalf("batchSize 3 -> %v", got)
	}
}

func TestDrainRowsEmptyAndError(t *testing.T) {
	// Empty source -> fn never called.
	called := false
	_ = drainRows(func() bool { return false }, func(*string) error { return nil }, func() error { return nil }, 2,
		func([]string) error { called = true; return nil })
	if called {
		t.Errorf("fn should not be called for empty source")
	}
	// fn error propagates.
	i := -1
	src := []string{"a", "b"}
	err := drainRows(func() bool { i++; return i < len(src) }, func(p *string) error { *p = src[i]; return nil }, func() error { return nil }, 1,
		func([]string) error { return errTest })
	if err != errTest {
		t.Errorf("want errTest, got %v", err)
	}
}

var errTest = errorString("boom")

type errorString string

func (e errorString) Error() string { return string(e) }

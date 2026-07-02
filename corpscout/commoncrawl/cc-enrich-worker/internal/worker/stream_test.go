package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/output"
)

// Two chunks streamed then committed must produce one multi-row-group file per kind holding ALL rows,
// only the kinds the mode opens, and no leftover .partial files.
func TestShardStreamerCommit(t *testing.T) {
	dir := t.TempDir()
	s, err := NewShardStreamer(dir, "", false /*runEmbed*/, true /*runTech*/)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ShardResult{
		Domains:  []output.DomainRow{{RootDomain: "a.com"}},
		Tech:     []output.TechRow{{RootDomain: "a.com", Technology: "nginx"}},
		Security: []output.SecurityRow{{RootDomain: "a.com", Headers: map[string]string{"server": "nginx"}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ShardResult{
		Domains: []output.DomainRow{{RootDomain: "b.com"}},
		Tech:    []output.TechRow{{RootDomain: "b.com", Technology: "php"}, {RootDomain: "b.com", Technology: "mysql"}},
	}); err != nil {
		t.Fatal(err)
	}
	if s.DomainCount() != 2 {
		t.Fatalf("DomainCount = %d, want 2", s.DomainCount())
	}
	if _, err := s.Commit(); err != nil {
		t.Fatal(err)
	}
	if m, _ := filepath.Glob(filepath.Join(dir, "*.partial")); len(m) != 0 {
		t.Fatalf("partials left after commit: %v", m)
	}
	// tech mode must NOT open industries/page_signals/embeddings
	if _, err := os.Stat(filepath.Join(dir, "industries.parquet")); !os.IsNotExist(err) {
		t.Fatal("industries.parquet should not exist in tech mode")
	}
	// both chunks readable from the single (multi-row-group) file
	dr, err := parquet.ReadFile[output.DomainRow](filepath.Join(dir, "domains.parquet"))
	if err != nil || len(dr) != 2 {
		t.Fatalf("domains: err=%v n=%d want 2", err, len(dr))
	}
	tr, err := parquet.ReadFile[output.TechRow](filepath.Join(dir, "tech.parquet"))
	if err != nil || len(tr) != 3 {
		t.Fatalf("tech: err=%v n=%d want 3", err, len(tr))
	}
	sr, err := parquet.ReadFile[output.SecurityRow](filepath.Join(dir, "security.parquet"))
	if err != nil || len(sr) != 1 || sr[0].Headers["server"] != "nginx" {
		t.Fatalf("security: err=%v rows=%+v", err, sr)
	}
}

// Abort after writing must leave NOTHING behind — so a part that fails the write contract can't leave a
// half-written shard.
func TestShardStreamerAbort(t *testing.T) {
	dir := t.TempDir()
	s, err := NewShardStreamer(dir, "", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(ShardResult{Domains: []output.DomainRow{{RootDomain: "a.com"}}}); err != nil {
		t.Fatal(err)
	}
	s.Abort()
	if m, _ := filepath.Glob(filepath.Join(dir, "*")); len(m) != 0 {
		t.Fatalf("abort left files: %v", m)
	}
}

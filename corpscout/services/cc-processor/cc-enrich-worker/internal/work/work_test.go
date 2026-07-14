package work

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/markers"
)

const (
	testCrawlID   = "CC-TEST-2026-01"
	testSelection = "pages25"
)

// pageRow is one catalog page fixture: parts absent from all rows are empty parts.
type pageRow struct {
	part           uint32
	domain, url    string
	rank           int
	offset, length int64
}

// writeCatalog builds a minimal catalog.duckdb at the exact path Open expects
// (<base>/<crawl>/warc-index/<selection>/catalog.duckdb) — the same schema shape the
// cmd runner fixtures use.
func writeCatalog(t *testing.T, base string, pages []pageRow) {
	t.Helper()
	dir := filepath.Join(base, testCrawlID, "warc-index", testSelection)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", filepath.Join(dir, "catalog.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE main.warcs (warc_index INT, warc_filename VARCHAR);
		CREATE TABLE main.pages (
			warc_index INT,
			root_domain VARCHAR,
			url VARCHAR,
			domain_page_rank INT,
			warc_record_offset BIGINT,
			warc_record_length BIGINT
		)`); err != nil {
		t.Fatal(err)
	}
	warcSeen := map[uint32]bool{}
	for _, p := range pages {
		if !warcSeen[p.part] {
			warcSeen[p.part] = true
			if _, err := db.Exec("INSERT INTO main.warcs VALUES (?, ?)",
				p.part, "part"+strings.ReplaceAll(p.domain, ".", "_")+".warc.gz"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := db.Exec("INSERT INTO main.pages VALUES (?, ?, ?, ?, ?, ?)",
			p.part, p.domain, p.url, p.rank, p.offset, p.length); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec("FORCE CHECKPOINT"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func openTech(t *testing.T, base string) *Run {
	t.Helper()
	r, err := Open(base, testCrawlID, testSelection, "tech")
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestOpenDerivesPrimaryPagesOnlyAndRejectsUnknownCmd(t *testing.T) {
	base := t.TempDir()
	writeCatalog(t, base, []pageRow{{part: 0, domain: "d0.com", url: "https://d0.com/", rank: 1, offset: 0, length: 100}})

	for cmd, want := range map[string]bool{"tech": false, "both": false, "industry": true, "embed": true} {
		r, err := Open(base, testCrawlID, testSelection, cmd)
		if err != nil {
			t.Fatalf("Open(%q): %v", cmd, err)
		}
		if r.primaryPagesOnly != want {
			t.Errorf("Open(%q).primaryPagesOnly = %v, want %v", cmd, r.primaryPagesOnly, want)
		}
	}
	if _, err := Open(base, testCrawlID, testSelection, "loader"); err == nil {
		t.Error("Open with unknown cmd should error")
	}
}

func TestOpenMissingCatalogNamesSyncDB(t *testing.T) {
	_, err := Open(t.TempDir(), testCrawlID, testSelection, "tech")
	if err == nil {
		t.Fatal("Open without a catalog should error")
	}
	if !strings.Contains(err.Error(), "sync-db") || !strings.Contains(err.Error(), "catalog.duckdb") {
		t.Errorf("error should name sync-db and the catalog path, got: %v", err)
	}
}

func TestOutDirAndEmbedDirForLayout(t *testing.T) {
	base := t.TempDir()
	writeCatalog(t, base, []pageRow{{part: 7, domain: "d7.com", url: "https://d7.com/", rank: 1, offset: 0, length: 100}})
	r := openTech(t, base)

	wantOut := filepath.Join(base, testCrawlID, "warc", testSelection, "out_tech_7")
	if got := r.OutDir(7); got != wantOut {
		t.Errorf("OutDir(7) = %q, want %q", got, wantOut)
	}
	// EmbedDirFor follows the ACTUAL outDir's basename — including a --out override.
	wantEmbed := filepath.Join(base, testCrawlID, "embedding", "warc", testSelection, "out_tech_7")
	if got := r.EmbedDirFor(wantOut); got != wantEmbed {
		t.Errorf("EmbedDirFor(default) = %q, want %q", got, wantEmbed)
	}
	wantOverride := filepath.Join(base, testCrawlID, "embedding", "warc", testSelection, "custom-out")
	if got := r.EmbedDirFor("/somewhere/else/custom-out"); got != wantOverride {
		t.Errorf("EmbedDirFor(override) = %q, want %q", got, wantOverride)
	}
}

func TestStatusArms(t *testing.T) {
	base := t.TempDir()
	writeCatalog(t, base, []pageRow{{part: 0, domain: "d0.com", url: "https://d0.com/", rank: 1, offset: 0, length: 100}})
	r := openTech(t, base)

	if got := r.Status(0); got != Pending {
		t.Errorf("no output dir: Status = %v, want Pending", got)
	}

	// Bare debris dir without markers stays Pending (the runner wipes it).
	if err := os.MkdirAll(r.OutDir(0), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(r.OutDir(0), "junk"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := r.Status(0); got != Pending {
		t.Errorf("debris dir: Status = %v, want Pending", got)
	}

	// .loaded preserves for any cmd.
	if err := markers.WriteLoaded(r.OutDir(0)); err != nil {
		t.Fatal(err)
	}
	if got := r.Status(0); got != Preserved {
		t.Errorf(".loaded dir: Status = %v, want Preserved", got)
	}

	// .produced wins over everything.
	if err := markers.WriteProduced(r.OutDir(0), markers.Produced{Part: 0, Cmd: "tech"}); err != nil {
		t.Fatal(err)
	}
	if got := r.Status(0); got != Produced {
		t.Errorf("marked dir: Status = %v, want Produced", got)
	}
}

type embeddingFixture struct {
	Value int64 `parquet:"value"`
}

func TestStatusEmbedPreservationIsScopedToEmbedCmd(t *testing.T) {
	base := t.TempDir()
	writeCatalog(t, base, []pageRow{{part: 0, domain: "d0.com", url: "https://d0.com/", rank: 1, offset: 0, length: 100}})

	embedRun, err := Open(base, testCrawlID, testSelection, "embed")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(embedRun.OutDir(0), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(filepath.Join(embedRun.OutDir(0), "embeddings.parquet"),
		[]embeddingFixture{{Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := embedRun.Status(0); got != Preserved {
		t.Errorf("embed cmd with complete embed file: Status = %v, want Preserved", got)
	}

	// The same complete embed file inside a TECH out dir does not preserve it.
	techRun := openTech(t, base)
	if err := os.MkdirAll(techRun.OutDir(0), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(filepath.Join(techRun.OutDir(0), "embeddings.parquet"),
		[]embeddingFixture{{Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if got := techRun.Status(0); got != Pending {
		t.Errorf("tech cmd with embed file: Status = %v, want Pending", got)
	}
}

func TestPartsClassifiesRange(t *testing.T) {
	base := t.TempDir()
	writeCatalog(t, base, []pageRow{
		{part: 0, domain: "d0.com", url: "https://d0.com/", rank: 1, offset: 0, length: 100},
		{part: 2, domain: "d2.com", url: "https://d2.com/", rank: 1, offset: 0, length: 100},
	})
	r := openTech(t, base)
	if err := os.MkdirAll(r.OutDir(2), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteProduced(r.OutDir(2), markers.Produced{Part: 2, Cmd: "tech"}); err != nil {
		t.Fatal(err)
	}

	parts, err := r.Parts(context.Background(), 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []Part{{0, Pending}, {1, Empty}, {2, Produced}, {3, Empty}}
	if len(parts) != len(want) {
		t.Fatalf("Parts = %v, want %v", parts, want)
	}
	for i := range want {
		if parts[i] != want[i] {
			t.Errorf("Parts[%d] = %+v, want %+v", i, parts[i], want[i])
		}
	}
}

func TestPlanFiltersPrimaryPagesByCmd(t *testing.T) {
	base := t.TempDir()
	writeCatalog(t, base, []pageRow{
		{part: 0, domain: "d0.com", url: "https://d0.com/", rank: 1, offset: 0, length: 100},
		{part: 0, domain: "d0.com", url: "https://d0.com/about", rank: 2, offset: 200, length: 100},
	})

	techPlan, err := openTech(t, base).Plan(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(techPlan.Items) != 2 {
		t.Errorf("tech plan items = %d, want 2 (all pages)", len(techPlan.Items))
	}

	embedRun, err := Open(base, testCrawlID, testSelection, "embed")
	if err != nil {
		t.Fatal(err)
	}
	embedPlan, err := embedRun.Plan(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(embedPlan.Items) != 1 {
		t.Errorf("embed plan items = %d, want 1 (primary only)", len(embedPlan.Items))
	}
}

func TestCompletedEmbedding(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		if _, _, complete := CompletedEmbedding(t.TempDir()); complete {
			t.Fatal("missing output reported complete")
		}
	})

	t.Run("nonempty parquet", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "embeddings.parquet")
		if err := parquet.WriteFile(path, []embeddingFixture{{Value: 1}}); err != nil {
			t.Fatal(err)
		}
		gotPath, rows, complete := CompletedEmbedding(directory)
		if !complete || gotPath != path || rows != 1 {
			t.Fatalf("path=%q rows=%d complete=%v", gotPath, rows, complete)
		}
	})

	t.Run("empty parquet needs completion marker", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "embeddings.parquet")
		if err := parquet.WriteFile(path, []embeddingFixture(nil)); err != nil {
			t.Fatal(err)
		}
		if _, _, complete := CompletedEmbedding(directory); complete {
			t.Fatal("unmarked empty output reported complete")
		}
		if err := os.WriteFile(path+".empty", nil, 0o644); err != nil {
			t.Fatal(err)
		}
		gotPath, rows, complete := CompletedEmbedding(directory)
		if !complete || gotPath != path || rows != 0 {
			t.Fatalf("path=%q rows=%d complete=%v", gotPath, rows, complete)
		}
	})

	t.Run("undecodable nonempty fp16 falls back to stat", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "embeddings_fp16.parquet")
		if err := os.WriteFile(path, []byte("not really parquet"), 0o644); err != nil {
			t.Fatal(err)
		}
		gotPath, rows, complete := CompletedEmbedding(directory)
		if !complete || gotPath != path || rows != 0 {
			t.Fatalf("path=%q rows=%d complete=%v", gotPath, rows, complete)
		}
	})
}

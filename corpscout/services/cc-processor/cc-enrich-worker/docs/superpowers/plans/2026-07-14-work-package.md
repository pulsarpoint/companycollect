# internal/work Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One package (`internal/work`) that answers "what remains to be done for this run" — catalog reads, marker/preservation status, and output-tree layout — with the CLI refactored onto it and every moved helper deleted from `cmd`.

**Architecture:** `work.Run` binds `(base, crawlID, selection, cmd)` at `Open` (which also absorbs the sync-db preflight) and exposes `Parts`/`Status`/`Plan`/`OutDir`/`EmbedDirFor` plus package-level `CompletedEmbedding`. Pure consolidation refactor — zero behavior, log-line, or exit-code change. Spec: `docs/superpowers/specs/2026-07-14-work-package-design.md`.

**Tech Stack:** Go 1.26; existing `internal/catalog` (DuckDB), `internal/markers`, `internal/warcinput`, `parquet-go`.

## Global Constraints

- `work` owns NO scheduling and NO output mutation (stale-dir wiping stays in the runner, keyed off `Pending`).
- Status semantics byte-identical to today: `Produced` = `.produced` marker; `Preserved` = `.loaded` marker (any cmd) or complete embed file (`cmd == "embed"` only), probed only when the out dir exists; `Empty` = no `PartStats` row; `Pending` = the rest.
- `Open` errors (not Fatals) on a missing catalog, naming the exact `sync-db` command; produce runs never sync.
- Path layouts unchanged: outDir `<base>/<crawl>/warc/<selection>/out_<cmd>_<part>`, embed sibling `<base>/<crawl>/embedding/warc/<selection>/<basename(outDir)>`.
- `runRangePool`/`runPartAttempt` take `*work.Run` instead of `outDirFor func(uint32) string`.
- No changes to marker file formats, log lines, exit codes, README, or the retry/breaker dispatcher.
- Working directory for all commands: `corpscout/services/cc-processor/cc-enrich-worker/`. Repo rule: `gofmt` + `go vet` clean; scoped `git add` by explicit path only (shared working tree — never `git add -A`).

---

### Task 1: internal/work package with unit tests

**Files:**
- Create: `internal/work/work.go`
- Create: `internal/work/embedding.go`
- Test: `internal/work/work_test.go`

**Interfaces:**
- Consumes: `catalog.LoadPartStats(ctx, path, lo, hi) ([]catalog.PartStats, error)`; `markers.Exists/ProducedPath/LoadedPath`; `warcinput.LoadPlan(base, crawlID, selection string, warcIndex uint32, primaryPagesOnly bool) (warcinput.Plan, error)`.
- Produces (Task 2 relies on these exact names): `work.Open(base, crawlID, selection, cmd string) (*Run, error)`; `(*Run).CatalogPath() string`; `(*Run).Parts(ctx, lo, hi uint32) ([]Part, error)`; `(*Run).Status(part uint32) Status`; `(*Run).Plan(part uint32) (warcinput.Plan, error)`; `(*Run).OutDir(part uint32) string`; `(*Run).EmbedDirFor(outDir string) string`; `work.CompletedEmbedding(dir string) (string, int64, bool)`; statuses `work.Pending`, `work.Empty`, `work.Produced`, `work.Preserved`.

- [ ] **Step 1: Write the failing tests**

Create `internal/work/work_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/work/ -v`
Expected: FAIL to build with `undefined: Open`, `undefined: Run`, `undefined: CompletedEmbedding`, etc.

- [ ] **Step 3: Write the implementation**

Create `internal/work/work.go`:

```go
// Package work answers "what remains to be done for this run" by composing the read-only catalog,
// the marker lifecycle, and the output-tree layout. It owns NO scheduling and NO mutation of
// outputs — the runner decides what to do with the answers.
package work

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/warcinput"
)

// Status classifies one part's completion state for a run.
type Status int

const (
	Pending   Status = iota // has pages, no marker: needs producing
	Empty                   // no pages in the catalog: nothing to do, ever
	Produced                // .produced marker present: done
	Preserved               // complete-but-unmarked output (.loaded, or a complete embed file): do not touch
)

// Part is one classified part of a planning sweep.
type Part struct {
	Index  uint32
	Status Status
}

// Run binds one run identity — (base, crawlID, selection, cmd) — to the local catalog and the
// output tree. All methods are safe for concurrent use (the fields are immutable after Open).
type Run struct {
	base, crawlID, selection, cmd string
	primaryPagesOnly              bool
	catalogPath                   string
}

// Open binds a run identity. cmd is one of tech|industry|embed|both (anything else errors).
// It fails when the local catalog is absent, with an error naming the exact sync-db command —
// produce runs never sync the catalog (sync-db is the explicit step).
func Open(base, crawlID, selection, cmd string) (*Run, error) {
	var primaryPagesOnly bool
	switch cmd {
	case "industry", "embed":
		primaryPagesOnly = true
	case "tech", "both":
		primaryPagesOnly = false
	default:
		return nil, errors.Newf("unknown command %q (want tech|industry|embed|both)", cmd)
	}
	path := filepath.Join(base, crawlID, "warc-index", selection, "catalog.duckdb")
	if _, err := os.Stat(path); err != nil {
		return nil, errors.Wrapf(err,
			"local catalog missing at %s — run `cc-enrich-worker sync-db --crawl-id %s --selection %s --base %s` first",
			path, crawlID, selection, base)
	}
	return &Run{
		base: base, crawlID: crawlID, selection: selection, cmd: cmd,
		primaryPagesOnly: primaryPagesOnly, catalogPath: path,
	}, nil
}

// CatalogPath is the local read-only DuckDB file every catalog query uses.
func (r *Run) CatalogPath() string { return r.catalogPath }

// OutDir is the single source of truth for the part's output directory:
//
//	<base>/<crawlID>/warc/<selection>/out_<cmd>_<part>
//
// (A single --part run's --out flag remains a caller-side override.)
func (r *Run) OutDir(part uint32) string {
	return filepath.Join(r.base, r.crawlID, "warc", r.selection, fmt.Sprintf("out_%s_%d", r.cmd, part))
}

// EmbedDirFor is the sibling embedding tree for an ACTUAL output directory:
//
//	<base>/<crawlID>/embedding/warc/<selection>/<basename(outDir)>
//
// It takes the outDir (not the part) because single-part runs can override --out, and the
// embedding sibling follows the override's basename.
func (r *Run) EmbedDirFor(outDir string) string {
	return filepath.Join(r.base, r.crawlID, "embedding", "warc", r.selection, filepath.Base(outDir))
}

// Status re-checks one known non-empty part's completion state from the filesystem only (marker,
// then preservation probe) — the cheap, authoritative gate the runner calls just before producing,
// because another host may have produced the part after Parts ran (rsync/NFS marker arrival).
// It never touches the catalog and never returns Empty.
func (r *Run) Status(part uint32) Status {
	outDir := r.OutDir(part)
	if markers.Exists(markers.ProducedPath(outDir)) {
		return Produced
	}
	if info, err := os.Stat(outDir); err == nil && info.IsDir() && r.preserved(outDir) {
		return Preserved
	}
	return Pending
}

// preserved reports whether a NON-EMPTY output dir that lacks a .produced marker must be kept
// (and its part skipped) instead of wiped as crashed-produce debris:
//
//   - a sibling .loaded marker means the retired cc-crawl produce→verify→load lifecycle already
//     loaded this output into ClickHouse and wrote .loaded (it did not always leave .produced
//     behind). Historical output on disk still has that shape, and wiping it would delete data the
//     DB still references — disk would diverge from ClickHouse.
//   - for embed, an already-complete embeddings file (the single-part verify-and-skip predicate,
//     CompletedEmbedding) is the expensive GPU artifact.
//
// Either way the on-disk output is authoritative, so the caller skips the part rather than
// reproducing it.
func (r *Run) preserved(outDir string) bool {
	if markers.Exists(markers.LoadedPath(outDir)) {
		return true
	}
	if r.cmd == "embed" {
		if _, _, complete := CompletedEmbedding(outDir); complete {
			return true
		}
	}
	return false
}

// Parts classifies every part in [lo,hi]: one LoadPartStats query plus one filesystem Status probe
// per present part. This is the PLANNING view; Status is the dispatch-time re-check.
func (r *Run) Parts(ctx context.Context, lo, hi uint32) ([]Part, error) {
	stats, err := catalog.LoadPartStats(ctx, r.catalogPath, lo, hi)
	if err != nil {
		return nil, errors.Wrapf(err, "load part stats from %s", r.catalogPath)
	}
	present := make(map[uint32]struct{}, len(stats))
	for _, stat := range stats {
		present[stat.WarcIndex] = struct{}{}
	}
	parts := make([]Part, 0, int(hi-lo)+1)
	for i := lo; ; i++ {
		status := Empty
		if _, ok := present[i]; ok {
			status = r.Status(i)
		}
		parts = append(parts, Part{Index: i, Status: status})
		if i == hi {
			break
		}
	}
	return parts, nil
}

// Plan loads one part's validated page selection. primaryPagesOnly is derived from cmd
// (industry|embed => true). Only meaningful for Pending parts.
func (r *Run) Plan(part uint32) (warcinput.Plan, error) {
	return warcinput.LoadPlan(r.base, r.crawlID, r.selection, part, r.primaryPagesOnly)
}
```

Create `internal/work/embedding.go` (moved verbatim from `cmd/cc-enrich-worker/main.go`, with `completedEmbedding` exported):

```go
package work

import (
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go"
)

// parquetRows returns a Parquet file's row count by reading only its footer. It errors if the file is
// missing or not a valid/complete Parquet (e.g. a write killed mid-flush) — used by embed verify-and-skip.
func parquetRows(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}

// CompletedEmbedding reports whether dir already holds a complete vector file under EITHER name —
// embeddings.parquet (fp32) or embeddings_fp16.parquet (converted offline). A zero-row file counts
// only with its .empty completion marker; an fp16 file parquet-go cannot decode counts when
// non-empty (its conversion step verified it before the fp32 was pruned).
func CompletedEmbedding(dir string) (string, int64, bool) {
	for _, name := range []string{"embeddings.parquet", "embeddings_fp16.parquet"} {
		path := filepath.Join(dir, name)
		rows, err := parquetRows(path)
		if err == nil {
			if rows > 0 {
				return path, rows, true
			}
			if _, markerErr := os.Stat(path + ".empty"); markerErr == nil {
				return path, 0, true
			}
			continue
		}
		// Converted fp16 files from the older toolchain may have a logical type parquet-go cannot
		// decode. Their conversion step verified the file before removing fp32, so retain that fallback.
		if name == "embeddings_fp16.parquet" {
			info, statErr := os.Stat(path)
			if statErr == nil && info.Size() > 0 {
				return path, 0, true
			}
		}
	}
	return "", 0, false
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/work/ -v`
Expected: PASS (8 test functions).

Then: `gofmt -l ./internal/work/` (must print nothing) and `go vet ./internal/work/`.

- [ ] **Step 5: Commit**

```bash
git add internal/work/work.go internal/work/embedding.go internal/work/work_test.go
git commit -m "feat(corpscout/cc-enrich-worker): internal/work — one API for what remains to be done"
```

---

### Task 2: Rewire cmd onto work and delete the moved helpers

**Files:**
- Modify: `cmd/cc-enrich-worker/main.go` (partDeps, openInput, processInput, run, deletions)
- Modify: `cmd/cc-enrich-worker/runrange.go` (runRange, runRangePool, runPartAttempt, deletions)
- Modify: `cmd/cc-enrich-worker/runrange_test.go` (techDeps builds a work.Run; pool call sites; TestPreserveStaleDir removed)
- Modify: `cmd/cc-enrich-worker/main_test.go` (TestCompletedEmbedding + embeddingFixture removed — moved to work)

**Interfaces:**
- Consumes: everything Task 1 produces, exactly as named there.
- Produces: `partDeps.work *work.Run`; `runRangePool(ctx, class, warcParallel, cmd, runID string, w *work.Run, produce partProducer, prog *poolProgress) rangeSummary`.

- [ ] **Step 1: main.go — partDeps field, imports, and deletions**

Add the import `"cc-enrich-worker/internal/work"` to the internal import block (after `"cc-enrich-worker/internal/warcinput"`), and delete the now-unused `"github.com/parquet-go/parquet-go"` import.

Delete the `parquetRows` and `completedEmbedding` functions entirely (both moved to `internal/work` in Task 1).

In `partDeps`, add the field after `objects fetch.ObjectGetter`:

```go
	// work answers "what remains for this run": catalog plans, part status, output layout.
	// Immutable after Open, safe for the range lanes' concurrent producePart calls.
	work *work.Run
```

Delete the `requireLocalCatalog` function, and restore `openInput`'s doc comment to sit directly above its func (the current text has the `openInput` comment stranded above `requireLocalCatalog`):

```go
// openInput loads one part's plan, prepares the WARC input as network range reads, and logs "WARC
// input ready".
func openInput(ctx context.Context, d partDeps, part uint32, outDir string) (preparedPart, error) {
```

- [ ] **Step 2: main.go — openInput reads the plan through work**

Replace:

```go
	primaryPagesOnly := d.mode == "industry" || d.mode == "embed"
	catalogCachePath := filepath.Join(o.base, o.crawlID, "warc-index", o.selection, "catalog.duckdb")
	catalogStarted := time.Now()
	log.Printf(
		"catalog: opening local catalog crawl=%s selection=%s path=%s",
		o.crawlID,
		o.selection,
		catalogCachePath,
	)
	// The synced LOCAL catalog is the only source — produce runs never sync from RustFS. `sync-db`
	// is the explicit step that downloads and validates it; the CLI entries preflight its presence
	// (requireLocalCatalog) so a missing catalog fails before any part starts.
	plan, err := warcinput.LoadPlan(o.base, o.crawlID, o.selection, part, primaryPagesOnly)
	if err != nil {
		return preparedPart{}, fmt.Errorf("load WARC catalog: %w", err)
	}
```

with:

```go
	catalogStarted := time.Now()
	log.Printf(
		"catalog: opening local catalog crawl=%s selection=%s path=%s",
		o.crawlID,
		o.selection,
		d.work.CatalogPath(),
	)
	// The synced LOCAL catalog is the only source — produce runs never sync from RustFS. `sync-db`
	// is the explicit step that downloads and validates it; work.Open preflighted its presence
	// so a missing catalog fails before any part starts.
	plan, err := d.work.Plan(part)
	if err != nil {
		return preparedPart{}, fmt.Errorf("load WARC catalog: %w", err)
	}
```

(`warcinput` stays imported — `preparedPart.input` is a `*warcinput.Input`.)

- [ ] **Step 3: main.go — processInput's two embedding-dir joins**

Replace (the tech/both streaming branch):

```go
		runEmbedChunk := mode == "both" // "both" also emits vectors -> the separate data/embedding tree
		embedDir := ""
		if runEmbedChunk {
			embedDir = filepath.Join(o.base, o.crawlID, "embedding", "warc", o.selection, filepath.Base(outDir))
		}
```

with:

```go
		runEmbedChunk := mode == "both" // "both" also emits vectors -> the separate data/embedding tree
		embedDir := ""
		if runEmbedChunk {
			embedDir = d.work.EmbedDirFor(outDir)
		}
```

Replace (the industry/embed vector-write branch):

```go
		if len(embeddings) > 0 || mode == "embed" {
			embedDir := outDir
			if mode != "embed" {
				embedDir = filepath.Join(o.base, o.crawlID, "embedding", "warc", o.selection, filepath.Base(outDir))
			}
```

with:

```go
		if len(embeddings) > 0 || mode == "embed" {
			embedDir := outDir
			if mode != "embed" {
				embedDir = d.work.EmbedDirFor(outDir)
			}
```

- [ ] **Step 4: main.go — run() opens the work run**

Replace:

```go
	o.base = base
	requireLocalCatalog(o.base, o.crawlID, o.selection)
```

with:

```go
	o.base = base
	w, err := work.Open(o.base, o.crawlID, o.selection, mode)
	if err != nil {
		log.Fatalf("%v", err)
	}
```

Replace the default-outDir derivation:

```go
	outDir := o.out
	if outDir == "" {
		outDir = filepath.Join(o.base, o.crawlID, "warc", o.selection, fmt.Sprintf("out_%s_%d", mode, o.part))
	}
```

with:

```go
	outDir := o.out
	if outDir == "" {
		outDir = w.OutDir(uint32(o.part))
	}
```

Replace the embed verify-and-skip call:

```go
	if mode == "embed" {
		if embPath, rows, complete := completedEmbedding(outDir); complete {
```

with:

```go
	if mode == "embed" {
		if embPath, rows, complete := work.CompletedEmbedding(outDir); complete {
```

And wire the run onto deps — replace:

```go
	d, err := buildPartDeps(ctx, mode, o, 1) // single-part run: one part shares the transport
	if err != nil {
		log.Fatalf("%v", err)
	}
```

with:

```go
	d, err := buildPartDeps(ctx, mode, o, 1) // single-part run: one part shares the transport
	if err != nil {
		log.Fatalf("%v", err)
	}
	d.work = w
```

Note on `:=`: `run()` declares `base, err := filepath.Abs(o.base)` earlier at the same scope, but
`w, err := work.Open(...)` is still legal Go — `:=` only requires ONE new variable on the left
(`w`), and `err` is reused, not shadowed. The same applies to `d, err :=` above and to `runRange`
in Step 5. Write them exactly as shown.

- [ ] **Step 5: runrange.go — delete preserveStaleDir, rewire runRange**

Delete the `preserveStaleDir` function and its entire doc comment (moved into `work.preserved`).

Add `"cc-enrich-worker/internal/work"` to the imports; remove `"cc-enrich-worker/internal/catalog"` (no longer used after this step).

In `runRange`, replace:

```go
	lo, hi := ro.parts.lo, ro.parts.hi
	// Produce runs never sync the catalog themselves — `sync-db` is the explicit, separate step
	// that downloads and validates it. Fail fast with that command when the local copy is absent.
	catalogPath := requireLocalCatalog(o.base, o.crawlID, o.selection)
	stats, err := catalog.LoadPartStats(ctx, catalogPath, lo, hi)
	if err != nil {
		log.Fatalf("load part stats from %s: %v", catalogPath, err)
	}

	// Every part with catalog stats is range-read; parts with no stats row are empty and skipped.
	present := make(map[uint32]struct{}, len(stats))
	for _, stat := range stats {
		present[stat.WarcIndex] = struct{}{}
	}
	var class []uint32
	empty := 0
	for i := lo; ; i++ {
		if _, ok := present[i]; ok {
			class = append(class, i)
		} else {
			empty++
		}
		if i == hi {
			break
		}
	}

	fmt.Printf("parts=%d selected=%d empty=%d\n", len(class)+empty, len(class), empty)
```

with:

```go
	lo, hi := ro.parts.lo, ro.parts.hi
	// Produce runs never sync the catalog themselves — `sync-db` is the explicit, separate step
	// that downloads and validates it; work.Open fails fast with that command when it is absent.
	w, err := work.Open(o.base, o.crawlID, o.selection, cmd)
	if err != nil {
		log.Fatalf("%v", err)
	}
	parts, err := w.Parts(ctx, lo, hi)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// Every part with catalog pages is range-read; Empty parts are skipped. Produced/Preserved
	// parts still enter the pool — runPartAttempt's Status re-check is the authoritative skip,
	// exactly as before.
	var class []uint32
	empty := 0
	for _, p := range parts {
		if p.Status == work.Empty {
			empty++
			continue
		}
		class = append(class, p.Index)
	}

	fmt.Printf("parts=%d selected=%d empty=%d\n", len(parts), len(class), empty)
```

Note: `runRange` declares `base, err := filepath.Abs(o.base)` earlier, so `w, err :=` would shadow — as in run(), it does NOT: `err` is already declared, so use `w, err := ...` only if the compiler accepts it (it does — `w` is new). Keep `w, err :=`.

Then delete the `outDirFor` closure:

```go
	outDirFor := func(part uint32) string {
		return filepath.Join(o.base, o.crawlID, "warc", o.selection, fmt.Sprintf("out_%s_%d", cmd, part))
	}
```

wire the run onto deps directly after `deps.runStats = runStats`:

```go
	deps.work = w
```

and change the pool call from:

```go
	sum := runRangePool(ctx, class, ro.warcParallel, cmd, runID, outDirFor, produce, prog)
```

to:

```go
	sum := runRangePool(ctx, class, ro.warcParallel, cmd, runID, w, produce, prog)
```

- [ ] **Step 6: runrange.go — runRangePool and runPartAttempt take the work.Run**

In `runRangePool`'s signature, replace `outDirFor func(uint32) string,` with `w *work.Run,`; in the worker goroutine, replace `runPartAttempt(ctx, pd, cmd, runID, outDirFor, produce, prog)` with `runPartAttempt(ctx, pd, cmd, runID, w, produce, prog)`.

Replace `runPartAttempt`'s signature and its skip/preserve/wipe block:

```go
func runPartAttempt(
	ctx context.Context,
	pd pendingPart,
	cmd, runID string,
	outDirFor func(uint32) string,
	produce partProducer,
	prog *poolProgress,
) partOutcome {
	// Stop cleanly once the run is cancelled: the attempt is dropped without being run or marked.
	if ctx.Err() != nil {
		return partOutcome{pending: pd, aborted: true}
	}
	outDir := outDirFor(pd.part)

	if markers.Exists(markers.ProducedPath(outDir)) {
		prog.addSkipped()
		return partOutcome{pending: pd, skipped: true}
	}
	// A non-empty output dir with no .produced marker is USUALLY debris from an attempt that
	// crashed or failed mid-write — remove it so producePart starts clean. But a complete-but-
	// unmarked output (.loaded from the retired cc-crawl lifecycle, or a complete embed file) is
	// authoritative: preserve it and skip the part rather than destroying loaded data.
	if info, statErr := os.Stat(outDir); statErr == nil && info.IsDir() {
		if preserveStaleDir(cmd, outDir) {
			log.Printf("range: preserving complete-but-unmarked output dir (skip) part=%d %s", pd.part, outDir)
			prog.addSkipped()
			return partOutcome{pending: pd, skipped: true}
		}
		log.Printf("range: removing stale output dir (crashed produce?) part=%d %s", pd.part, outDir)
		if rmErr := os.RemoveAll(outDir); rmErr != nil {
			log.Printf("range: remove stale output dir part=%d: %v", pd.part, rmErr)
		}
	}
```

with:

```go
func runPartAttempt(
	ctx context.Context,
	pd pendingPart,
	cmd, runID string,
	w *work.Run,
	produce partProducer,
	prog *poolProgress,
) partOutcome {
	// Stop cleanly once the run is cancelled: the attempt is dropped without being run or marked.
	if ctx.Err() != nil {
		return partOutcome{pending: pd, aborted: true}
	}
	outDir := w.OutDir(pd.part)

	// The dispatch-time authoritative re-check: another host may have produced or loaded this part
	// after the planning sweep (rsync/NFS marker arrival).
	switch w.Status(pd.part) {
	case work.Produced:
		prog.addSkipped()
		return partOutcome{pending: pd, skipped: true}
	case work.Preserved:
		log.Printf("range: preserving complete-but-unmarked output dir (skip) part=%d %s", pd.part, outDir)
		prog.addSkipped()
		return partOutcome{pending: pd, skipped: true}
	}
	// Pending with a non-empty output dir is debris from an attempt that crashed or failed
	// mid-write — remove it so producePart starts clean (output mutation stays in the runner).
	if info, statErr := os.Stat(outDir); statErr == nil && info.IsDir() {
		log.Printf("range: removing stale output dir (crashed produce?) part=%d %s", pd.part, outDir)
		if rmErr := os.RemoveAll(outDir); rmErr != nil {
			log.Printf("range: remove stale output dir part=%d: %v", pd.part, rmErr)
		}
	}
```

Also update `runRangePool`'s doc-comment sentence "Per attempt a worker honors the .produced marker (skip), preserves a complete-but-unmarked output (.loaded or a complete embed file) as skipped, removes a stale output dir left by a crashed or failed produce, runs the producer, and on success writes the .produced marker with the row counts." to:

```go
// a dispatcher loop (this function) that owns all scheduling state. Per attempt a worker re-checks
// the part's work.Status (Produced/Preserved → skip), removes a stale Pending output dir left by a
// crashed or failed produce, runs the producer, and on success writes the .produced marker with the
// row counts.
```

(`markers` stays imported in runrange.go — `WriteProduced` still uses it. `filepath` stays — `filepath.Abs` in runRange.)

- [ ] **Step 7: Test updates**

In `cmd/cc-enrich-worker/runrange_test.go`:

1. Add `"cc-enrich-worker/internal/work"` to the imports.
2. In `techDeps`, wire the work run — replace:

```go
	return partDeps{
		mode: "tech",
		o: opts{
			crawlID:     testCrawlID,
			selection:   testSelection,
			base:        base,
			concurrency: 2,
			chunk:       1024,
			techEngine:  "fast",
		},
		objects: getter,
	}
```

with:

```go
	w, err := work.Open(base, testCrawlID, testSelection, "tech")
	if err != nil {
		t.Fatal(err)
	}
	return partDeps{
		mode: "tech",
		o: opts{
			crawlID:     testCrawlID,
			selection:   testSelection,
			base:        base,
			concurrency: 2,
			chunk:       1024,
			techEngine:  "fast",
		},
		objects: getter,
		work:    w,
	}
```

3. Every `runRangePool(...)` call site passes `deps.work` where `outDirFor` was passed. The tests name the deps variable `deps`; the calls become e.g.:

```go
	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 2, "tech", "test-run", deps.work, produce, nil)
```

(`outDirForTest` remains for building assertion paths — its layout is identical to `work.OutDir`.)

4. Delete `TestPreserveStaleDir` entirely (its arms are covered by `TestStatusArms` + `TestStatusEmbedPreservationIsScopedToEmbedCmd` in `internal/work`). Note it references `embeddingFixture` — see main_test cleanup below.

In `cmd/cc-enrich-worker/main_test.go`: delete `TestCompletedEmbedding` and the `embeddingFixture` type (both moved to `internal/work` in Task 1), and remove the now-unused imports (`os`, `path/filepath`, `github.com/parquet-go/parquet-go`), keeping `testing` and `cc-enrich-worker/internal/worker` for `TestFetchConcurrencyFor`.

- [ ] **Step 8: Full verification gate**

Run, from `cc-enrich-worker/`:

```bash
gofmt -l . && go vet ./... && go test ./... && go test -race ./cmd/cc-enrich-worker/ -run TestRunRangePool
```

Expected: `gofmt -l` prints nothing; vet clean; all packages PASS (including the new `internal/work`); race run clean. If any pool test fails on paths, the fixture layout and `work.OutDir` have diverged — fix the code, not the test.

- [ ] **Step 9: Commit**

```bash
git add cmd/cc-enrich-worker/main.go cmd/cc-enrich-worker/runrange.go cmd/cc-enrich-worker/runrange_test.go cmd/cc-enrich-worker/main_test.go
git commit -m "refactor(corpscout/cc-enrich-worker): rewire cmd onto internal/work; delete moved helpers"
```

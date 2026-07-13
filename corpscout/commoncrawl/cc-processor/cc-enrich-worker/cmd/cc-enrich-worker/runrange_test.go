package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
	"cc-raw/fetch"
)

// gzWarc wraps an HTTP response string as a gzipped single WARC response record (copied from the
// internal/worker test fixtures — they are unexported there, so this minimal copy keeps the runner
// test self-contained rather than exporting a fixture helper).
func gzWarc(httpResp string) []byte {
	rec := "WARC/1.0\r\nWARC-Type: response\r\n\r\n" + httpResp
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	gw.Write([]byte(rec))
	gw.Close()
	return buf.Bytes()
}

// rangeGetter serves fixed WARC record bytes keyed by "<key>:<start>" (mirrors internal/worker's
// multiGetter) and satisfies the full fetch.ObjectGetter so producePart's range-read path (one
// HEAD/ObjectSize + per-record GetRange) runs against local fixtures. A part whose bytes are absent
// from ranges fails at fetch time, which is exactly the "WARC bytes missing" scenario.
type rangeGetter struct {
	ranges map[string][]byte
	size   int64
}

func (g *rangeGetter) GetRange(_ context.Context, _, key string, start, _ int64) ([]byte, error) {
	b, ok := g.ranges[fmt.Sprintf("%s:%d", key, start)]
	if !ok {
		return nil, fmt.Errorf("no bytes for %s:%d", key, start)
	}
	return b, nil
}

func (g *rangeGetter) ObjectSize(_ context.Context, _, _ string) (int64, error) {
	return g.size, nil
}

func (g *rangeGetter) DownloadObject(_ context.Context, _, _ string, _ *os.File) error {
	return fmt.Errorf("whole-file download not supported in tests")
}

var _ fetch.ObjectGetter = (*rangeGetter)(nil)

// fixturePart describes one WARC part for the catalog fixture: a single primary page whose body is
// served (present=true) or withheld (present=false) by the getter.
type fixturePart struct {
	index   uint32
	present bool
}

const (
	testCrawlID   = "CC-TEST-2026-01"
	testSelection = "pages25"
)

// fixtureWARCFilename / fixtureBody produce the WARC object identity and bytes for part i: one domain
// d<i>.com whose single primary page record sits at offset 0. The whole object IS that record, so the
// range read (offset 0..len-1) returns the entire object's bytes.
func fixtureWARCFilename(index uint32) string { return fmt.Sprintf("part%d.warc.gz", index) }

func fixtureBody(index uint32) []byte {
	domain := fmt.Sprintf("d%d.com", index)
	return gzWarc(fmt.Sprintf("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><body>%s company</body></html>", domain))
}

// writeFixtureCatalog builds the catalog.duckdb at the exact path openInput reads
// (<base>/<crawl>/warc-index/<selection>/catalog.duckdb). Each part i is one domain d<i>.com with a
// single primary page whose record length equals its whole-object length.
func writeFixtureCatalog(t *testing.T, base string, parts []fixturePart) {
	t.Helper()
	catalogDir := filepath.Join(base, testCrawlID, "warc-index", testSelection)
	if err := os.MkdirAll(catalogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("duckdb", filepath.Join(catalogDir, "catalog.duckdb"))
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
	for _, p := range parts {
		filename := fixtureWARCFilename(p.index)
		domain := fmt.Sprintf("d%d.com", p.index)
		length := int64(len(fixtureBody(p.index)))
		if _, err := db.Exec("INSERT INTO main.warcs VALUES (?, ?)", p.index, filename); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			"INSERT INTO main.pages VALUES (?, ?, ?, ?, ?, ?)",
			p.index, domain, "https://"+domain+"/", 1, 0, length,
		); err != nil {
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

// writeRangeFixture builds the catalog and returns a range getter serving the present parts' records.
func writeRangeFixture(t *testing.T, base string, parts []fixturePart) *rangeGetter {
	t.Helper()
	writeFixtureCatalog(t, base, parts)
	getter := &rangeGetter{ranges: map[string][]byte{}, size: 1 << 30}
	for _, p := range parts {
		if p.present {
			getter.ranges[fmt.Sprintf("%s:%d", fixtureWARCFilename(p.index), 0)] = fixtureBody(p.index)
		}
	}
	return getter
}

// techDeps builds a tech-mode partDeps around a fixture getter with no ClickHouse/embed endpoint.
func techDeps(t *testing.T, base string, getter fetch.ObjectGetter) partDeps {
	t.Helper()
	fm, err := tech.NewFastMatcher()
	if err != nil {
		t.Fatal(err)
	}
	tech.SetFastMatcher(fm)
	t.Cleanup(func() { tech.SetFastMatcher(nil) })
	// COMMONCRAWL_CATALOG_S3_BASE must be empty so openInput reads the LOCAL fixture catalog.
	t.Setenv("COMMONCRAWL_CATALOG_S3_BASE", "")
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
		source:  "test",
	}
}

func outDirForTest(base, cmd string) func(uint32) string {
	return func(part uint32) string {
		return filepath.Join(base, testCrawlID, "warc", testSelection, fmt.Sprintf("out_%s_%d", cmd, part))
	}
}

func domainsFromParquet(t *testing.T, outDir string) []string {
	t.Helper()
	rows, err := parquet.ReadFile[output.DomainRow](filepath.Join(outDir, "domains.parquet"))
	if err != nil {
		t.Fatalf("read domains.parquet in %s: %v", outDir, err)
	}
	var out []string
	for _, r := range rows {
		out = append(out, r.RootDomain)
	}
	sort.Strings(out)
	return out
}

func reflectEqualU32(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRunRangePoolAllSucceed(t *testing.T) {
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, {index: 1, present: true},
		{index: 2, present: true}, {index: 3, present: true},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 2, "tech", "test-run", outDirFor, produce, nil)

	if sum.Produced != 4 || sum.Failed != 0 || sum.Skipped != 0 || sum.Breaker {
		t.Fatalf("summary = %+v, want produced=4 failed=0 skipped=0 breaker=false", sum)
	}
	for _, part := range []uint32{0, 1, 2, 3} {
		outDir := outDirFor(part)
		if !markers.Exists(markers.ProducedPath(outDir)) {
			t.Errorf("part %d: missing .produced marker", part)
		}
		if got := domainsFromParquet(t, outDir); !reflectEqualStr(got, []string{fmt.Sprintf("d%d.com", part)}) {
			t.Errorf("part %d: domains = %v", part, got)
		}
	}
}

func reflectEqualStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestRunRangePoolFailureAndResume(t *testing.T) {
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, {index: 1, present: true},
		{index: 2, present: false}, // bytes withheld -> this part fails
		{index: 3, present: true},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var mu sync.Mutex
	producedParts := map[uint32]int{}
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		mu.Lock()
		producedParts[part]++
		mu.Unlock()
		return producePart(ctx, deps, part, outDir)
	}

	// warcParallel=1 keeps failure ordering deterministic.
	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", outDirFor, produce, nil)
	if sum.Produced != 3 || sum.Failed != 1 || !reflectEqualU32(sum.FailedParts, []uint32{2}) {
		t.Fatalf("run1 summary = %+v, want produced=3 failed=1 failedParts=[2]", sum)
	}
	for _, part := range []uint32{0, 1, 3} {
		if !markers.Exists(markers.ProducedPath(outDirFor(part))) {
			t.Errorf("run1: part %d should be marked produced", part)
		}
	}
	if markers.Exists(markers.ProducedPath(outDirFor(2))) {
		t.Error("run1: failed part 2 must not be marked produced")
	}

	// Rerun: only the failed part is attempted; the three marked parts are skipped.
	for k := range producedParts {
		delete(producedParts, k)
	}
	sum2 := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", outDirFor, produce, nil)
	if sum2.Skipped != 3 {
		t.Errorf("run2 skipped = %d, want 3", sum2.Skipped)
	}
	if len(producedParts) != 1 || producedParts[2] != 1 {
		t.Errorf("run2 attempted parts = %v, want only {2:1}", producedParts)
	}
}

func TestRunRangePoolParallelDeterministic(t *testing.T) {
	parts := []fixturePart{
		{index: 0, present: true}, {index: 1, present: true},
		{index: 2, present: true}, {index: 3, present: true},
	}
	run := func(warcParallel int) map[uint32][]string {
		base := t.TempDir()
		getter := writeRangeFixture(t, base, parts)
		deps := techDeps(t, base, getter)
		outDirFor := outDirForTest(base, "tech")
		produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
			return producePart(ctx, deps, part, outDir)
		}
		sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, warcParallel, "tech", "test-run", outDirFor, produce, nil)
		if sum.Produced != 4 {
			t.Fatalf("warcParallel=%d produced=%d, want 4", warcParallel, sum.Produced)
		}
		out := map[uint32][]string{}
		for _, part := range []uint32{0, 1, 2, 3} {
			out[part] = domainsFromParquet(t, outDirFor(part))
		}
		return out
	}

	seq := run(1)
	par := run(2)
	for part := uint32(0); part < 4; part++ {
		if !reflectEqualStr(seq[part], par[part]) {
			t.Errorf("part %d: sequential domains %v != parallel %v", part, seq[part], par[part])
		}
	}
}

func TestRunRangePoolBreaker(t *testing.T) {
	base := t.TempDir()
	// Every part is in the catalog but NO bytes are served -> every part fails.
	var parts []fixturePart
	var class []uint32
	for i := uint32(0); i < 10; i++ {
		parts = append(parts, fixturePart{index: i, present: false})
		class = append(class, i)
	}
	getter := writeRangeFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var attempts atomic.Int64
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		attempts.Add(1)
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), class, 1, "tech", "test-run", outDirFor, produce, nil)

	if !sum.Breaker {
		t.Error("breaker should have tripped")
	}
	if sum.Failed != 5 {
		t.Errorf("failed = %d, want exactly 5 (breaker limit)", sum.Failed)
	}
	if got := attempts.Load(); got != 5 {
		t.Errorf("produce attempts = %d, want exactly 5", got)
	}
}

// TestPreserveStaleDir unit-tests the predicate that guards the stale-dir wipe: a bare debris dir
// is wipeable, a sibling .loaded marker or a complete embed file is authoritative and preserved,
// and the embed check is scoped to the embed command only.
func TestPreserveStaleDir(t *testing.T) {
	// Bare non-empty dir, no markers -> wipeable (not preserved).
	bare := t.TempDir()
	if err := os.WriteFile(filepath.Join(bare, "junk"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if preserveStaleDir("tech", bare) {
		t.Error("bare debris dir should not be preserved")
	}

	// A sibling .loaded marker -> preserved for any command.
	loaded := filepath.Join(t.TempDir(), "out_tech_0")
	if err := os.MkdirAll(loaded, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteLoaded(loaded); err != nil {
		t.Fatal(err)
	}
	if !preserveStaleDir("tech", loaded) {
		t.Error(".loaded dir must be preserved")
	}

	// A complete embeddings file -> preserved for embed, but NOT for tech (embed check is scoped).
	emb := t.TempDir()
	if err := parquet.WriteFile(filepath.Join(emb, "embeddings.parquet"), []embeddingFixture{{Value: 1}}); err != nil {
		t.Fatal(err)
	}
	if !preserveStaleDir("embed", emb) {
		t.Error("complete embed dir must be preserved for embed")
	}
	if preserveStaleDir("tech", emb) {
		t.Error("an embeddings file must not preserve a tech dir")
	}
}

// TestRunRangePoolPreservesLoadedDir proves a non-empty output dir carrying a .loaded marker but no
// .produced (the retired cc-crawl produce→load lifecycle) survives a range run: it is skipped, its content
// and .loaded marker are untouched, and no .produced is written — while a sibling part still runs.
func TestRunRangePoolPreservesLoadedDir(t *testing.T) {
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{{index: 0, present: true}, {index: 1, present: true}})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	// Pre-create part 1's output with content + a .loaded marker but NO .produced.
	loaded := outDirFor(1)
	if err := os.MkdirAll(loaded, 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(loaded, "domains.parquet")
	if err := os.WriteFile(sentinel, []byte("loaded-data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := markers.WriteLoaded(loaded); err != nil {
		t.Fatal(err)
	}

	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if part == 1 {
			t.Errorf("part 1 (loaded, preserved) must not be produced")
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1}, 1, "tech", "test-run", outDirFor, produce, nil)

	if sum.Produced != 1 || sum.Skipped != 1 || sum.Failed != 0 {
		t.Fatalf("summary = %+v, want produced=1 skipped=1 failed=0", sum)
	}
	if !markers.Exists(markers.LoadedPath(loaded)) {
		t.Error("preserved dir lost its .loaded marker")
	}
	if markers.Exists(markers.ProducedPath(loaded)) {
		t.Error("preserved dir must not be marked .produced")
	}
	if b, err := os.ReadFile(sentinel); err != nil || string(b) != "loaded-data" {
		t.Errorf("preserved content wiped: b=%q err=%v", b, err)
	}
	if !markers.Exists(markers.ProducedPath(outDirFor(0))) {
		t.Error("healthy sibling part 0 should be produced")
	}
}

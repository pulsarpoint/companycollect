package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
	"cc-enrich-worker/internal/warcinput"
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
// multiGetter) and satisfies the full fetch.ObjectGetter so producePart's ModeRange path (one
// HEAD/ObjectSize + per-record GetRange) runs unchanged against local fixtures. A part whose bytes
// are absent from ranges fails at fetch time, which is exactly the "WARC bytes missing" scenario.
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
// d<i>.com whose single primary page record sits at offset 0. The whole object IS that record, so
// range-mode (offset 0..len-1) and whole-file-mode (download the file) both see identical bytes.
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

// localFixtureGetter serves whole-WARC downloads for the local-lane tests. Each present part's WARC
// object is its single gzipped record, so ObjectSize is the record length and DownloadObject writes
// those bytes; an absent part is withheld, so ObjectSize/DownloadObject fail — the "WARC bytes
// missing" scenario. It instruments the maxWARCFiles bound: every download, after writing its bytes,
// waits `delay` and samples the number of whole-WARC temp files on disk under base, recording the
// peak. A correct pipeline never lets that peak exceed maxWARCFiles.
type localFixtureGetter struct {
	objects map[string][]byte
	base    string
	delay   time.Duration

	mu         sync.Mutex
	peakOnDisk int
}

func (g *localFixtureGetter) ObjectSize(_ context.Context, _, key string) (int64, error) {
	b, ok := g.objects[key]
	if !ok {
		return 0, fmt.Errorf("no object %s", key)
	}
	return int64(len(b)), nil
}

func (g *localFixtureGetter) GetRange(_ context.Context, _, key string, start, end int64) ([]byte, error) {
	b, ok := g.objects[key]
	if !ok {
		return nil, fmt.Errorf("no object %s", key)
	}
	if start < 0 || end < start || end >= int64(len(b)) {
		return nil, fmt.Errorf("invalid range %d-%d for %s (len %d)", start, end, key, len(b))
	}
	return append([]byte(nil), b[start:end+1]...), nil
}

func (g *localFixtureGetter) DownloadObject(_ context.Context, _, key string, f *os.File) error {
	b, ok := g.objects[key]
	if !ok {
		return fmt.Errorf("no object %s", key)
	}
	if _, err := f.Write(b); err != nil {
		return err
	}
	if g.delay > 0 {
		time.Sleep(g.delay)
	}
	n := countWholeWARCTempFiles(g.base)
	g.mu.Lock()
	if n > g.peakOnDisk {
		g.peakOnDisk = n
	}
	g.mu.Unlock()
	return nil
}

func (g *localFixtureGetter) peak() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.peakOnDisk
}

var _ fetch.ObjectGetter = (*localFixtureGetter)(nil)

// writeLocalFixture builds the catalog and returns a whole-file getter for the present parts.
func writeLocalFixture(t *testing.T, base string, parts []fixturePart) *localFixtureGetter {
	t.Helper()
	writeFixtureCatalog(t, base, parts)
	getter := &localFixtureGetter{objects: map[string][]byte{}, base: base}
	for _, p := range parts {
		if p.present {
			getter.objects[fixtureWARCFilename(p.index)] = fixtureBody(p.index)
		}
	}
	return getter
}

// countWholeWARCTempFiles counts the downloaded whole-WARC temp files (created by warcinput inside
// each part's .warc-input dir) currently on disk under base — the true measure of on-disk WARC files.
func countWholeWARCTempFiles(base string) int {
	n := 0
	_ = filepath.WalkDir(base, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), "cc-enrich-") && strings.HasSuffix(d.Name(), ".warc.gz") {
			n++
		}
		return nil
	})
	return n
}

// countWARCInputDirs counts leftover .warc-input directories under base (must be 0 after a clean run).
func countWARCInputDirs(base string) int {
	n := 0
	_ = filepath.WalkDir(base, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && d.Name() == ".warc-input" {
			n++
		}
		return nil
	})
	return n
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

func TestSelectClass(t *testing.T) {
	c := catalog.Classification{Local: []uint32{0, 3}, Remote: []uint32{1}, Empty: []uint32{2}}

	if got := selectClass("tech", c); !reflectEqualU32(got, []uint32{1}) {
		t.Errorf("tech selectClass = %v, want [1]", got)
	}
	if got := selectClass("both", c); !reflectEqualU32(got, []uint32{1}) {
		t.Errorf("both selectClass = %v, want [1]", got)
	}
	if got := selectClass("industry", c); !reflectEqualU32(got, []uint32{0, 1, 3}) {
		t.Errorf("industry selectClass = %v, want [0 1 3]", got)
	}
	if got := selectClass("embed", c); !reflectEqualU32(got, []uint32{0, 1, 3}) {
		t.Errorf("embed selectClass = %v, want [0 1 3]", got)
	}
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
		return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 2, "tech", "test-run", outDirFor, produce)

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
		return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
	}

	// warcParallel=1 keeps failure ordering deterministic.
	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", outDirFor, produce)
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
	sum2 := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", outDirFor, produce)
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
			return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
		}
		sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, warcParallel, "tech", "test-run", outDirFor, produce)
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
		return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
	}

	sum := runRangePool(context.Background(), class, 1, "tech", "test-run", outDirFor, produce)

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
// .produced (cc-crawl's produce→load lifecycle) survives a range run: it is skipped, its content
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
		return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1}, 1, "tech", "test-run", outDirFor, produce)

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

// localLanes builds the download (ModeWholeFile) + process closures for the local pool over deps.
func localLanes(deps partDeps) (partOpener, partProcessor) {
	open := func(ctx context.Context, part uint32, outDir string) (preparedPart, error) {
		return openInput(ctx, deps, part, warcinput.ModeWholeFile, outDir)
	}
	process := func(ctx context.Context, prepared preparedPart) (partResult, error) {
		return processInput(ctx, deps, prepared)
	}
	return open, process
}

// TestRunRangeLocalPoolIdenticalToWholeFile proves the local (download/process) pipeline yields the
// same per-part domains.parquet as sequential ModeWholeFile producePart runs, marks every part, and
// leaves no temp state behind.
func TestRunRangeLocalPoolIdenticalToWholeFile(t *testing.T) {
	class := []uint32{0, 1, 2}
	parts := []fixturePart{{index: 0, present: true}, {index: 1, present: true}, {index: 2, present: true}}

	// Reference: sequential ModeWholeFile producePart into an isolated base.
	refBase := t.TempDir()
	refGetter := writeLocalFixture(t, refBase, parts)
	refDeps := techDeps(t, refBase, refGetter)
	refOutFor := outDirForTest(refBase, "tech")
	want := map[uint32][]string{}
	for _, part := range class {
		if _, err := producePart(context.Background(), refDeps, part, warcinput.ModeWholeFile, refOutFor(part)); err != nil {
			t.Fatalf("reference producePart part %d: %v", part, err)
		}
		want[part] = domainsFromParquet(t, refOutFor(part))
	}

	// Under test: the local pool, single-file / single-lane so ordering is deterministic.
	base := t.TempDir()
	getter := writeLocalFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")
	open, process := localLanes(deps)

	sum := runRangeLocalPool(context.Background(), class, 1, 1, 1, "tech", "test-run", outDirFor, open, process)

	if sum.Produced != 3 || sum.Failed != 0 || sum.Skipped != 0 || sum.Breaker {
		t.Fatalf("summary = %+v, want produced=3 failed=0 skipped=0 breaker=false", sum)
	}
	for _, part := range class {
		outDir := outDirFor(part)
		if !markers.Exists(markers.ProducedPath(outDir)) {
			t.Errorf("part %d: missing .produced marker", part)
		}
		if got := domainsFromParquet(t, outDir); !reflectEqualStr(got, want[part]) {
			t.Errorf("part %d: local domains %v != whole-file %v", part, got, want[part])
		}
	}
	if n := countWARCInputDirs(base); n != 0 {
		t.Errorf("leftover .warc-input dirs = %d, want 0", n)
	}
}

// TestRunRangeLocalPoolConcurrencyBound proves the single slots semaphore keeps on-disk whole-WARC
// files at or below maxWARCFiles even when downloadParallel would otherwise run more downloads at
// once, and that all temp state is removed at exit.
func TestRunRangeLocalPoolConcurrencyBound(t *testing.T) {
	cases := []struct {
		name             string
		downloadParallel int
		processParallel  int
		maxWARCFiles     int
	}{
		{"brief-2x2", 2, 2, 2},
		{"semaphore-below-download", 2, 1, 1}, // downloadParallel > maxWARCFiles: slot must gate
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var parts []fixturePart
			var class []uint32
			for i := uint32(0); i < 6; i++ {
				parts = append(parts, fixturePart{index: i, present: true})
				class = append(class, i)
			}
			base := t.TempDir()
			getter := writeLocalFixture(t, base, parts)
			getter.delay = 25 * time.Millisecond // widen the on-disk window so overlap is observable
			deps := techDeps(t, base, getter)
			outDirFor := outDirForTest(base, "tech")
			open, process := localLanes(deps)

			sum := runRangeLocalPool(context.Background(), class, tc.downloadParallel, tc.processParallel, tc.maxWARCFiles, "tech", "test-run", outDirFor, open, process)

			if sum.Produced != len(class) || sum.Failed != 0 {
				t.Fatalf("summary = %+v, want produced=%d failed=0", sum, len(class))
			}
			peak := getter.peak()
			t.Logf("peak on-disk WARC files = %d (maxWARCFiles=%d, downloadParallel=%d)", peak, tc.maxWARCFiles, tc.downloadParallel)
			if peak > tc.maxWARCFiles {
				t.Errorf("peak on-disk WARC files = %d, exceeds maxWARCFiles=%d", peak, tc.maxWARCFiles)
			}
			if n := countWARCInputDirs(base); n != 0 {
				t.Errorf("leftover .warc-input dirs = %d, want 0", n)
			}
			if n := countWholeWARCTempFiles(base); n != 0 {
				t.Errorf("leftover whole-WARC temp files = %d, want 0", n)
			}
		})
	}
}

// TestRunRangeLocalPoolFailureNoMarkerNoTemp proves a part whose WARC bytes are missing is recorded
// failed, writes no .produced marker, and leaves no temp file behind, while healthy parts still
// produce.
func TestRunRangeLocalPoolFailureNoMarkerNoTemp(t *testing.T) {
	base := t.TempDir()
	parts := []fixturePart{
		{index: 0, present: true},
		{index: 1, present: false}, // bytes withheld -> download fails
		{index: 2, present: true},
	}
	getter := writeLocalFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")
	open, process := localLanes(deps)

	sum := runRangeLocalPool(context.Background(), []uint32{0, 1, 2}, 1, 1, 2, "tech", "test-run", outDirFor, open, process)

	if sum.Produced != 2 || sum.Failed != 1 || !reflectEqualU32(sum.FailedParts, []uint32{1}) {
		t.Fatalf("summary = %+v, want produced=2 failed=1 failedParts=[1]", sum)
	}
	if markers.Exists(markers.ProducedPath(outDirFor(1))) {
		t.Error("failed part 1 must not be marked produced")
	}
	for _, part := range []uint32{0, 2} {
		if !markers.Exists(markers.ProducedPath(outDirFor(part))) {
			t.Errorf("part %d should be marked produced", part)
		}
	}
	if n := countWARCInputDirs(base); n != 0 {
		t.Errorf("leftover .warc-input dirs = %d, want 0", n)
	}
	if n := countWholeWARCTempFiles(base); n != 0 {
		t.Errorf("leftover whole-WARC temp files = %d, want 0", n)
	}
}

// TestRunRangeLocalPoolResumeSkips proves a rerun skips parts already carrying a .produced marker
// (download slot acquired then released on skip) and attempts only the unmarked part.
func TestRunRangeLocalPoolResumeSkips(t *testing.T) {
	base := t.TempDir()
	parts := []fixturePart{{index: 0, present: true}, {index: 1, present: true}, {index: 2, present: true}}
	getter := writeLocalFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var mu sync.Mutex
	opened := map[uint32]int{}
	baseOpen, process := localLanes(deps)
	open := func(ctx context.Context, part uint32, outDir string) (preparedPart, error) {
		mu.Lock()
		opened[part]++
		mu.Unlock()
		return baseOpen(ctx, part, outDir)
	}

	if sum := runRangeLocalPool(context.Background(), []uint32{0, 1, 2}, 1, 1, 1, "tech", "test-run", outDirFor, open, process); sum.Produced != 3 {
		t.Fatalf("run1 produced = %d, want 3", sum.Produced)
	}
	// Wipe part 1's marker so only it reruns.
	if err := os.Remove(markers.ProducedPath(outDirFor(1))); err != nil {
		t.Fatal(err)
	}
	for k := range opened {
		delete(opened, k)
	}

	sum2 := runRangeLocalPool(context.Background(), []uint32{0, 1, 2}, 1, 1, 1, "tech", "test-run", outDirFor, open, process)
	if sum2.Skipped != 2 || sum2.Produced != 1 {
		t.Errorf("run2 summary = %+v, want skipped=2 produced=1", sum2)
	}
	if len(opened) != 1 || opened[1] != 1 {
		t.Errorf("run2 opened parts = %v, want only {1:1}", opened)
	}
}

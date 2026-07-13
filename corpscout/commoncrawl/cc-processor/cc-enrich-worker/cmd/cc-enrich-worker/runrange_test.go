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

// writeRangeFixture builds a 4-part-style catalog.duckdb at the exact path producePart reads
// (<base>/<crawl>/warc-index/<selection>/catalog.duckdb) and returns a getter serving the present
// parts' records. Each part i is one domain d<i>.com with a single primary page.
func writeRangeFixture(t *testing.T, base string, parts []fixturePart) *rangeGetter {
	t.Helper()
	catalogDir := filepath.Join(base, testCrawlID, "warc-index", testSelection)
	if err := os.MkdirAll(catalogDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(catalogDir, "catalog.duckdb")
	db, err := sql.Open("duckdb", path)
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

	getter := &rangeGetter{ranges: map[string][]byte{}, size: 1 << 30}
	for _, p := range parts {
		filename := fmt.Sprintf("part%d.warc.gz", p.index)
		domain := fmt.Sprintf("d%d.com", p.index)
		body := gzWarc(fmt.Sprintf("HTTP/1.1 200 OK\r\nServer: nginx\r\n\r\n<html><body>%s company</body></html>", domain))
		length := int64(len(body))
		if _, err := db.Exec("INSERT INTO main.warcs VALUES (?, ?)", p.index, filename); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(
			"INSERT INTO main.pages VALUES (?, ?, ?, ?, ?, ?)",
			p.index, domain, "https://"+domain+"/", 1, 0, length,
		); err != nil {
			t.Fatal(err)
		}
		if p.present {
			getter.ranges[fmt.Sprintf("%s:%d", filename, 0)] = body
		}
	}
	if _, err := db.Exec("FORCE CHECKPOINT"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
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

	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 2, "tech", outDirFor, produce)

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
	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", outDirFor, produce)
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
	sum2 := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", outDirFor, produce)
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
		sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, warcParallel, "tech", outDirFor, produce)
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

	sum := runRangePool(context.Background(), class, 1, "tech", outDirFor, produce)

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

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
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
	"cc-enrich-worker/internal/work"
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
		tech:    fm,
		objects: getter,
		work:    w,
	}
}

func outDirForTest(base, cmd string) func(uint32) string {
	return func(part uint32) string {
		return filepath.Join(base, testCrawlID, "warc", testSelection, fmt.Sprintf("out_%s_%d", cmd, part))
	}
}

// shrinkBackoff makes retry backoffs effectively immediate so exhaustion tests run in milliseconds.
func shrinkBackoff(t *testing.T) {
	t.Helper()
	old := partBackoffBase
	partBackoffBase = time.Microsecond
	t.Cleanup(func() { partBackoffBase = old })
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

	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 2, "tech", "test-run", deps.work, produce, nil)

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
	shrinkBackoff(t)
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
	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", deps.work, produce, nil)
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
	sum2 := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 1, "tech", "test-run", deps.work, produce, nil)
	if sum2.Skipped != 3 {
		t.Errorf("run2 skipped = %d, want 3", sum2.Skipped)
	}
	// Run 2 never produces (healthy parts all skip, and a skip is not a phase-2 transition), so the
	// still-cold part 2 trips the phase-1 breaker on its 5th consecutive failure instead of
	// exhausting all attempts. The part stays unmarked either way.
	if !sum2.Breaker {
		t.Error("run2: breaker should trip — only failures and skips, no successes")
	}
	if len(producedParts) != 1 || producedParts[2] != consecutiveFailureLimit {
		t.Errorf("run2 attempted parts = %v, want only {2:%d}", producedParts, consecutiveFailureLimit)
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
		sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, warcParallel, "tech", "test-run", deps.work, produce, nil)
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
	var selectedParts []uint32
	for i := uint32(0); i < 10; i++ {
		parts = append(parts, fixturePart{index: i, present: false})
		selectedParts = append(selectedParts, i)
	}
	getter := writeRangeFixture(t, base, parts)
	deps := techDeps(t, base, getter)

	var attempts atomic.Int64
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		attempts.Add(1)
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), selectedParts, 1, "tech", "test-run", deps.work, produce, nil)

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

	sum := runRangePool(context.Background(), []uint32{0, 1}, 1, "tech", "test-run", deps.work, produce, nil)

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

// TestRunRangePoolRetriesThenSucceeds proves a transiently-failing part is requeued with backoff,
// its stale output debris is wiped between attempts, and it ends Produced — not Failed.
func TestRunRangePoolRetriesThenSucceeds(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{{index: 0, present: true}})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var attempts atomic.Int64
	var junk string
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if attempts.Add(1) <= 2 {
			// Simulate a produce that failed after partial output: the debris must be wiped
			// by the stale-dir cleanup before the retry attempt.
			if err := os.MkdirAll(outDir, 0o755); err != nil {
				t.Fatal(err)
			}
			junk = filepath.Join(outDir, "partial.tmp")
			if err := os.WriteFile(junk, []byte("debris"), 0o644); err != nil {
				t.Fatal(err)
			}
			return partResult{}, fmt.Errorf("transient S3 coldness")
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0}, 1, "tech", "test-run", deps.work, produce, nil)

	if sum.Produced != 1 || sum.Failed != 0 || sum.Retries != 2 || sum.Breaker {
		t.Fatalf("summary = %+v, want produced=1 failed=0 retries=2 breaker=false", sum)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	if !markers.Exists(markers.ProducedPath(outDirFor(0))) {
		t.Error("part 0 should be marked produced")
	}
	if _, err := os.Stat(junk); !os.IsNotExist(err) {
		t.Errorf("junk from failed attempt survived: err=%v", err)
	}
	if got := domainsFromParquet(t, outDirFor(0)); !reflectEqualStr(got, []string{"d0.com"}) {
		t.Errorf("domains = %v", got)
	}
}

// TestRunRangePoolExhaustsPersistentlyFailingPart proves a part whose WARC bytes never appear is
// attempted exactly maxPartAttempts times, then counted Failed once and left unmarked.
func TestRunRangePoolExhaustsPersistentlyFailingPart(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, // succeeds first: the run is in phase 2 before the failures
		{index: 1, present: false},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	var attempts1 atomic.Int64
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if part == 1 {
			attempts1.Add(1)
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1}, 1, "tech", "test-run", deps.work, produce, nil)

	if sum.Produced != 1 || sum.Failed != 1 || !reflectEqualU32(sum.FailedParts, []uint32{1}) || sum.Breaker {
		t.Fatalf("summary = %+v, want produced=1 failed=1 failedParts=[1] breaker=false", sum)
	}
	if sum.Retries != maxPartAttempts-1 {
		t.Errorf("retries = %d, want %d", sum.Retries, maxPartAttempts-1)
	}
	if got := attempts1.Load(); got != maxPartAttempts {
		t.Errorf("part 1 attempts = %d, want %d", got, maxPartAttempts)
	}
	if markers.Exists(markers.ProducedPath(outDirFor(1))) {
		t.Error("exhausted part must not be marked produced")
	}
}

// TestRunRangePoolTransientFailuresDoNotTripBreakerAfterSuccess: 16 attempt failures (far past the
// old 5-consecutive limit) from two cold parts must not kill a run that has already produced —
// phase 2 counts only consecutive EXHAUSTED parts, and 2 < 5.
func TestRunRangePoolTransientFailuresDoNotTripBreakerAfterSuccess(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true},
		{index: 1, present: false}, {index: 2, present: false},
		{index: 3, present: true}, {index: 4, present: true},
	})
	deps := techDeps(t, base, getter)
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3, 4}, 1, "tech", "test-run", deps.work, produce, nil)

	if sum.Breaker {
		t.Fatal("breaker must not trip on transient failures after a success")
	}
	if sum.Produced != 3 || sum.Failed != 2 {
		t.Fatalf("summary = %+v, want produced=3 failed=2", sum)
	}
	if sum.Retries != 2*(maxPartAttempts-1) {
		t.Errorf("retries = %d, want %d", sum.Retries, 2*(maxPartAttempts-1))
	}
}

// TestRunRangePoolExhaustedPartsTripBreaker: in phase 2, 5 consecutive EXHAUSTED parts still trip.
func TestRunRangePoolExhaustedPartsTripBreaker(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	parts := []fixturePart{{index: 0, present: true}}
	selectedParts := []uint32{0}
	for i := uint32(1); i <= 5; i++ {
		parts = append(parts, fixturePart{index: i, present: false})
		selectedParts = append(selectedParts, i)
	}
	getter := writeRangeFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), selectedParts, 1, "tech", "test-run", deps.work, produce, nil)

	if !sum.Breaker {
		t.Fatal("5 consecutive exhausted parts must trip the breaker")
	}
	if sum.Produced != 1 || sum.Failed != 5 {
		t.Fatalf("summary = %+v, want produced=1 failed=5", sum)
	}
}

// TestRunRangePoolPhase1TripSurvivesStragglerSuccess: with two workers, part 0's produce blocks
// until the breaker trips (it waits on the pool context), then completes successfully. That
// straggler success drains AFTER the phase-1 trip and must not erase the trip's failure report:
// the summary still lists the five distinct parts that fed the breaker, and Breaker stays set.
// This also exercises the halted-drain path with outstanding work on a second worker.
func TestRunRangePoolPhase1TripSurvivesStragglerSuccess(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	parts := []fixturePart{{index: 0, present: true}}
	selectedParts := []uint32{0}
	for i := uint32(1); i <= 5; i++ {
		parts = append(parts, fixturePart{index: i, present: false}) // bytes withheld -> fail
		selectedParts = append(selectedParts, i)
	}
	getter := writeRangeFixture(t, base, parts)
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if part == 0 {
			<-ctx.Done() // hold this attempt in flight until the breaker cancels the pool
			return partResult{Rows: map[string]int{"domains": 1}}, nil
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(context.Background(), selectedParts, 2, "tech", "test-run", deps.work, produce, nil)

	if !sum.Breaker {
		t.Fatal("phase-1 breaker should have tripped")
	}
	if sum.Produced != 1 {
		t.Errorf("produced = %d, want 1 (the straggler success is still a real produce)", sum.Produced)
	}
	if sum.Failed != 5 || !reflectEqualU32(sum.FailedParts, []uint32{1, 2, 3, 4, 5}) {
		t.Errorf("failed = %d parts %v, want 5 parts [1 2 3 4 5]", sum.Failed, sum.FailedParts)
	}
	if !markers.Exists(markers.ProducedPath(outDirFor(0))) {
		t.Error("straggler success should still write part 0's marker")
	}
}

// TestRunRangePoolExternalCancelDrains proves an external cancel (operator interrupt) drains the
// pool promptly — the test would hang past its deadline on a dispatcher/worker deadlock.
func TestRunRangePoolExternalCancelDrains(t *testing.T) {
	shrinkBackoff(t)
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, {index: 1, present: true}, {index: 2, present: true},
	})
	deps := techDeps(t, base, getter)
	outDirFor := outDirForTest(base, "tech")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		if part == 0 {
			cancel()
			return partResult{}, ctx.Err()
		}
		return producePart(ctx, deps, part, outDir)
	}

	sum := runRangePool(ctx, []uint32{0, 1, 2}, 1, "tech", "test-run", deps.work, produce, nil)

	if sum.Produced != 0 {
		t.Errorf("produced = %d, want 0 after immediate cancel", sum.Produced)
	}
	for _, part := range []uint32{0, 1, 2} {
		if markers.Exists(markers.ProducedPath(outDirFor(part))) {
			t.Errorf("part %d must not be marked produced after cancel", part)
		}
	}
}

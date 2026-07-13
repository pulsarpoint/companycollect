package main

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"
	"time"

	"cc-enrich-worker/internal/worker"
	"cc-raw/fetch"
)

func TestFormatRangeStats(t *testing.T) {
	pool := poolSnapshot{inFlight: 8, produced: 42, skipped: 2, failed: 1, total: 100}
	rs := worker.RunStatsSnapshot{
		Pages:   1204480,
		FetchNs: 1204480 * 165 * int64(time.Millisecond),
		TechNs:  1204480 * 38 * int64(time.Millisecond),
	}
	// Cumulative getter counters this tick; prev is the empty first-tick baseline, so the whole of
	// s3cur is the per-tick delta: 16520 attempts / 10s = 1652 req/s; 510 MiB / 10s = 51.0 MiB/s.
	// retries = attempts - head - get = 16520 - 0 - 16520 = 0 (all GetObject, no SDK retries).
	s3cur := fetch.S3Stats{
		HTTPAttempts:   16520,
		GetObjectCalls: 16520,
		BodyBytes:      51 * 1024 * 1024 * 10,
	}

	got := formatRangeStats(pool, rs, s3cur, fetch.S3Stats{}, true, 100*time.Minute, 10*time.Second)

	// parts + pages + cumulative-429 + rates + avg segments present and correct.
	want := "stats: parts run=8 done=42/100 skip=2 fail=1 | pages 200.7/s (1204480 total) | s3 1652 req/s 51.0 MiB/s 429=0 5xx=0 retries=0 | avg fetch=165ms tech=38ms"
	if got != want {
		t.Fatalf("formatRangeStats mismatch:\n got=%q\nwant=%q", got, want)
	}
	// The spec's own example line is 153 chars, so "~140" is a soft target, not a hard cap: assert a
	// realistic ceiling that keeps the line to one terminal row while preserving the required shape.
	if len(got) > 160 {
		t.Errorf("line length %d exceeds 160: %q", len(got), got)
	}
}

// Zero-page / zero-elapsed start must not divide by zero and must omit the s3 segment when the
// getter is not stats-capable (hasS3=false).
func TestFormatRangeStatsZeroStart(t *testing.T) {
	pool := poolSnapshot{inFlight: 1, produced: 0, skipped: 0, failed: 0, total: 4}
	got := formatRangeStats(pool, worker.RunStatsSnapshot{}, fetch.S3Stats{}, fetch.S3Stats{}, false, 0, 0)
	want := "stats: parts run=1 done=0/4 skip=0 fail=0 | pages 0.0/s (0 total) | avg fetch=0ms tech=0ms"
	if got != want {
		t.Fatalf("zero-start mismatch:\n got=%q\nwant=%q", got, want)
	}
}

// Cumulative 429/5xx/retries must survive across ticks even when the per-tick request delta is zero.
func TestFormatRangeStatsCumulativeThrottleSignals(t *testing.T) {
	pool := poolSnapshot{inFlight: 8, produced: 1, skipped: 0, failed: 0, total: 8}
	s3cur := fetch.S3Stats{HTTPAttempts: 500, GetObjectCalls: 400, HeadObjectCalls: 50, HTTP429s: 7, HTTP503s: 3}
	// prev == cur => zero request delta this tick, but the cumulative 429/5xx must still show.
	got := formatRangeStats(pool, worker.RunStatsSnapshot{Pages: 10}, s3cur, s3cur, true, time.Minute, 10*time.Second)
	// retries = 500 - 400 - 50 = 50 (cumulative).
	want := "stats: parts run=8 done=1/8 skip=0 fail=0 | pages 0.2/s (10 total) | s3 0 req/s 0.0 MiB/s 429=7 5xx=3 retries=50 | avg fetch=0ms tech=0ms"
	if got != want {
		t.Fatalf("cumulative signals mismatch:\n got=%q\nwant=%q", got, want)
	}
}

// TestRunRangeSinkAggregatesAndSilencesPerChunkLogs wires the process-wide sink through a real range
// run over the duckdb+getter fixtures and proves the two contracts the feature rests on: (1) the sink
// accumulates exactly the pages the parts processed (one primary page per fixture part), and (2) the
// per-chunk "timing/page" noise is gone while the per-part "done:" line is kept.
func TestRunRangeSinkAggregatesAndSilencesPerChunkLogs(t *testing.T) {
	base := t.TempDir()
	getter := writeRangeFixture(t, base, []fixturePart{
		{index: 0, present: true}, {index: 1, present: true},
		{index: 2, present: true}, {index: 3, present: true},
	})
	deps := techDeps(t, base, getter)

	// Wire the sink: this is what puts producePart's ShardConfig into range mode (aggregate + quiet).
	runStats := &worker.RunStats{}
	deps.runStats = runStats

	outDirFor := outDirForTest(base, "tech")
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}

	// Capture log output. The stdlib logger serializes writes under its own mutex, so the concurrent
	// pool goroutines can't race the buffer; we read it only after the pool has fully returned.
	var buf bytes.Buffer
	prevOut := log.Writer()
	log.SetOutput(&buf)
	t.Cleanup(func() { log.SetOutput(prevOut) })

	prog := &poolProgress{total: 4}
	sum := runRangePool(context.Background(), []uint32{0, 1, 2, 3}, 2, "tech", "test-run", outDirFor, produce, prog)
	log.SetOutput(prevOut)

	if sum.Produced != 4 || sum.Failed != 0 {
		t.Fatalf("summary = %+v, want produced=4 failed=0", sum)
	}
	// One primary page per fixture part => 4 pages folded into the sink.
	if got := runStats.Snapshot().Pages; got != 4 {
		t.Errorf("RunStats.pages = %d, want 4 (one page per part)", got)
	}
	if prog.produced.Load() != 4 {
		t.Errorf("poolProgress.produced = %d, want 4", prog.produced.Load())
	}
	out := buf.String()
	if strings.Contains(out, "timing/page") {
		t.Errorf("per-chunk 'timing/page' line must be suppressed in range mode; log:\n%s", out)
	}
	if strings.Contains(out, "S3 range reads") {
		t.Errorf("per-chunk 'S3 range reads' line must be suppressed in range mode; log:\n%s", out)
	}
	if strings.Contains(out, "progress:") {
		t.Errorf("per-part 'progress:' line must be suppressed in range mode; log:\n%s", out)
	}
	if !strings.Contains(out, "done:") {
		t.Errorf("per-part 'done:' line must be KEPT in range mode; log:\n%s", out)
	}
}

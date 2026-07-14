package main

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/worker"
)

// startRangeStatsTicker launches the 10-second cumulative-stats goroutine for a range run and returns
// a stop function. The stop function prints one FINAL line (so short runs that never reach a 10s tick
// still get one) and then blocks until the goroutine has exited. The s3 segment is included only when
// the shared getter is a *fetch.S3Getter exposing cumulative Stats(); any other getter (anonymous
// HTTPS) is tolerated by dropping that segment.
func startRangeStatsTicker(getter fetch.ObjectGetter, rs *worker.RunStats, prog *poolProgress, start time.Time) func() {
	s3Getter, hasS3 := getter.(*fetch.S3Getter)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		prev := fetch.S3Stats{}
		if hasS3 {
			prev = s3Getter.Stats()
		}
		prevT := start
		emit := func() {
			now := time.Now()
			cur := fetch.S3Stats{}
			if hasS3 {
				cur = s3Getter.Stats()
			}
			log.Print(formatRangeStats(prog.snapshot(), rs.Snapshot(), cur, prev, hasS3, now.Sub(start), now.Sub(prevT)))
			prev, prevT = cur, now
		}
		for {
			select {
			case <-stop:
				emit() // final line before the pool summary
				return
			case <-t.C:
				emit()
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			wg.Wait()
		})
	}
}

// poolProgress is the range pool's live tally, read by the stats ticker. It is separate from the
// returned rangeSummary (which is the FINAL, post-run value): the ticker needs the counts WHILE the
// pool runs, including how many parts are producing right now (inFlight). All methods are nil-safe so
// a single --part run and the unit tests can pass a nil *poolProgress and get plain no-ops.
type poolProgress struct {
	inFlight, produced, skipped, failed atomic.Int64
	total                               int
}

func (p *poolProgress) startPart() {
	if p != nil {
		p.inFlight.Add(1)
	}
}

func (p *poolProgress) endPart() {
	if p != nil {
		p.inFlight.Add(-1)
	}
}

func (p *poolProgress) addProduced() {
	if p != nil {
		p.produced.Add(1)
	}
}

func (p *poolProgress) addSkipped() {
	if p != nil {
		p.skipped.Add(1)
	}
}

func (p *poolProgress) addFailed() {
	if p != nil {
		p.failed.Add(1)
	}
}

// poolSnapshot is a consistent-enough read of poolProgress for one stats line.
type poolSnapshot struct {
	inFlight, produced, skipped, failed int64
	total                               int
}

func (p *poolProgress) snapshot() poolSnapshot {
	if p == nil {
		return poolSnapshot{}
	}
	return poolSnapshot{
		inFlight: p.inFlight.Load(),
		produced: p.produced.Load(),
		skipped:  p.skipped.Load(),
		failed:   p.failed.Load(),
		total:    p.total,
	}
}

// formatRangeStats renders the single cumulative progress line for a range run. It is pure (no clock,
// no I/O) so it can be unit-tested against fixed snapshots.
//
//   - parts:  run = producing right now, done/total, skipped, failed (the pool tally).
//   - pages:  cumulative processed pages + a whole-run rate (cumulative / elapsed).
//   - s3:     req/s and MiB/s are the per-tick DELTA (s3cur.Delta(s3prev) over tick); but 429/5xx/
//     sdk-retries are shown CUMULATIVE — they are the "is CC throttling us" signals and must
//     never be lost between ticks. Omitted entirely when the getter is not stats-capable.
//   - avg:    per-page fetch/tech latency, cumulative (ns totals / pages).
//
// elapsed is time since the run started (pages rate); tick is the wall time since the previous line
// (s3 delta rates). Guards keep a zero-page / zero-duration start from dividing by zero.
func formatRangeStats(pool poolSnapshot, rs worker.RunStatsSnapshot, s3cur, s3prev fetch.S3Stats, hasS3 bool, elapsed, tick time.Duration) string {
	var b strings.Builder
	fmt.Fprintf(&b, "stats: parts run=%d done=%d/%d skip=%d fail=%d",
		pool.inFlight, pool.produced, pool.total, pool.skipped, pool.failed)

	pagesRate := 0.0
	if elapsed > 0 {
		pagesRate = float64(rs.Pages) / elapsed.Seconds()
	}
	fmt.Fprintf(&b, " | pages %.1f/s (%d total) errs=%d", pagesRate, rs.Pages, rs.Errs)

	if hasS3 {
		d := s3cur.Delta(s3prev)
		reqRate, mibRate := 0.0, 0.0
		if tick > 0 {
			reqRate = float64(d.HTTPAttempts) / tick.Seconds()
			mibRate = float64(d.BodyBytes) / (1024 * 1024) / tick.Seconds()
		}
		retries := s3cur.HTTPAttempts - s3cur.HeadObjectCalls - s3cur.GetObjectCalls
		if retries < 0 {
			retries = 0
		}
		// 5xx currently means HTTP 503 only: fetch.S3Stats has no generic 5xx bucket, and 503
		// SlowDown is the server-error signal Common Crawl's throttling actually emits.
		fmt.Fprintf(&b, " | s3 %.0f req/s %.1f MiB/s 429=%d 5xx=%d retries=%d",
			reqRate, mibRate, s3cur.HTTP429s, s3cur.HTTP503s, retries)
	}

	fetchMs, techMs := 0.0, 0.0
	if rs.Pages > 0 {
		fetchMs = float64(rs.FetchNs) / float64(rs.Pages) / 1e6
		techMs = float64(rs.TechNs) / float64(rs.Pages) / 1e6
	}
	fmt.Fprintf(&b, " | avg fetch=%.0fms tech=%.0fms", fetchMs, techMs)
	return b.String()
}

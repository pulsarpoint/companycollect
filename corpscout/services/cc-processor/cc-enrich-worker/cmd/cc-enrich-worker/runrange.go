package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/worker"
)

// consecutiveFailureLimit is the circuit breaker threshold. Until the first successful produce it
// counts CONSECUTIVE attempt failures of any kind — a systemically broken source (dead catalog, bad
// output root) must fail fast, not spin through retry backoffs. After any success it counts only
// consecutive EXHAUSTED parts (each already retried maxPartAttempts times), so bursty S3 coldness
// cannot kill an otherwise working run.
const consecutiveFailureLimit = 5

// rangeSummary is the tallied outcome of a range-runner pool. It is a pure value with no printing or
// os.Exit so the pool can be unit-tested; the CLI entry (runRange) prints it and sets the exit code.
type rangeSummary struct {
	Produced    int
	Skipped     int
	Failed      int      // parts that exhausted maxPartAttempts (or fed a phase-1 breaker trip)
	FailedParts []uint32 // in failure order
	Retries     int      // failed attempts that were requeued for another try
	Breaker     bool     // true if the consecutive-failure breaker tripped
}

// partProducer produces one part into outDir, returning its per-kind row counts. In production it
// wraps producePart (range reads); tests inject a fixture-backed producer.
type partProducer func(ctx context.Context, part uint32, outDir string) (partResult, error)

// preserveStaleDir reports whether a NON-EMPTY output dir that lacks a .produced marker must be
// kept (and its part skipped) instead of wiped as crashed-produce debris:
//
//   - a sibling .loaded marker means the retired cc-crawl produce→verify→load lifecycle already
//     loaded this output into ClickHouse and wrote .loaded (it did not always leave .produced behind).
//     Historical output on disk still has that shape, and wiping it would delete data the DB still
//     references — disk would diverge from ClickHouse.
//   - for embed, an already-complete embeddings file (the single-part verify-and-skip predicate,
//     completedEmbedding) is the expensive GPU artifact; spec §2 keeps that as the inner safety net.
//
// Either way the on-disk output is authoritative, so the caller skips the part rather than
// reproducing it.
func preserveStaleDir(cmd, outDir string) bool {
	if markers.Exists(markers.LoadedPath(outDir)) {
		return true
	}
	if cmd == "embed" {
		if _, _, complete := completedEmbedding(outDir); complete {
			return true
		}
	}
	return false
}

// runRangePool consumes class over min(warcParallel, len(class)) worker goroutines, coordinated by
// a dispatcher loop (this function) that owns all scheduling state. Per attempt a worker honors the
// .produced marker (skip), preserves a complete-but-unmarked output (.loaded or a complete embed
// file) as skipped, removes a stale output dir left by a crashed or failed produce, runs the
// producer, and on success writes the .produced marker with the row counts.
//
// Failure handling (spec 2026-07-14-part-retry-backoff-design.md): a failed part is requeued with
// exponential backoff (partBackoff) and counts as Failed only after maxPartAttempts attempts. The
// breaker is two-phase (see consecutiveFailureLimit); a trip cancels the context and abandons
// everything still pending. It returns the tally — no printing, no os.Exit.
func runRangePool(
	ctx context.Context,
	class []uint32,
	warcParallel int,
	cmd, runID string,
	outDirFor func(uint32) string,
	produce partProducer,
	prog *poolProgress,
) rangeSummary {
	workers := warcParallel
	if workers > len(class) {
		workers = len(class)
	}
	if workers < 1 {
		workers = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	parts := make(chan pendingPart)
	// Buffered to the worker count so a worker never blocks reporting: at most workers attempts are
	// in flight, hence at most workers unread outcomes.
	results := make(chan partOutcome, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for pd := range parts {
				results <- runPartAttempt(ctx, pd, cmd, runID, outDirFor, produce, prog)
			}
		}()
	}

	pending := &partQueue{}
	for _, part := range class {
		pending.add(pendingPart{part: part})
	}

	d := &poolDispatcher{pending: pending, prog: prog, failedSeen: map[uint32]bool{}}
	outstanding := 0
	tripped := false
	done := ctx.Done()
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		halted := tripped || ctx.Err() != nil
		if outstanding == 0 && (halted || pending.Len() == 0) {
			break
		}
		// Arm exactly one of: a dispatch of the eligible head, or a timer for when it becomes
		// eligible. Halted runs arm neither and only drain outstanding attempts.
		var dispatch chan pendingPart
		var head pendingPart
		var timerC <-chan time.Time
		timer.Stop()
		if !halted && pending.Len() > 0 {
			head = pending.peek()
			if wait := time.Until(head.eligibleAt); wait <= 0 {
				dispatch = parts
			} else {
				timer.Reset(wait)
				timerC = timer.C
			}
		}
		select {
		case dispatch <- head:
			pending.next()
			if head.attempts > 0 {
				prog.endRetryWait()
			}
			outstanding++
		case out := <-results:
			outstanding--
			if d.handle(out) {
				tripped = true
				cancel()
			}
		case <-timerC:
			// the head part's backoff expired; loop re-evaluates eligibility
		case <-done:
			done = nil // stop dispatching; keep looping to drain outstanding attempts
		}
	}
	close(parts)
	wg.Wait()

	if d.sum.Breaker && !d.producedAny {
		// Phase-1 trip: nothing succeeded and nothing is exhausted yet (a part cannot reach
		// maxPartAttempts failures before 5 consecutive failures trip). Report every part that fed
		// the breaker as failed — the run is being abandoned as systemically broken.
		d.sum.Failed = len(d.firstFailed)
		d.sum.FailedParts = d.firstFailed
	}
	return d.sum
}

// partOutcome is one worker's report of one dispatched attempt.
type partOutcome struct {
	pending pendingPart
	skipped bool
	aborted bool // the context was already cancelled; the attempt never ran and is not tallied
	err     error
}

// poolDispatcher is the single-goroutine scheduling state of runRangePool. Only the dispatcher
// loop touches it; workers communicate via partOutcome messages.
type poolDispatcher struct {
	sum         rangeSummary
	firstFailed []uint32 // distinct failed parts in first-failure order (phase-1 breaker report)
	failedSeen  map[uint32]bool
	producedAny bool
	consecutive int // phase 1: consecutive attempt failures; phase 2: consecutive exhausted parts
	pending     *partQueue
	prog        *poolProgress
}

// handle folds one outcome into the tally and requeues transient failures. It reports whether the
// breaker tripped on this outcome.
func (d *poolDispatcher) handle(out partOutcome) (trip bool) {
	if out.aborted {
		return false
	}
	if out.skipped {
		d.sum.Skipped++
		return false
	}
	if out.err == nil {
		d.sum.Produced++
		d.producedAny = true
		d.consecutive = 0
		return false
	}

	attempts := out.pending.attempts + 1
	if !d.failedSeen[out.pending.part] {
		d.failedSeen[out.pending.part] = true
		d.firstFailed = append(d.firstFailed, out.pending.part)
	}
	if !d.producedAny {
		d.consecutive++
		if d.consecutive >= consecutiveFailureLimit {
			d.sum.Breaker = true
			return true
		}
	}
	if attempts >= maxPartAttempts {
		d.sum.Failed++
		d.sum.FailedParts = append(d.sum.FailedParts, out.pending.part)
		d.prog.addFailed()
		log.Printf("range: part %d FAILED after %d attempts: %v", out.pending.part, attempts, out.err)
		if d.producedAny {
			d.consecutive++
			if d.consecutive >= consecutiveFailureLimit {
				d.sum.Breaker = true
				return true
			}
		}
		return false
	}
	backoff := partBackoff(attempts)
	d.sum.Retries++
	d.prog.addRetryWait()
	log.Printf("range: part %d failed attempt %d/%d, retrying in %s: %v",
		out.pending.part, attempts, maxPartAttempts, backoff, out.err)
	d.pending.add(pendingPart{part: out.pending.part, attempts: attempts, eligibleAt: time.Now().Add(backoff)})
	return false
}

// runPartAttempt executes one dispatched attempt of one part: marker skip, preserve/wipe of stale
// output, produce, marker write. All tallying is the dispatcher's; this only reports what happened.
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

	partStart := time.Now()
	prog.startPart()
	res, perr := produce(ctx, pd.part, outDir)
	prog.endPart()
	if perr == nil {
		perr = markers.WriteProduced(outDir, markers.Produced{
			Part:        pd.part,
			Cmd:         cmd,
			Rows:        res.Rows,
			SourceRunID: runID,
			DurationS:   time.Since(partStart).Seconds(),
			FinishedAt:  time.Now().UTC(),
		})
		if perr != nil {
			perr = fmt.Errorf("write produced marker: %w", perr)
		}
	}
	if perr == nil {
		prog.addProduced()
		log.Printf("range: part %d produced -> %s", pd.part, outDir)
	}
	return partOutcome{pending: pd, err: perr}
}

// runRange is the CLI entry for `<cmd> --parts A-B`: resolve and (if configured) sync the catalog,
// select every non-empty part in the range (range reads are the only fetch strategy), build the
// shared deps once, run the bounded pool, print the summary, and exit non-zero if any part failed
// (breaker included).
func runRange(cmd string, o opts, ro runnerOpts) {
	ctx := context.Background()
	if o.base == "" {
		log.Fatal("--base is required (output root)")
	}
	base, err := filepath.Abs(o.base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", o.base, err)
	}
	o.base = base

	lo, hi := ro.parts.lo, ro.parts.hi
	catalogS3Base := strings.TrimSpace(os.Getenv("COMMONCRAWL_CATALOG_S3_BASE"))
	if catalogS3Base != "" {
		// Sync the committed RustFS catalog into the local cache up front, so LoadPartStats and every
		// per-part LoadPlan read a present local catalog. SyncLocal pulls the whole catalog (not a
		// single WARC), so it needs no index and any error (bad credentials, unreachable RustFS,
		// corrupt commit) is a fatal setup failure.
		if _, err := catalog.SyncLocal(
			ctx,
			catalog.S3Config{
				BaseURI:   catalogS3Base,
				Endpoint:  os.Getenv("CORPSCOUT_S3_ENDPOINT"),
				Region:    envOr("CORPSCOUT_S3_REGION", "us-east-1"),
				AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
				SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
			},
			o.base, o.crawlID, o.selection,
		); err != nil {
			log.Fatalf("sync catalog cache: %v", err)
		}
	}

	catalogPath := filepath.Join(o.base, o.crawlID, "warc-index", o.selection, "catalog.duckdb")
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

	if len(class) == 0 {
		log.Printf("range: nothing to do — no parts to process in [%d,%d]", lo, hi)
		return
	}

	// Parts-level parallelism sizes the shared transport (fetchConcurrencyFor): every part range-reads
	// concurrently, so warcParallel parts genuinely contend for the one transport (spec §2).
	deps, err := buildPartDeps(ctx, cmd, o, ro.warcParallel)
	if err != nil {
		log.Fatalf("%v", err)
	}

	outDirFor := func(part uint32) string {
		return filepath.Join(o.base, o.crawlID, "warc", o.selection, fmt.Sprintf("out_%s_%d", cmd, part))
	}

	start := time.Now()
	// One run id for every .produced marker written by this invocation. No crawl-wide run identity
	// exists in the CLI, so derive the cheapest truthful one: command + range + start time.
	runID := fmt.Sprintf("%s-%d-%d-%d", cmd, lo, hi, start.Unix())

	// The process-wide sink: every chunk of every part folds its page/fetch/tech counters here (and
	// its per-chunk logs fall silent) so the ticker below can print ONE cumulative line. Wiring it on
	// deps is what puts producePart's ShardConfig into range mode; the single --part path never does.
	runStats := &worker.RunStats{}
	deps.runStats = runStats
	prog := &poolProgress{total: len(class)}
	stopTicker := startRangeStatsTicker(deps.objects, runStats, prog, start)

	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}
	sum := runRangePool(ctx, class, ro.warcParallel, cmd, runID, outDirFor, produce, prog)
	stopTicker() // prints the final cumulative line, before the summary below
	elapsed := time.Since(start).Round(time.Second)

	partsMsg := ""
	if len(sum.FailedParts) > 0 {
		partsMsg = fmt.Sprintf(" [failed parts: %s]", joinParts(sum.FailedParts))
	}
	fmt.Printf("produced=%d skipped=%d failed=%d retries=%d%s elapsed=%s\n",
		sum.Produced, sum.Skipped, sum.Failed, sum.Retries, partsMsg, elapsed)

	if sum.Breaker {
		last := sum.FailedParts
		if len(last) > consecutiveFailureLimit {
			last = last[len(last)-consecutiveFailureLimit:]
		}
		log.Printf("range: BREAKER tripped after %d consecutive failures — abandoned remaining parts; last failures: %s",
			consecutiveFailureLimit, joinParts(last))
	}
	if sum.Failed > 0 {
		os.Exit(1)
	}
}

// joinParts renders a uint32 part list as a comma-separated string for log/summary lines.
func joinParts(parts []uint32) string {
	strs := make([]string, len(parts))
	for i, p := range parts {
		strs[i] = fmt.Sprintf("%d", p)
	}
	return strings.Join(strs, ",")
}

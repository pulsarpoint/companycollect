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

	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/work"
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

// runRangePool consumes selectedParts over min(warcParallel, len(selectedParts)) worker goroutines, coordinated by
// a dispatcher loop (this function) that owns all scheduling state. Per attempt a worker re-checks
// the part's work.Status (Produced/Preserved → skip), removes a stale Pending output dir left by a
// crashed or failed produce, runs the producer, and on success writes the .produced marker with the
// row counts.
//
// Failure handling (spec 2026-07-14-part-retry-backoff-design.md): a failed part is requeued with
// exponential backoff (partBackoff) and counts as Failed only after maxPartAttempts attempts. The
// breaker is two-phase (see consecutiveFailureLimit); a trip cancels the context and abandons
// everything still pending. It returns the tally — no printing, no os.Exit.
func runRangePool(
	ctx context.Context,
	selectedParts []uint32,
	warcParallel int,
	cmd, runID string,
	w *work.Run,
	produce partProducer,
	prog *poolProgress,
) rangeSummary {
	workers := warcParallel
	if workers > len(selectedParts) {
		workers = len(selectedParts)
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
				results <- runPartAttempt(ctx, pd, cmd, runID, w, produce, prog)
			}
		}()
	}

	pending := &partQueue{}
	for _, part := range selectedParts {
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

	if d.phase1Trip {
		// Phase-1 trip: nothing had succeeded when the breaker tripped — a straggler success may
		// have drained afterwards (an in-flight produce can finish after cancel and still be
		// tallied as Produced). Report every part that fed the breaker as failed regardless — the
		// run is being abandoned as systemically broken.
		d.sum.Failed = len(d.firstFailed)
		d.sum.FailedParts = d.firstFailed
		for range d.firstFailed {
			d.prog.addFailed()
		}
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
	phase1Trip  bool // latched true the instant the phase-1 breaker trips; never cleared
	consecutive int  // phase 1: consecutive attempt failures; phase 2: consecutive exhausted parts
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
			d.phase1Trip = true
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

// runRange is the CLI entry for `<cmd> --parts A-B`: require the synced local catalog (sync-db is
// the explicit sync step), select every non-empty part in the range (range reads are the only fetch
// strategy), build the shared deps once, run the bounded pool, print the summary, and exit non-zero
// if any part failed (breaker included).
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
	var selectedParts []uint32
	empty := 0
	for _, p := range parts {
		if p.Status == work.Empty {
			empty++
			continue
		}
		selectedParts = append(selectedParts, p.Index)
	}

	fmt.Printf("parts=%d selected=%d empty=%d\n", len(parts), len(selectedParts), empty)

	if len(selectedParts) == 0 {
		log.Printf("range: nothing to do — no parts to process in [%d,%d]", lo, hi)
		return
	}

	// Parts-level parallelism sizes the shared transport (fetchConcurrencyFor): every part range-reads
	// concurrently, so warcParallel parts genuinely contend for the one transport (spec §2).
	deps, err := buildPartDeps(ctx, cmd, o, ro.warcParallel)
	if err != nil {
		log.Fatalf("%v", err)
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
	deps.work = w
	prog := &poolProgress{total: len(selectedParts)}
	stopTicker := startRangeStatsTicker(deps.objects, runStats, prog, start)

	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}
	sum := runRangePool(ctx, selectedParts, ro.warcParallel, cmd, runID, w, produce, prog)
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
	if sum.Failed > 0 || sum.Breaker {
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

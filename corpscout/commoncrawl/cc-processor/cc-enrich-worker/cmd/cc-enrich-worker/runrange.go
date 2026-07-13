package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/markers"
	"cc-enrich-worker/internal/warcinput"
)

// consecutiveFailureLimit is the circuit breaker: after this many CONSECUTIVE part failures the pool
// cancels and abandons the rest of the range, on the theory that the source is systemically broken
// (throttling, auth, dead catalog) rather than a few parts decaying independently.
const consecutiveFailureLimit = 5

// rangeSummary is the tallied outcome of a range-runner pool. It is a pure value with no printing or
// os.Exit so the pool can be unit-tested; the CLI entry (runRange) prints it and sets the exit code.
type rangeSummary struct {
	Produced    int
	Skipped     int
	Failed      int
	FailedParts []uint32 // in failure order
	Breaker     bool     // true if the consecutive-failure breaker tripped
}

// partProducer produces one part into outDir, returning its per-kind row counts. In production it
// wraps producePart with warcinput.ModeRange; tests inject a fixture-backed producer.
type partProducer func(ctx context.Context, part uint32, outDir string) (partResult, error)

// selectClass picks which classified parts a command actually processes. industry/embed selections
// are sparse — even a part the threshold called "local" is small enough to range-read — so they take
// EVERY non-empty part (local + remote, ascending). tech/both take only the remote lane; their local
// parts are large and belong to the local (whole-WARC download) runner.
func selectClass(cmd string, c catalog.Classification) []uint32 {
	if cmd == "industry" || cmd == "embed" {
		merged := make([]uint32, 0, len(c.Local)+len(c.Remote))
		merged = append(merged, c.Local...)
		merged = append(merged, c.Remote...)
		sort.Slice(merged, func(i, j int) bool { return merged[i] < merged[j] })
		return merged
	}
	return append([]uint32(nil), c.Remote...)
}

// preserveStaleDir reports whether a NON-EMPTY output dir that lacks a .produced marker must be
// kept (and its part skipped) instead of wiped as crashed-produce debris:
//
//   - a sibling .loaded marker means cc-crawl's produce→verify→load lifecycle already loaded this
//     output into ClickHouse and wrote .loaded (it does not always leave .produced behind). Wiping
//     it would delete data the DB still references — disk would diverge from ClickHouse.
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

// runRangePool consumes class over min(warcParallel, len(class)) worker goroutines. Per part it
// honors the .produced marker (skip), preserves a complete-but-unmarked output (.loaded or a
// complete embed file) as skipped, removes a stale output dir left by a crashed produce, runs the
// producer, and on success writes the .produced marker with the row counts. A shared consecutive-
// failure counter trips the breaker (cancels the context) at consecutiveFailureLimit; parts not yet
// started are neither run nor marked. It returns the tally — no printing, no os.Exit.
func runRangePool(
	ctx context.Context,
	class []uint32,
	warcParallel int,
	cmd, runID string,
	outDirFor func(uint32) string,
	produce partProducer,
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

	parts := make(chan uint32)
	var mu sync.Mutex
	var sum rangeSummary
	consecutive := 0

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for part := range parts {
				// Stop cleanly once the breaker has tripped: any part still buffered in the channel is
				// dropped without being produced or marked.
				if ctx.Err() != nil {
					return
				}
				outDir := outDirFor(part)

				if markers.Exists(markers.ProducedPath(outDir)) {
					mu.Lock()
					sum.Skipped++
					mu.Unlock()
					continue
				}
				// A non-empty output dir with no .produced marker is USUALLY debris from a produce that
				// crashed mid-write — remove it so producePart starts clean. But a complete-but-unmarked
				// output (.loaded from cc-crawl, or a complete embed file) is authoritative: preserve it
				// and skip the part rather than destroying loaded data.
				if info, statErr := os.Stat(outDir); statErr == nil && info.IsDir() {
					if preserveStaleDir(cmd, outDir) {
						log.Printf("range: preserving complete-but-unmarked output dir (skip) part=%d %s", part, outDir)
						mu.Lock()
						sum.Skipped++
						mu.Unlock()
						continue
					}
					log.Printf("range: removing stale output dir (crashed produce?) part=%d %s", part, outDir)
					if rmErr := os.RemoveAll(outDir); rmErr != nil {
						log.Printf("range: remove stale output dir part=%d: %v", part, rmErr)
					}
				}

				partStart := time.Now()
				res, perr := produce(ctx, part, outDir)
				if perr == nil {
					perr = markers.WriteProduced(outDir, markers.Produced{
						Part:        part,
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

				mu.Lock()
				if perr != nil {
					sum.Failed++
					sum.FailedParts = append(sum.FailedParts, part)
					consecutive++
					log.Printf("range: part %d FAILED: %v", part, perr)
					tripped := consecutive >= consecutiveFailureLimit
					if tripped {
						sum.Breaker = true
					}
					mu.Unlock()
					if tripped {
						cancel()
						return
					}
					continue
				}
				consecutive = 0
				sum.Produced++
				mu.Unlock()
				log.Printf("range: part %d produced -> %s", part, outDir)
			}
		}()
	}

	// Feed parts; stop early once the context is canceled by the breaker.
	go func() {
		defer close(parts)
		for _, part := range class {
			select {
			case <-ctx.Done():
				return
			case parts <- part:
			}
		}
	}()

	wg.Wait()
	return sum
}

// partOpener downloads/opens one part's WARC input (the local lane forces warcinput.ModeWholeFile).
// In production it wraps openInput; tests inject a fixture-backed opener.
type partOpener func(ctx context.Context, part uint32, outDir string) (preparedPart, error)

// partProcessor runs the post-open remainder for one prepared part. In production it wraps
// processInput (whose deferred cleanup closes the input and removes its temp dir).
type partProcessor func(ctx context.Context, prepared preparedPart) (partResult, error)

// downloadedPart carries a downloaded (opened) part from the download pool to the process pool. The
// part index rides alongside the preparedPart so the processor can write its marker / report failure.
type downloadedPart struct {
	part      uint32
	prepared  preparedPart
	startedAt time.Time // when this part's download began, for the marker's duration_s
}

// cleanupPrepared releases a preparedPart that was DROPPED between the download and process stages
// (breaker trip / context cancel) and so will never reach processInput's deferred cleanup: it closes
// the input (removing the downloaded whole-WARC file) and removes the .warc-input temp dir. Safe to
// call on a dropped part exactly once; a part handed to processInput must NOT also be cleaned here.
func cleanupPrepared(p preparedPart) {
	if p.input != nil {
		if err := p.input.Close(); err != nil {
			log.Printf("range local: close dropped WARC input: %v", err)
		}
	}
	if p.warcTempDirectory != "" {
		if err := os.RemoveAll(p.warcTempDirectory); err != nil {
			log.Printf("range local: remove dropped WARC temp dir %s: %v", p.warcTempDirectory, err)
		}
	}
}

// runRangeLocalPool runs the local (whole-WARC download) lane as a bounded two-stage pipeline.
//
// A SINGLE slots semaphore (capacity maxWARCFiles) is the on-disk WARC-file bound: one token is
// acquired BEFORE a part's download begins and released only after the part finishes processing AND
// its temp dir is removed (or immediately after cleanup on skip/failure/drop). So in-flight +
// buffered whole WARC files can never exceed maxWARCFiles — this semaphore, not any channel buffer,
// IS the --max-warc-files guarantee (the handoff channel is deliberately unbuffered).
//
// downloadParallel goroutines pull parts, honor the .produced marker (skip), preserve a
// complete-but-unmarked output (.loaded) as skipped, remove a stale output dir left by a crashed
// produce, then open the input as ModeWholeFile and hand it to processParallel
// goroutines. Processors run processInput (its defer closes the input + removes the temp dir), write
// the .produced marker, and release the slot. A prepared part dropped between stages (breaker trip /
// cancel) is closed and its temp dir removed via cleanupPrepared before its slot is released. The
// shared consecutive-failure counter trips the same breaker as the remote pool. Returns the tally —
// no printing, no os.Exit.
func runRangeLocalPool(
	ctx context.Context,
	class []uint32,
	downloadParallel, processParallel, maxWARCFiles int,
	cmd, runID string,
	outDirFor func(uint32) string,
	open partOpener,
	process partProcessor,
) rangeSummary {
	if downloadParallel < 1 {
		downloadParallel = 1
	}
	if downloadParallel > len(class) {
		downloadParallel = len(class)
	}
	if processParallel < 1 {
		processParallel = 1
	}
	if maxWARCFiles < 1 {
		maxWARCFiles = 1
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slots := make(chan struct{}, maxWARCFiles)
	parts := make(chan uint32)
	ready := make(chan downloadedPart) // unbuffered: the slot cap, not a buffer, bounds disk

	var mu sync.Mutex
	var sum rangeSummary
	consecutive := 0

	// fail records a part failure under the lock and trips + cancels the breaker at the limit.
	fail := func(part uint32, err error) {
		mu.Lock()
		sum.Failed++
		sum.FailedParts = append(sum.FailedParts, part)
		consecutive++
		tripped := consecutive >= consecutiveFailureLimit
		if tripped {
			sum.Breaker = true
		}
		mu.Unlock()
		log.Printf("range local: part %d FAILED: %v", part, err)
		if tripped {
			cancel()
		}
	}

	// Feed parts; stop early once the breaker cancels.
	go func() {
		defer close(parts)
		for _, part := range class {
			select {
			case <-ctx.Done():
				return
			case parts <- part:
			}
		}
	}()

	// DOWNLOAD pool.
	var downloaders sync.WaitGroup
	for i := 0; i < downloadParallel; i++ {
		downloaders.Add(1)
		go func() {
			defer downloaders.Done()
			for part := range parts {
				if ctx.Err() != nil {
					return
				}
				outDir := outDirFor(part)

				// One token per WARC file on disk: acquire BEFORE the download starts.
				select {
				case <-ctx.Done():
					return
				case slots <- struct{}{}:
				}

				if markers.Exists(markers.ProducedPath(outDir)) {
					mu.Lock()
					sum.Skipped++
					mu.Unlock()
					<-slots
					continue
				}
				// A non-empty output dir with no .produced marker is USUALLY debris from a crashed produce
				// — remove it so the download starts clean (openInput recreates the dir). But a
				// complete-but-unmarked output (.loaded from cc-crawl) is authoritative: preserve it and
				// skip the part rather than wiping loaded data. (tech/both never hit the embed branch.)
				if info, statErr := os.Stat(outDir); statErr == nil && info.IsDir() {
					if preserveStaleDir(cmd, outDir) {
						log.Printf("range local: preserving complete-but-unmarked output dir (skip) part=%d %s", part, outDir)
						mu.Lock()
						sum.Skipped++
						mu.Unlock()
						<-slots
						continue
					}
					log.Printf("range local: removing stale output dir (crashed produce?) part=%d %s", part, outDir)
					if rmErr := os.RemoveAll(outDir); rmErr != nil {
						log.Printf("range local: remove stale output dir part=%d: %v", part, rmErr)
					}
				}

				partStart := time.Now()
				prepared, err := open(ctx, part, outDir)
				if err != nil {
					fail(part, err)
					<-slots
					continue
				}

				select {
				case <-ctx.Done():
					// Dropped before any processor took it: close + remove temp, then release the slot.
					cleanupPrepared(prepared)
					<-slots
					return
				case ready <- downloadedPart{part: part, prepared: prepared, startedAt: partStart}:
					// Ownership (and the slot) transfers to the processor.
				}
			}
		}()
	}

	// Close ready once every downloader has stopped so processors drain and exit.
	go func() {
		downloaders.Wait()
		close(ready)
	}()

	// PROCESS pool.
	var processors sync.WaitGroup
	for i := 0; i < processParallel; i++ {
		processors.Add(1)
		go func() {
			defer processors.Done()
			for dp := range ready {
				// Breaker already tripped: drop this downloaded part without processing.
				if ctx.Err() != nil {
					cleanupPrepared(dp.prepared)
					<-slots
					continue
				}
				res, err := process(ctx, dp.prepared) // processInput's defer closes input + removes temp dir
				if err != nil {
					fail(dp.part, err)
					<-slots
					continue
				}
				if merr := markers.WriteProduced(dp.prepared.outDir, markers.Produced{
					Part:        dp.part,
					Cmd:         cmd,
					Rows:        res.Rows,
					SourceRunID: runID,
					DurationS:   time.Since(dp.startedAt).Seconds(),
					FinishedAt:  time.Now().UTC(),
				}); merr != nil {
					fail(dp.part, fmt.Errorf("write produced marker: %w", merr))
					<-slots
					continue
				}
				mu.Lock()
				consecutive = 0
				sum.Produced++
				mu.Unlock()
				<-slots
				log.Printf("range local: part %d produced -> %s", dp.part, dp.prepared.outDir)
			}
		}()
	}

	processors.Wait()
	return sum
}

// runRange is the CLI entry for `<cmd> --parts A-B --mode local|remote`: resolve and (if configured)
// sync the catalog, classify the range, pick this command's parts (local lane = the large whole-WARC
// parts, remote lane = the range-read parts), build the shared deps once, run the lane's bounded pool
// (the two-stage download/process pipeline for local, the single pool for remote), print the summary,
// and exit non-zero if any part failed (breaker included).
func runRange(cmd string, o opts, ro runnerOpts) {
	ctx := context.Background()
	if o.base == "" {
		log.Fatal("no --base / OUT_BASE_DIR — the output root is required")
	}
	base, err := filepath.Abs(o.base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", o.base, err)
	}
	o.base = base

	lo, hi := ro.parts.lo, ro.parts.hi
	catalogS3Base := strings.TrimSpace(os.Getenv("COMMONCRAWL_CATALOG_S3_BASE"))
	if catalogS3Base != "" {
		// Sync the committed RustFS catalog into the local cache up front (side effect only), so
		// LoadPartStats and every per-part LoadPlan read a present local catalog. A "requested index
		// absent from the catalog" error is expected when lo itself has no selected pages; any other
		// error (bad credentials, unreachable RustFS, corrupt commit) is a fatal setup failure.
		_, planErr := warcinput.LoadS3Plan(
			ctx,
			catalog.S3Config{
				BaseURI:   catalogS3Base,
				Endpoint:  os.Getenv("CORPSCOUT_S3_ENDPOINT"),
				Region:    envOr("CORPSCOUT_S3_REGION", "us-east-1"),
				AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
				SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
			},
			o.base, o.crawlID, o.selection, lo, false,
		)
		if planErr != nil && !errors.Is(planErr, catalog.ErrWARCIndexAbsent) {
			log.Fatalf("sync catalog cache: %v", planErr)
		}
	}

	catalogPath := filepath.Join(o.base, o.crawlID, "warc-index", o.selection, "catalog.duckdb")
	stats, err := catalog.LoadPartStats(ctx, catalogPath, lo, hi)
	if err != nil {
		log.Fatalf("load part stats from %s: %v", catalogPath, err)
	}
	classification := catalog.ClassifyParts(stats, lo, hi, ro.remoteMaxPages)
	// The local lane owns the large (whole-WARC download) parts; the remote lane owns the range-read
	// parts (selectClass). Task 1 validation already rejects industry/embed + local, so only tech/both
	// reach the local branch.
	var class []uint32
	if ro.mode == "local" {
		class = append([]uint32(nil), classification.Local...)
	} else {
		class = selectClass(cmd, classification)
	}

	fmt.Printf("mode=%s X=%d parts=%d local=%d remote=%d empty=%d\n",
		ro.mode, ro.remoteMaxPages, len(class), len(classification.Local), len(classification.Remote), len(classification.Empty))

	if len(class) == 0 {
		log.Printf("range: nothing to do — no parts to process in [%d,%d]", lo, hi)
		return
	}

	// Parts-level parallelism sizes the shared transport (fetchConcurrencyFor). Remote: every part
	// range-reads concurrently, so warcParallel parts genuinely contend (spec §2). Local: parts are
	// processed from LOCAL downloaded files (ReadAt, no S3), so only the in-flight whole-WARC
	// downloads touch the transport — downloadParallel is the honest S3 parts-parallelism there.
	partsParallel := ro.warcParallel
	if ro.mode == "local" {
		partsParallel = ro.downloadParallel
	}
	deps, err := buildPartDeps(ctx, cmd, o, partsParallel)
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
	var sum rangeSummary
	if ro.mode == "local" {
		open := func(ctx context.Context, part uint32, outDir string) (preparedPart, error) {
			return openInput(ctx, deps, part, warcinput.ModeWholeFile, outDir)
		}
		process := func(ctx context.Context, prepared preparedPart) (partResult, error) {
			return processInput(ctx, deps, prepared)
		}
		sum = runRangeLocalPool(ctx, class, ro.downloadParallel, ro.processParallel, ro.maxWARCFiles, cmd, runID, outDirFor, open, process)
	} else {
		produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
			return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
		}
		sum = runRangePool(ctx, class, ro.warcParallel, cmd, runID, outDirFor, produce)
	}
	elapsed := time.Since(start).Round(time.Second)

	partsMsg := ""
	if len(sum.FailedParts) > 0 {
		partsMsg = fmt.Sprintf(" [failed parts: %s]", joinParts(sum.FailedParts))
	}
	fmt.Printf("produced=%d skipped=%d failed=%d%s elapsed=%s\n",
		sum.Produced, sum.Skipped, sum.Failed, partsMsg, elapsed)

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

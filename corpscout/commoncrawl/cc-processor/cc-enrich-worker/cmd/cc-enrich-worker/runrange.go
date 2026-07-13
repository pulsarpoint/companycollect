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

// runRangePool consumes class over min(warcParallel, len(class)) worker goroutines. Per part it
// honors the .produced marker (skip), removes a stale output dir left by a crashed produce, runs the
// producer, and on success writes the .produced marker with the row counts. A shared consecutive-
// failure counter trips the breaker (cancels the context) at consecutiveFailureLimit; parts not yet
// started are neither run nor marked. It returns the tally — no printing, no os.Exit.
func runRangePool(
	ctx context.Context,
	class []uint32,
	warcParallel int,
	cmd string,
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
				// A non-empty output dir with no marker is the debris of a produce that crashed mid-write.
				// Remove it so producePart starts from a clean slate.
				if info, statErr := os.Stat(outDir); statErr == nil && info.IsDir() {
					log.Printf("range: removing stale output dir (crashed produce?) part=%d %s", part, outDir)
					if rmErr := os.RemoveAll(outDir); rmErr != nil {
						log.Printf("range: remove stale output dir part=%d: %v", part, rmErr)
					}
				}

				res, perr := produce(ctx, part, outDir)
				if perr == nil {
					perr = markers.WriteProduced(outDir, markers.Produced{
						Part:       part,
						Cmd:        cmd,
						Rows:       res.Rows,
						FinishedAt: time.Now().UTC(),
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

// runRange is the CLI entry for `<cmd> --parts A-B --mode remote`: resolve and (if configured) sync
// the catalog, classify the range, pick this command's parts, build the shared deps once, run the
// bounded pool, print the summary, and exit non-zero if any part failed (breaker included).
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
		if planErr != nil && !strings.Contains(planErr.Error(), "is absent from the catalog") {
			log.Fatalf("sync catalog cache: %v", planErr)
		}
	}

	catalogPath := filepath.Join(o.base, o.crawlID, "warc-index", o.selection, "catalog.duckdb")
	stats, err := catalog.LoadPartStats(ctx, catalogPath, lo, hi)
	if err != nil {
		log.Fatalf("load part stats from %s: %v", catalogPath, err)
	}
	classification := catalog.ClassifyParts(stats, lo, hi, ro.remoteMaxPages)
	class := selectClass(cmd, classification)

	fmt.Printf("mode=remote X=%d parts=%d local=%d remote=%d empty=%d\n",
		ro.remoteMaxPages, len(class), len(classification.Local), len(classification.Remote), len(classification.Empty))

	if len(class) == 0 {
		log.Printf("range: nothing to do — no parts to process in [%d,%d]", lo, hi)
		return
	}

	deps, err := buildPartDeps(ctx, cmd, o)
	if err != nil {
		log.Fatalf("%v", err)
	}

	outDirFor := func(part uint32) string {
		return filepath.Join(o.base, o.crawlID, "warc", o.selection, fmt.Sprintf("out_%s_%d", cmd, part))
	}
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, warcinput.ModeRange, outDir)
	}

	start := time.Now()
	sum := runRangePool(ctx, class, ro.warcParallel, cmd, outDirFor, produce)
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

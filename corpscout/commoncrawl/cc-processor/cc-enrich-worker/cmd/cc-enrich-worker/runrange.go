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
// wraps producePart (range reads); tests inject a fixture-backed producer.
type partProducer func(ctx context.Context, part uint32, outDir string) (partResult, error)

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
	produce := func(ctx context.Context, part uint32, outDir string) (partResult, error) {
		return producePart(ctx, deps, part, outDir)
	}
	sum := runRangePool(ctx, class, ro.warcParallel, cmd, runID, outDirFor, produce)
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

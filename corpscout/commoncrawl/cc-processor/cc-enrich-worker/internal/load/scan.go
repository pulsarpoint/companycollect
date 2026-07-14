package load

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"cc-enrich-worker/internal/markers"
)

// ScanResult summarises one Sweep pass over a producer output root.
//
//	Pending    output dirs found needing load (== Loaded+Failed for the pass)
//	Loaded     dirs that loaded and verified, now marked .loaded
//	Failed     dirs whose load or row-count verification failed (no .loaded written)
//	Skipped    produced dirs deliberately not loaded (embed-only output — embeddings.parquet is
//	           not a loadable kind, so it stays produced-but-unloaded forever by design)
//	Pruned     output dirs whose Parquet was removed after a verified load (only with --delete-loaded):
//	           after-load deletes + catch-up deletes of pre-existing .produced+.loaded leftovers
//	FailedDirs the output dirs behind Failed, for logging/retry
type ScanResult struct {
	Loaded, Pending, Failed, Skipped, Pruned int
	FailedDirs                               []string
}

// loadFunc loads every output parquet in dir. Sweep injects load.FromDir; tests inject a fake.
type loadFunc func(ctx context.Context, dir string) ([]Result, error)

// scanDirs walks root once and classifies every output dir carrying a sibling ".produced" marker by
// whether its ".loaded" marker also exists: pending (produced, not yet loaded) vs loaded (both
// markers present). Markers are siblings of the dir they describe (<dir>.produced / <dir>.loaded),
// so the dir is the ".produced" path with the suffix trimmed.
//
// scanDirs does NOT inspect marker contents: pending holds EVERY produced-but-unloaded dir, including
// embed-only ones (sweep classifies and skips those). loaded is only consumed by the --delete-loaded
// catch-up prune; the plain load path ignores it.
func scanDirs(root string) (pending, loaded []string, err error) {
	werr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".produced") {
			return nil
		}
		out := strings.TrimSuffix(path, ".produced")
		if markers.Exists(markers.LoadedPath(out)) {
			loaded = append(loaded, out)
			return nil
		}
		pending = append(pending, out)
		return nil
	})
	if werr != nil {
		return nil, nil, werr
	}
	return pending, loaded, nil
}

// findPending returns each output dir whose ".produced" marker exists but whose ".loaded" does not.
// It is the pending half of scanDirs, kept as a stable pure-filesystem predicate that tests rely on.
func findPending(root string) ([]string, error) {
	pending, _, err := scanDirs(root)
	return pending, err
}

// Sweep finds every produced-but-not-loaded output dir under root, loads each into ClickHouse via
// FromDir (up to parallel concurrently), verifies the loaded row counts against each dir's .produced
// marker, and writes .loaded on success. Per-dir failures are logged and counted but never abort the
// sweep. Returns an error only when the directory walk itself fails.
//
// deleteLoaded enables parquet reclamation (see sweepPrune): after a dir loads+verifies its output
// directory is removed, and any pre-existing already-loaded dir is caught up. The durable .produced /
// .loaded markers are siblings of the dir and are never removed.
func Sweep(ctx context.Context, conn driver.Conn, root string, parallel int, deleteLoaded bool) (ScanResult, error) {
	return sweepPrune(ctx, root, parallel, deleteLoaded, func(ctx context.Context, dir string) ([]Result, error) {
		return FromDir(ctx, conn, dir)
	})
}

// sweep is the pruning-free entry point, retained so existing unit tests keep the 4-arg loadFn seam.
// It delegates with deleteLoaded=false.
func sweep(ctx context.Context, root string, parallel int, load loadFunc) (ScanResult, error) {
	return sweepPrune(ctx, root, parallel, false, load)
}

// sweepPrune is Sweep's implementation with the loadFn injected for CH-free unit tests.
//
// When deleteLoaded is true the sweep reclaims local Parquet after ClickHouse has the data:
//   - after-load delete: a dir that loads+verifies has os.RemoveAll(dir) called STRICTLY AFTER
//     markers.WriteLoaded succeeded (inside loadAndVerify). A crash between the write and the delete is
//     harmless: the next sweep sees .loaded and treats the leftover dir as a catch-up prune below.
//   - catch-up prune: any still-existing dir that ALREADY has both .produced and .loaded (leftovers
//     from before the flag, or from that crash window) is removed. This makes a --watch daemon
//     self-cleaning and reclaims historical disk on first flagged run.
//
// Markers are NEVER removed: they are siblings (dir+".produced"/".loaded") in the PARENT dir, so
// RemoveAll(dir) cannot reach them; they remain the range runner's resume record and status's ledger.
// Embed output is never a prune target: embed dirs are partitioned into Skipped (never .loaded), and
// catch-up prune keys purely on .loaded, so an embed dir with only .produced always survives.
//
// A prune removal failure (e.g. permission) is logged and does NOT increment Pruned, but is
// deliberately NOT a load failure: the data is already in ClickHouse and .loaded is written, so the
// load succeeded — only disk reclamation is deferred to a future sweep.
func sweepPrune(ctx context.Context, root string, parallel int, deleteLoaded bool, load loadFunc) (ScanResult, error) {
	found, loaded, err := scanDirs(root)
	if err != nil {
		return ScanResult{}, err
	}
	// Partition out embed-only produced dirs: their sole artifact is embeddings.parquet, which is not
	// in load.Kinds, so FromDir can never load it. Without this every embed dir would fail on EVERY
	// sweep, permanently failing `load --scan` (cron exits 1 forever). They are skipped, not failed:
	// the produced-but-unloaded state is correct and final for embed output.
	var pending []string
	var skipped int
	for _, dir := range found {
		if p, perr := markers.ReadProduced(dir); perr == nil && p.Cmd == "embed" {
			log.Printf("load sweep: skipping embed-only output %s (embeddings.parquet is not a loadable kind)", dir)
			skipped++
			continue
		}
		pending = append(pending, dir)
	}
	res := ScanResult{Pending: len(pending), Skipped: skipped}

	// Catch-up prune: reclaim already-loaded leftovers (from before the flag or a crash window).
	if deleteLoaded {
		for _, dir := range loaded {
			if _, statErr := os.Stat(dir); os.IsNotExist(statErr) {
				continue // already reclaimed by an earlier sweep — nothing to do
			}
			if prune(dir) {
				res.Pruned++
			}
		}
	}

	if len(pending) == 0 {
		return res, nil
	}
	if parallel < 1 {
		parallel = 1
	}

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, parallel)
	)
	for _, dir := range pending {
		wg.Add(1)
		sem <- struct{}{}
		go func(dir string) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := loadAndVerify(ctx, dir, load); err != nil {
				log.Printf("load sweep: %s failed: %v", dir, err)
				mu.Lock()
				res.Failed++
				res.FailedDirs = append(res.FailedDirs, dir)
				mu.Unlock()
				return
			}
			// After-load delete: .loaded is now written (inside loadAndVerify), so removing the dir is
			// safe — a crash before this line just defers reclamation to the next sweep's catch-up.
			pruned := deleteLoaded && prune(dir)
			mu.Lock()
			res.Loaded++
			if pruned {
				res.Pruned++
			}
			mu.Unlock()
		}(dir)
	}
	wg.Wait()
	return res, nil
}

// prune removes a loaded output dir's Parquet, reclaiming disk. It reports whether the dir was
// removed. A removal failure is logged and returns false (not counted, not fatal to the load): the
// data is in ClickHouse and .loaded is written, so the next sweep's catch-up retries the delete. The
// .produced/.loaded markers are siblings of dir and are untouched by os.RemoveAll(dir).
func prune(dir string) bool {
	if err := os.RemoveAll(dir); err != nil {
		log.Printf("load sweep: prune %s failed: %v (data is loaded; retry on next sweep)", dir, err)
		return false
	}
	return true
}

// loadAndVerify reads dir's .produced marker, loads the dir, checks that per-kind loaded rows meet
// the marker's recorded counts, then writes .loaded. Any step failing returns an error and leaves
// .loaded unwritten.
func loadAndVerify(ctx context.Context, dir string, load loadFunc) error {
	produced, err := markers.ReadProduced(dir)
	if err != nil {
		return fmt.Errorf("read produced marker: %w", err)
	}
	results, err := load(ctx, dir)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	// Sum loaded rows per kind (kind = the parquet basename FromDir loaded from).
	loaded := make(map[string]int, len(results))
	for _, r := range results {
		kind := strings.TrimSuffix(filepath.Base(r.Path), ".parquet")
		loaded[kind] += r.Rows
	}
	for kind, want := range produced.Rows {
		if loaded[kind] < want {
			return fmt.Errorf("verify: %s loaded %d rows < %d recorded in marker", kind, loaded[kind], want)
		}
	}
	if err := markers.WriteLoaded(dir); err != nil {
		return fmt.Errorf("write loaded marker: %w", err)
	}
	return nil
}

package load

import (
	"context"
	"fmt"
	"io/fs"
	"log"
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
//	FailedDirs the output dirs behind Failed, for logging/retry
type ScanResult struct {
	Loaded, Pending, Failed int
	FailedDirs              []string
}

// loadFunc loads every output parquet in dir. Sweep injects load.FromDir; tests inject a fake.
type loadFunc func(ctx context.Context, dir string) ([]Result, error)

// findPending walks root and returns each output dir whose sibling ".produced" marker exists but
// whose ".loaded" marker does not. Markers are siblings of the dir they describe
// (<dir>.produced / <dir>.loaded), so the dir is the ".produced" path with the suffix trimmed.
func findPending(root string) ([]string, error) {
	var pending []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".produced") {
			return nil
		}
		out := strings.TrimSuffix(path, ".produced")
		if markers.Exists(markers.LoadedPath(out)) {
			return nil
		}
		pending = append(pending, out)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return pending, nil
}

// Sweep finds every produced-but-not-loaded output dir under root, loads each into ClickHouse via
// FromDir (up to parallel concurrently), verifies the loaded row counts against each dir's .produced
// marker, and writes .loaded on success. Per-dir failures are logged and counted but never abort the
// sweep. Returns an error only when the directory walk itself fails.
func Sweep(ctx context.Context, conn driver.Conn, root string, parallel int) (ScanResult, error) {
	return sweep(ctx, root, parallel, func(ctx context.Context, dir string) ([]Result, error) {
		return FromDir(ctx, conn, dir)
	})
}

func sweep(ctx context.Context, root string, parallel int, load loadFunc) (ScanResult, error) {
	pending, err := findPending(root)
	if err != nil {
		return ScanResult{}, err
	}
	res := ScanResult{Pending: len(pending)}
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
			mu.Lock()
			res.Loaded++
			mu.Unlock()
		}(dir)
	}
	wg.Wait()
	return res, nil
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

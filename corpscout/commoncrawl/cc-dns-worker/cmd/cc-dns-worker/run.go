package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"cc-dns-worker/internal/load"
	"cc-dns-worker/internal/store"
)

// cycleState is persisted (orchestrator-state.json) so a restart resumes the current cycle's phase
// instead of starting a new scan. A new cycle is minted only once the current one is fully done.
// The db path is NOT stored — it's always derived from CycleID (dbName) so there's one source of truth.
type cycleState struct {
	CycleID string `json:"cycle_id"`
	Phase   string `json:"phase"` // seeding | scanning | flushing | done
}

const (
	phaseSeeding  = "seeding"
	phaseScanning = "scanning"
	phaseAXFR     = "axfr"
	phaseFlushing = "flushing"
	phaseDone     = "done"
)

// dbName derives the SQLite stage filename for a cycle from its id.
func dbName(cycleID string) string { return "scan-" + cycleID + ".db" }

// runOrchestrator loops forever: mint-or-resume a cycle, seed it (CH stream + host-load), scan it
// (incrementally loading to CH as it goes), final-flush the load, prune old DBs, then start the next
// cycle. Designed as the systemd ExecStart (Restart=always as a backstop). Every phase is crash-safe:
// seed resumes from its completion markers, scan resumes from the pending queue, load resumes from a
// rowid watermark and dedups in CH.
func runOrchestrator(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dir := fs.String("dir", ".", "working directory for cycle DBs and the state file")
	build := scanFlags(fs)
	_ = fs.Parse(args)
	cfg, err := build()
	if err != nil {
		return err
	}
	statePath := filepath.Join(*dir, "orchestrator-state.json")
	ctx := context.Background()

	for {
		state, err := loadOrStartCycle(statePath)
		if err != nil {
			return err
		}
		dbPath := filepath.Join(*dir, dbName(state.CycleID))
		st, err := store.Open(dbPath)
		if err != nil {
			return err
		}
		cfg.scanID = state.CycleID
		cfg.runID = state.CycleID
		err = runBoundedCycle(ctx, st, cfg)
		_ = st.Close()
		if err != nil {
			log.Printf("cycle %s: bounded engine error: %v — retrying in 30s", state.CycleID, err)
			time.Sleep(30 * time.Second)
			continue
		}
		log.Printf("cycle %s: complete", state.CycleID)
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(dbPath + suffix)
		}
		_ = os.Remove(statePath)
	}
}

// loadOrStartCycle resumes a persisted, not-yet-done cycle, or mints a new one keyed by UTC start
// time (db = scan-<ts>.db). Minting a new cycle only happens when there is no in-progress state.
func loadOrStartCycle(statePath string) (cycleState, error) {
	if b, err := os.ReadFile(statePath); err == nil {
		var s cycleState
		if json.Unmarshal(b, &s) == nil && s.CycleID != "" && s.Phase != phaseDone {
			log.Printf("resuming cycle %s (phase=%s, db=%s)", s.CycleID, s.Phase, dbName(s.CycleID))
			return s, nil
		}
	}
	s := cycleState{CycleID: time.Now().UTC().Format("20060102T150405Z"), Phase: phaseSeeding}
	if err := saveState(statePath, s); err != nil {
		return cycleState{}, err
	}
	log.Printf("starting new cycle %s (db=%s)", s.CycleID, dbName(s.CycleID))
	return s, nil
}

// saveState writes the state file atomically (temp + rename).
func saveState(path string, s cycleState) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// runSeedPhase runs the SEEDING phase: stream domains from CH into the queue and, unless explicitly
// disabled, run the CT+registry host-load. No records exist yet, so no incremental loader runs — this
// is a distinct phase so status reflects the long seed/host-load window rather than reading "scanning"
// throughout.
func runSeedPhase(ctx context.Context, state cycleState, dbPath string, cfg scanConfig) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	cfg.scanID = state.CycleID
	cfg.runID = state.CycleID
	return seedCycle(ctx, st, cfg)
}

// runAXFRPhase runs the AXFR phase after scanning: stage+probe every resolved domain's non-hyperscaler
// nameservers for open zone transfers (reusing the ns_endpoints/ns_ips resolution already stored), then
// the AXFR ClickHouse load pass — both via the shared axfrCycle (axfr.go), the SAME function `scan
// --axfr` calls. Separate from resolution entirely; runs by default unless AXFR is explicitly disabled
// (runOrchestrator enters this phase when cfg.axfr is true).
func runAXFRPhase(ctx context.Context, state cycleState, dbPath string, cfg scanConfig) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	cfg.scanID = state.CycleID
	cfg.runID = state.CycleID
	return axfrCycle(ctx, st, cfg, axfrLoad)
}

// runScanPhase scans the cycle while a background goroutine incrementally loads new records to CH.
// The loader shares the store (SQLite serializes the few DB ops; the slow CH insert holds no lock).
// Seeding + host-load already completed in runSeedPhase, so this resolves the pending queue only.
func runScanPhase(ctx context.Context, state cycleState, dbPath string, cfg scanConfig, loadInterval time.Duration, loadBatch int) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	cfg.scanID = state.CycleID
	cfg.runID = state.CycleID

	stopLoader := make(chan struct{})
	loaderDone := make(chan struct{})
	go func() {
		defer close(loaderDone)
		ticker := time.NewTicker(loadInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopLoader:
				return
			case <-ticker.C:
				nr, nd, err := incLoad(ctx, st, state.CycleID, loadBatch)
				if err != nil {
					log.Printf("cycle %s: incremental load error: %v", state.CycleID, err)
				} else if nr > 0 || nd > 0 {
					log.Printf("cycle %s: incrementally loaded %d records and %d domain summaries to ClickHouse", state.CycleID, nr, nd)
				}
			}
		}
	}()

	scanErr := scanResolve(ctx, st, cfg)
	close(stopLoader)
	<-loaderDone
	return scanErr
}

// runFlushPhase does the final incremental load after the scan finished (both watermarks → end), then
// the hostname registry write-back. The AXFR ClickHouse load pass no longer happens here — it now runs
// as part of the AXFR phase itself (runAXFRPhase, via the shared axfrCycle), so a cycle with --axfr on
// has already loaded its AXFR results by the time it reaches this phase.
//
// A hostname-registry write-back failure is treated as a RETRYABLE flush failure, exactly like a load
// failure: it is returned, not merely logged, so runOrchestrator's phaseFlushing branch neither advances
// the cycle to phaseDone nor reaches pruneOldDBs (both live in the caller's loop, after this function
// returns nil) — the cycle instead retries the whole flush phase (idempotent: both loads resume from
// their watermarks, and the registry write-back recomputes identical rows from stable staged data). Only
// once records, domain summaries, AND the registry all succeed does the cycle get marked done and old
// DBs get pruned.
func runFlushPhase(ctx context.Context, state cycleState, dbPath string, loadBatch int) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	nr, nd, err := incLoad(ctx, st, state.CycleID, loadBatch)
	if err != nil {
		return fmt.Errorf("incremental load: %w", err)
	}
	log.Printf("cycle %s: final flush loaded %d records and %d domain summaries to ClickHouse", state.CycleID, nr, nd)
	hn, err := regWriteBack(ctx, st, state.CycleID)
	if err != nil {
		return fmt.Errorf("hostname registry write-back: %w", err)
	}
	if hn > 0 {
		log.Printf("cycle %s: registered %d discovered hostnames", state.CycleID, hn)
	}
	return nil
}

// incLoad opens a fresh ClickHouse connection and runs one incremental load pass (records + domain
// summaries committed since their respective watermarks). A fresh conn per pass reconnects cleanly if CH
// was briefly down.
func incLoad(ctx context.Context, st *store.Store, scanID string, loadBatch int) (int, int, error) {
	conn, err := chConn()
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()
	return load.Incremental(ctx, conn, st, scanID, loadBatch)
}

// regWriteBack opens a fresh ClickHouse connection and upserts the cycle's discovered hostnames into
// the durable registry.
func regWriteBack(ctx context.Context, st *store.Store, scanID string) (int, error) {
	conn, err := chConn()
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	return load.WriteHostnameRegistry(ctx, conn, st, scanID)
}

// pruneOldDBs keeps the newest keep+1 cycle DBs (current + keep previous) and deletes older ones
// (with their -wal/-shm sidecars). Timestamped names sort chronologically.
func pruneOldDBs(dir string, keep int) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var dbs []string
	for _, e := range entries {
		n := e.Name()
		if strings.HasPrefix(n, "scan-") && strings.HasSuffix(n, ".db") {
			dbs = append(dbs, n)
		}
	}
	sort.Strings(dbs)
	keepN := keep + 1
	if len(dbs) <= keepN {
		return
	}
	for _, n := range dbs[:len(dbs)-keepN] {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(filepath.Join(dir, n+suffix))
		}
		log.Printf("pruned old cycle db %s", n)
	}
}

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
	loadInterval := fs.Duration("load-interval", 30*time.Minute, "how often to incrementally load the running cycle to ClickHouse")
	loadBatch := fs.Int("load-batch", 20000, "records per incremental-load batch")
	keepDBs := fs.Int("keep-dbs", 1, "previous cycle DBs to keep after loading (older are deleted)")
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

		if state.Phase == phaseSeeding {
			if err := runSeedPhase(ctx, state, dbPath, cfg); err != nil {
				log.Printf("cycle %s: seed phase error: %v — retrying in 30s (resumes, no new cycle)", state.CycleID, err)
				time.Sleep(30 * time.Second)
				continue
			}
			state.Phase = phaseScanning
			if err := saveState(statePath, state); err != nil {
				return err
			}
		}

		if state.Phase == phaseScanning {
			if err := runScanPhase(ctx, state, dbPath, cfg, *loadInterval, *loadBatch); err != nil {
				log.Printf("cycle %s: scan phase error: %v — retrying in 30s (resumes, no new cycle)", state.CycleID, err)
				time.Sleep(30 * time.Second)
				continue
			}
			if cfg.axfr {
				state.Phase = phaseAXFR
			} else {
				state.Phase = phaseFlushing
			}
			if err := saveState(statePath, state); err != nil {
				return err
			}
		}

		if state.Phase == phaseAXFR {
			if err := runAXFRPhase(ctx, state, dbPath, cfg); err != nil {
				log.Printf("cycle %s: axfr phase error: %v — retrying in 30s (resumes, no new cycle)", state.CycleID, err)
				time.Sleep(30 * time.Second)
				continue
			}
			state.Phase = phaseFlushing
			if err := saveState(statePath, state); err != nil {
				return err
			}
		}

		if state.Phase == phaseFlushing {
			if err := runFlushPhase(ctx, state, dbPath, *loadBatch, cfg.axfr); err != nil {
				log.Printf("cycle %s: flush phase error: %v — retrying in 30s", state.CycleID, err)
				time.Sleep(30 * time.Second)
				continue
			}
			state.Phase = phaseDone
			if err := saveState(statePath, state); err != nil {
				return err
			}
		}

		// Done: prune old DBs and clear state so the next iteration mints a fresh cycle.
		pruneOldDBs(*dir, *keepDBs)
		log.Printf("cycle %s: complete", state.CycleID)
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

// runSeedPhase runs the SEEDING phase: stream domains from CH into the queue and (when --host-enrich)
// the CT+registry host-load. No records exist yet, so no incremental loader runs — this is a distinct
// phase so status reflects the long seed/host-load window rather than reading "scanning" throughout.
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

// runAXFRPhase runs the AXFR phase after scanning: a standalone worker pool probes each resolved
// domain's non-hyperscaler nameservers for open zone transfers (reusing the ns_ips resolution stored).
// Separate from resolution entirely; runs only when --axfr is set.
func runAXFRPhase(ctx context.Context, state cycleState, dbPath string, cfg scanConfig) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	cfg.scanID = state.CycleID
	cfg.runID = state.CycleID
	return runAXFRPipeline(ctx, st, cfg)
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
				if n, err := incLoad(ctx, st, state.CycleID, loadBatch); err != nil {
					log.Printf("cycle %s: incremental load error: %v", state.CycleID, err)
				} else if n > 0 {
					log.Printf("cycle %s: incrementally loaded %d records to ClickHouse", state.CycleID, n)
				}
			}
		}
	}()

	scanErr := scanResolve(ctx, st, cfg)
	close(stopLoader)
	<-loaderDone
	return scanErr
}

// runFlushPhase does the final incremental load after the scan finished (watermark → end), then the
// hostname registry write-back, then — only when the cycle ran the AXFR phase — the AXFR load pass
// (staged axfr_probes/axfr_changes → dns_axfr_latest/dns_axfr_state_changes). Skipping the AXFR load
// when --axfr was never on avoids a ClickHouse round trip every cycle for a phase that staged nothing.
func runFlushPhase(ctx context.Context, state cycleState, dbPath string, loadBatch int, axfrEnabled bool) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	n, err := incLoad(ctx, st, state.CycleID, loadBatch)
	if err != nil {
		return err
	}
	log.Printf("cycle %s: final flush loaded %d records to ClickHouse", state.CycleID, n)
	if hn, herr := regWriteBack(ctx, st, state.CycleID); herr != nil {
		log.Printf("cycle %s: hostname registry write-back error: %v", state.CycleID, herr)
	} else if hn > 0 {
		log.Printf("cycle %s: registered %d discovered hostnames", state.CycleID, hn)
	}
	if axfrEnabled {
		if err := axfrLoad(ctx, st, state.CycleID); err != nil {
			return fmt.Errorf("axfr load: %w", err)
		}
		log.Printf("cycle %s: AXFR results loaded to ClickHouse", state.CycleID)
	}
	return nil
}

// axfrLoad opens a fresh ClickHouse connection and runs one AXFR load pass (staged axfr_probes/
// axfr_changes → dns_axfr_latest/dns_axfr_state_changes) for scanID.
func axfrLoad(ctx context.Context, st *store.Store, scanID string) error {
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	return load.LoadAXFR(ctx, conn, st, scanID, time.Now())
}

// incLoad opens a fresh ClickHouse connection and runs one incremental load pass (records committed
// since the watermark). A fresh conn per pass reconnects cleanly if CH was briefly down.
func incLoad(ctx context.Context, st *store.Store, scanID string, loadBatch int) (int, error) {
	conn, err := chConn()
	if err != nil {
		return 0, err
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
	return load.WriteHostnameRegistry(ctx, conn, st, scanID, time.Now())
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

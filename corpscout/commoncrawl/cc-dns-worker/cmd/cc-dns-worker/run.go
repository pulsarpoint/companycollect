package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"cc-dns-worker/internal/store"
)

type cycleState struct {
	CycleID string `json:"cycle_id"`
}

func dbName(cycleID string) string { return "scan-" + cycleID + ".db" }

func runOrchestrator(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dir := fs.String("dir", ".", "working directory for the active cycle DB and state file")
	build := scanFlags(fs)
	_ = fs.Parse(args)
	cfg, err := build()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*dir, 0o755); err != nil {
		return fmt.Errorf("create working directory: %w", err)
	}
	statePath := filepath.Join(*dir, "orchestrator-state.json")
	ctx := context.Background()
	for {
		state, err := loadOrStartCycle(statePath)
		if err != nil {
			return err
		}
		dbPath := filepath.Join(*dir, dbName(state.CycleID))
		localStore, err := store.Open(dbPath)
		if err != nil {
			return fmt.Errorf("open cycle store: %w", err)
		}
		cfg.scanID, cfg.runID = state.CycleID, state.CycleID
		err = runBoundedCycle(ctx, localStore, cfg)
		_ = localStore.Close()
		if err != nil {
			slog.Error("cycle failed; retaining restartable local state", "scan_id", state.CycleID, "error", err)
			time.Sleep(30 * time.Second)
			continue
		}
		slog.Info("cycle complete", "scan_id", state.CycleID)
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(dbPath + suffix)
		}
		_ = os.Remove(statePath)
	}
}

func loadOrStartCycle(statePath string) (cycleState, error) {
	data, err := os.ReadFile(statePath)
	if err == nil {
		var state cycleState
		if json.Unmarshal(data, &state) == nil && state.CycleID != "" {
			slog.Info("resuming cycle", "scan_id", state.CycleID, "db", dbName(state.CycleID))
			return state, nil
		}
	} else if !os.IsNotExist(err) {
		return cycleState{}, fmt.Errorf("read cycle state: %w", err)
	}
	state := cycleState{CycleID: time.Now().UTC().Format("20060102T150405Z")}
	if err := saveState(statePath, state); err != nil {
		return cycleState{}, err
	}
	slog.Info("starting cycle", "scan_id", state.CycleID, "db", dbName(state.CycleID))
	return state, nil
}

func saveState(path string, state cycleState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode cycle state: %w", err)
	}
	temporaryPath := path + ".tmp"
	if err := os.WriteFile(temporaryPath, data, 0o644); err != nil {
		return fmt.Errorf("write cycle state: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace cycle state: %w", err)
	}
	return nil
}

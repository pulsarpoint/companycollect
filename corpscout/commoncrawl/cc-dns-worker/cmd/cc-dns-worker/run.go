package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"cc-dns-worker/internal/axfrscan"
	"cc-dns-worker/internal/dnsscan"

	"golang.org/x/sync/errgroup"
)

type cycleState struct {
	CycleID string `json:"cycle_id"`
}

func runOrchestrator(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	directory := flags.String("dir", ".", "working directory for independent DNS and AXFR state")
	build := scannerFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	config, err := build()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*directory, 0o755); err != nil {
		return fmt.Errorf("create worker directory: %w", err)
	}

	group, groupContext := errgroup.WithContext(ctx)
	if config.RunDNS {
		group.Go(func() error { return superviseDNS(groupContext, *directory, config.DNS) })
	}
	if config.RunAXFR {
		group.Go(func() error { return superviseAXFR(groupContext, *directory, config.AXFR) })
	}
	err = group.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func superviseDNS(ctx context.Context, directory string, config dnsscan.Config) error {
	statePath := filepath.Join(directory, "dns-cycle-state.json")
	for ctx.Err() == nil {
		state, err := loadOrStartCycle(statePath, "dns")
		if err != nil {
			slog.Error("DNS supervisor cannot load cycle state", "error", err)
			if err := retryWait(ctx); err != nil {
				return err
			}
			continue
		}
		config.ScanID, config.RunID = state.CycleID, state.CycleID
		databasePath := filepath.Join(directory, cycleDBName("dns", state.CycleID))
		if err := dnsscan.RunCycle(ctx, databasePath, config); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("DNS cycle failed; retaining restartable DNS state", "scan_id", state.CycleID, "error", err)
			if err := retryWait(ctx); err != nil {
				return err
			}
			continue
		}
		slog.Info("DNS cycle complete", "scan_id", state.CycleID)
		removeCycleFiles(databasePath, statePath)
	}
	return ctx.Err()
}

func superviseAXFR(ctx context.Context, directory string, config axfrscan.Config) error {
	statePath := filepath.Join(directory, "axfr-cycle-state.json")
	for ctx.Err() == nil {
		state, err := loadOrStartCycle(statePath, "axfr")
		if err != nil {
			slog.Error("AXFR supervisor cannot load cycle state", "error", err)
			if err := retryWait(ctx); err != nil {
				return err
			}
			continue
		}
		config.ScanID = state.CycleID
		databasePath := filepath.Join(directory, cycleDBName("axfr", state.CycleID))
		if err := axfrscan.RunCycle(ctx, databasePath, config); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("AXFR cycle failed; retaining restartable AXFR state", "scan_id", state.CycleID, "error", err)
			if err := retryWait(ctx); err != nil {
				return err
			}
			continue
		}
		slog.Info("AXFR cycle complete", "scan_id", state.CycleID)
		removeCycleFiles(databasePath, statePath)
	}
	return ctx.Err()
}

func cycleDBName(scanner, cycleID string) string { return scanner + "-scan-" + cycleID + ".db" }

func loadOrStartCycle(statePath, scanner string) (cycleState, error) {
	data, err := os.ReadFile(statePath)
	if err == nil {
		var state cycleState
		if json.Unmarshal(data, &state) == nil && state.CycleID != "" {
			slog.Info("resuming cycle", "component", scanner, "scan_id", state.CycleID,
				"db", cycleDBName(scanner, state.CycleID))
			return state, nil
		}
	} else if !os.IsNotExist(err) {
		return cycleState{}, fmt.Errorf("read %s cycle state: %w", scanner, err)
	}
	state := cycleState{CycleID: time.Now().UTC().Format("20060102T150405Z")}
	if err := saveState(statePath, state); err != nil {
		return cycleState{}, err
	}
	slog.Info("starting cycle", "component", scanner, "scan_id", state.CycleID,
		"db", cycleDBName(scanner, state.CycleID))
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

func removeCycleFiles(databasePath, statePath string) {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(databasePath + suffix)
	}
	_ = os.Remove(statePath)
}

func retryWait(ctx context.Context) error {
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

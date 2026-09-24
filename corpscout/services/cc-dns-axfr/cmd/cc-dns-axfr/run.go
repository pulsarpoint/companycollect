package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"cc-dns-axfr/internal/axfrscan"
	"cc-dns-axfr/internal/cyclestate"
)

const (
	axfrCycleStateFile = "axfr-cycle-state.json"
	retryInterval      = 30 * time.Second
)

func runSupervisor(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	directory := flags.String("dir", ".", "working directory for resumable AXFR state")
	buildConfig := axfrScannerFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if err := os.MkdirAll(*directory, 0o755); err != nil {
		return fmt.Errorf("create AXFR worker directory: %w", err)
	}

	err := superviseAXFR(ctx, *directory, buildConfig())
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func superviseAXFR(ctx context.Context, directory string, config axfrscan.Config) error {
	statePath := filepath.Join(directory, axfrCycleStateFile)
	for ctx.Err() == nil {
		state, resumed, err := cyclestate.LoadOrStart(statePath)
		if err != nil {
			slog.Error("AXFR supervisor cannot load cycle state", "error", err)
			if err := waitToRetry(ctx); err != nil {
				return err
			}
			continue
		}

		config.ScanID = state.CycleID
		databasePath := filepath.Join(directory, cyclestate.DatabaseName(state.CycleID))
		if resumed {
			slog.Info("resuming AXFR cycle", "scan_id", state.CycleID, "db", databasePath)
		} else {
			slog.Info("starting AXFR cycle", "scan_id", state.CycleID, "db", databasePath)
		}

		if err := axfrscan.RunCycle(ctx, databasePath, config); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("AXFR cycle failed; retaining restartable state", "scan_id", state.CycleID, "error", err)
			if err := waitToRetry(ctx); err != nil {
				return err
			}
			continue
		}

		slog.Info("AXFR cycle complete", "scan_id", state.CycleID)
		cyclestate.RemoveFiles(databasePath, statePath)
	}
	return ctx.Err()
}

func waitToRetry(ctx context.Context) error {
	timer := time.NewTimer(retryInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

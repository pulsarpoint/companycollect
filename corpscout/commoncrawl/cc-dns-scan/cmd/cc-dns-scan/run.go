package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"cc-dns-scan/internal/cyclestate"
	"cc-dns-scan/internal/dnsscan"
)

func runSupervisor(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	directory := flags.String("dir", ".", "working directory for DNS state")
	build := dnsScannerFlags(flags)
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

	err = superviseDNS(ctx, *directory, config)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func superviseDNS(ctx context.Context, directory string, config dnsscan.Config) error {
	statePath := filepath.Join(directory, "dns-cycle-state.json")
	for ctx.Err() == nil {
		state, resumed, err := cyclestate.LoadOrStart(statePath)
		if err != nil {
			slog.Error("DNS supervisor cannot load cycle state", "error", err)
			if err := retryWait(ctx); err != nil {
				return err
			}
			continue
		}
		config.ScanID, config.RunID = state.CycleID, state.CycleID
		databasePath := filepath.Join(directory, cyclestate.DatabaseName("dns", state.CycleID))
		if resumed {
			slog.Info("resuming cycle", "component", "dns", "scan_id", state.CycleID, "db", databasePath)
		} else {
			slog.Info("starting cycle", "component", "dns", "scan_id", state.CycleID, "db", databasePath)
		}
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
		cyclestate.RemoveFiles(databasePath, statePath)
	}
	return ctx.Err()
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

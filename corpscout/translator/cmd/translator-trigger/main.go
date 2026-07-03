package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/pulsarpoint/corpscout/translator/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "translator-trigger: %v\n", err)
		os.Exit(1)
	}
}

// run parses flags only. translator-trigger used to signal per-source
// workflow actions (load-and-run, run, load-queue); the shared queue has no
// per-source identity left to signal.
//
// TODO(task 8): rewrite translator-trigger to signal the single shared-queue
// workflow (engine.ProcessWorkflowID) via orchestration.TemporalWorkflowStarter.StartProcess.
func run(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("translator-trigger", flag.ContinueOnError)
	fs.SetOutput(stdout)

	_ = fs.String("config", defaultConfigPath(), "path to translator config file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	return errors.New("translator-trigger is mid-migration to the single shared-queue workflow (plan task 8); refusing to start")
}

func defaultConfigPath() string {
	if path := os.Getenv(config.ConfigFileEnv); path != "" {
		return path
	}
	return config.DefaultConfigPath
}

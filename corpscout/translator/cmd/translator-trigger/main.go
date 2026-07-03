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
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
	"go.temporal.io/sdk/client"
)

// processStarter signals (or starts) the single shared-queue translation
// workflow. *orchestration.TemporalWorkflowStarter satisfies it.
type processStarter interface {
	StartProcess(ctx context.Context) (orchestration.WorkflowActionResult, error)
}

type starterFactory func(ctx context.Context, cfg config.Config) (processStarter, func(), error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, newTemporalStarter); err != nil {
		fmt.Fprintf(os.Stderr, "translator-trigger: %v\n", err)
		os.Exit(1)
	}
}

// run loads config, signals (or starts) the single shared-queue translation
// workflow, and prints the resulting workflow/run IDs.
func run(ctx context.Context, args []string, stdout io.Writer, newStarter starterFactory) error {
	fs := flag.NewFlagSet("translator-trigger", flag.ContinueOnError)
	fs.SetOutput(stdout)

	configPath := fs.String("config", defaultConfigPath(), "path to translator config file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	starter, closeStarter, err := newStarter(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeStarter()

	result, err := starter.StartProcess(ctx)
	if err != nil {
		return fmt.Errorf("start process workflow: %w", err)
	}

	fmt.Fprintf(stdout, "workflow_id=%s run_id=%s\n", result.WorkflowID, result.RunID)
	return nil
}

func newTemporalStarter(_ context.Context, cfg config.Config) (processStarter, func(), error) {
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect temporal: %w", err)
	}

	return orchestration.NewTemporalWorkflowStarter(
			temporalClient,
			cfg.Temporal.BatchSize,
			cfg.Temporal.TimeoutSeconds,
			cfg.Temporal.BatchesPerRun,
			cfg.Queue.FlushEveryBatches,
		),
		temporalClient.Close,
		nil
}

func defaultConfigPath() string {
	if path := os.Getenv(config.ConfigFileEnv); path != "" {
		return path
	}
	return config.DefaultConfigPath
}

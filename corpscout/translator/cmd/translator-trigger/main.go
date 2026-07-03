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
	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
	"go.temporal.io/sdk/client"
)

type sourceActionStarter interface {
	StartSourceAction(ctx context.Context, source string, action string) (orchestration.WorkflowActionResult, error)
}

type starterFactory func(ctx context.Context, cfg config.Config) (sourceActionStarter, func(), error)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, newTemporalStarter); err != nil {
		fmt.Fprintf(os.Stderr, "translator-trigger: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, newStarter starterFactory) error {
	fs := flag.NewFlagSet("translator-trigger", flag.ContinueOnError)
	fs.SetOutput(stdout)

	configPath := fs.String("config", defaultConfigPath(), "path to translator config file")
	source := fs.String("source", "norway_brreg", "translation source")
	action := fs.String("action", engine.ActionLoadAndRun, "source action: load-and-run, run, or load-queue")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	if err := validateAction(*action); err != nil {
		return err
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

	result, err := starter.StartSourceAction(ctx, *source, *action)
	if err != nil {
		return fmt.Errorf("start %s action: %w", *action, err)
	}

	fmt.Fprintf(
		stdout,
		"workflow_id=%s run_id=%s source=%s action=%s\n",
		result.WorkflowID,
		result.RunID,
		*source,
		*action,
	)
	return nil
}

func newTemporalStarter(_ context.Context, cfg config.Config) (sourceActionStarter, func(), error) {
	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connect temporal: %w", err)
	}

	sources := make([]string, 0, len(cfg.Sources))
	for name := range cfg.Sources {
		sources = append(sources, name)
	}

	return orchestration.NewTemporalWorkflowStarter(
			temporalClient,
			sources,
			cfg.Temporal.BatchSize,
			cfg.Temporal.TimeoutSeconds,
			cfg.Temporal.BatchesPerRun,
		),
		temporalClient.Close,
		nil
}

func validateAction(action string) error {
	switch action {
	case engine.ActionLoadAndRun, engine.ActionLoadQueue, engine.ActionRun:
		return nil
	default:
		return fmt.Errorf("unsupported action: %s", action)
	}
}

func defaultConfigPath() string {
	if path := os.Getenv(config.ConfigFileEnv); path != "" {
		return path
	}
	return config.DefaultConfigPath
}

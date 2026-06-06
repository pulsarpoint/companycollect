package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/config"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/fixture"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/llm"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/report"
	"github.com/pulsarpoint/companycollect/data-pipelines/golang-translate/internal/runner"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(context.Background(), os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		logger.Error("golang translate benchmark failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := config.Parse(args)
	if err != nil {
		return err
	}
	input, err := fixture.Load(cfg.InputPath)
	if err != nil {
		return err
	}
	client := llm.NewClient(cfg.BaseURL, cfg.APIKey, cfg.Model, cfg.RequestTimeout)
	rep, responses, runErr := runner.Run(ctx, cfg, input, client)
	rep.Print(os.Stdout)
	if cfg.ReportJSON != "" {
		if err := report.WriteJSON(cfg.ReportJSON, rep); err != nil {
			return errors.Wrap(err, "write report")
		}
	}
	if cfg.ResponsesJSON != "" {
		if err := report.WriteJSON(cfg.ResponsesJSON, responses); err != nil {
			return errors.Wrap(err, "write responses")
		}
	}
	if runErr != nil {
		return runErr
	}
	if rep.Status != "PASS" {
		return fmt.Errorf("translation benchmark status is %s", rep.Status)
	}
	return nil
}

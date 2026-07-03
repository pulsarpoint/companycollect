package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/pulsarpoint/corpscout/translator/internal/config"
	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"go.temporal.io/sdk/client"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, _, err := config.LoadFromEnvironment()
	if err != nil {
		logger.Error("failed to load translator config", "err", err)
		os.Exit(1)
	}
	if cfg.ClickHouse.NativeURL == "" {
		logger.Error("clickhouse native_url is required")
		os.Exit(1)
	}

	clickHouse, err := engine.OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		logger.Error("failed to connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer clickHouse.Close()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Error("failed to connect temporal", "err", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	// TODO(task 8): bridge only. The per-source loop, translation provider,
	// runtime, and worker/router wiring that used to live here are rewritten
	// wholesale for the shared-queue architecture; this binary refuses to
	// start in the meantime.
	if cfg.EndpointID == "" {
		logger.Error("endpoint_id is required")
		os.Exit(1)
	}
	endpointConfig, ok := cfg.Endpoints[cfg.EndpointID]
	if !ok {
		logger.Error("endpoint not found", "endpoint_id", cfg.EndpointID)
		os.Exit(1)
	}
	_ = endpointConfig
	logger.Error("translator-api is mid-migration to the shared-queue architecture (plan task 8); refusing to start")
	os.Exit(1)
}

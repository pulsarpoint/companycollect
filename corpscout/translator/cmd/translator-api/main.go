package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/api"
	"github.com/pulsarpoint/corpscout/translator/internal/config"
	"github.com/pulsarpoint/corpscout/translator/internal/engine"
	"github.com/pulsarpoint/corpscout/translator/internal/orchestration"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, configPath, err := config.LoadFromEnvironment()
	if err != nil {
		logger.Error("failed to load translator config", "err", err)
		os.Exit(1)
	}
	if cfg.ClickHouse.NativeURL == "" {
		logger.Error("clickhouse native_url is required")
		os.Exit(1)
	}
	if cfg.EndpointID == "" {
		logger.Error("endpoint_id is required")
		os.Exit(1)
	}
	endpointConfig, ok := cfg.Endpoints[cfg.EndpointID]
	if !ok {
		logger.Error("endpoint not found", "endpoint_id", cfg.EndpointID)
		os.Exit(1)
	}
	if endpointConfig.BaseURL == "" {
		logger.Error("endpoint base_url is required", "endpoint_id", cfg.EndpointID)
		os.Exit(1)
	}
	if endpointConfig.Model == "" {
		logger.Error("endpoint model is required", "endpoint_id", cfg.EndpointID)
		os.Exit(1)
	}
	if endpointConfig.APIKey == "" {
		logger.Error("endpoint api_key is required", "endpoint_id", cfg.EndpointID)
		os.Exit(1)
	}

	clickHouse, err := engine.OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		logger.Error("failed to connect clickhouse", "err", err)
		os.Exit(1)
	}
	defer clickHouse.Close()

	provider, err := translation.Init(translation.Config{
		BaseURL:   endpointConfig.BaseURL,
		Model:     endpointConfig.Model,
		APIKey:    endpointConfig.APIKey,
		MaxTokens: endpointConfig.MaxTokens,
		ExtraBody: endpointConfig.ExtraBody,
		Logger:    logger,
	})
	if err != nil {
		logger.Error("failed to initialize translation provider", "err", err)
		os.Exit(1)
	}

	runtime, err := engine.NewRuntime(ctx, engine.RuntimeConfig{
		QueuePath:    cfg.Queue.Path,
		Source:       clickHouse,
		Translator:   provider,
		ProviderName: cfg.EndpointID,
		Model:        endpointConfig.Model,
		Logger:       logger,
	})
	if err != nil {
		logger.Error("failed to initialize translator runtime", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := runtime.Close(); err != nil {
			logger.Error("failed to close translator runtime", "err", err)
		}
	}()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.Address,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Error("failed to connect temporal", "err", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	temporalWorker := worker.New(temporalClient, engine.ProcessTaskQueue, worker.Options{})
	if err := orchestration.RegisterProcess(temporalWorker, runtime); err != nil {
		logger.Error("failed to register translation workflow", "err", err)
		os.Exit(1)
	}
	if err := temporalWorker.Start(); err != nil {
		logger.Error("failed to start temporal worker", "err", err)
		os.Exit(1)
	}
	defer temporalWorker.Stop()

	starter := orchestration.NewTemporalWorkflowStarter(
		temporalClient,
		cfg.Temporal.BatchSize,
		cfg.Temporal.TimeoutSeconds,
		cfg.Temporal.BatchesPerRun,
		cfg.Queue.FlushEveryBatches,
	)

	// Boot resume: never strand a half-full queue if the process crashed or
	// was redeployed while items were pending.
	if stats, err := runtime.Stats(ctx); err != nil {
		logger.Error("boot queue stats failed", "err", err)
	} else if stats.Pending > 0 {
		if result, err := starter.StartProcess(ctx); err != nil {
			logger.Error("boot resume failed", "err", err, "pending", stats.Pending)
		} else {
			logger.Info(
				"boot resume started workflow",
				"pending", stats.Pending,
				"workflow_id", result.WorkflowID,
				"run_id", result.RunID,
			)
		}
	}

	server := &http.Server{
		Addr:              cfg.Server.ListenAddress,
		Handler:           api.NewRouterWithLogger(runtime, starter, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		serverErrors <- server.ListenAndServe()
	}()

	logger.Info(
		"starting translator api",
		"addr", cfg.Server.ListenAddress,
		"config_path", configPath,
		"temporal_address", cfg.Temporal.Address,
		"temporal_namespace", cfg.Temporal.Namespace,
		"batches_per_run", cfg.Temporal.BatchesPerRun,
		"endpoint_id", cfg.EndpointID,
		"queue_path", cfg.Queue.Path,
	)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shutdown translator api", "err", err)
			os.Exit(1)
		}
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("translator api stopped", "err", err)
			os.Exit(1)
		}
	}
}

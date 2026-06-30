package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/api"
	"github.com/pulsarpoint/corpscout/translator/internal/brreg"
	"github.com/pulsarpoint/corpscout/translator/internal/config"
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
		logger.Error("failed to load translator config", "error", err)
		os.Exit(1)
	}

	sourceConfig, endpointConfig, err := norwayBRREGConfig(cfg)
	if err != nil {
		logger.Error("invalid norway brreg translator config", "error", err)
		os.Exit(1)
	}

	clickHouse, err := brreg.OpenClickHouse(ctx, cfg.ClickHouse.NativeURL)
	if err != nil {
		logger.Error("failed to connect clickhouse", "error", err)
		os.Exit(1)
	}
	defer clickHouse.Close()

	provider, err := translation.Init(translation.Config{
		BaseURL:   endpointConfig.BaseURL,
		Model:     endpointConfig.Model,
		APIKey:    endpointConfig.APIKey,
		MaxTokens: endpointConfig.MaxTokens,
		ExtraBody: endpointConfig.ExtraBody,
		PromptData: translation.PromptData{
			SourceLanguage: endpointConfig.PromptData.SourceLanguage,
			TargetLanguage: endpointConfig.PromptData.TargetLanguage,
		},
	})
	if err != nil {
		logger.Error("failed to initialize translation provider", "error", err)
		os.Exit(1)
	}

	brregRuntime, err := brreg.NewRuntime(ctx, brreg.RuntimeConfig{
		QueuePath:    sourceConfig.QueuePath,
		Source:       clickHouse,
		Translator:   provider,
		ProviderName: sourceConfig.EndpointID,
		Model:        endpointConfig.Model,
	})
	if err != nil {
		logger.Error("failed to initialize brreg runtime", "error", err)
		os.Exit(1)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := brregRuntime.Close(shutdownCtx); err != nil {
			logger.Error("failed to close brreg runtime", "error", err)
		}
	}()

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.Temporal.HostPort,
		Namespace: cfg.Temporal.Namespace,
	})
	if err != nil {
		logger.Error("failed to connect temporal", "error", err)
		os.Exit(1)
	}
	defer temporalClient.Close()

	temporalWorker := worker.New(temporalClient, cfg.Temporal.TaskQueue, worker.Options{})
	orchestration.RegisterNorwayBRREG(temporalWorker, brregRuntime)
	if err := temporalWorker.Start(); err != nil {
		logger.Error("failed to start temporal worker", "error", err)
		os.Exit(1)
	}
	defer temporalWorker.Stop()

	workflowStarter := api.NewTemporalWorkflowStarter(
		temporalClient,
		cfg.Temporal.TaskQueue,
		cfg.Temporal.BatchSize,
		cfg.Temporal.TimeoutSeconds,
	)

	server := &http.Server{
		Addr:              cfg.Server.ListenAddress,
		Handler:           api.NewRouter(workflowStarter),
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
		"temporal_host", cfg.Temporal.HostPort,
		"temporal_namespace", cfg.Temporal.Namespace,
		"temporal_task_queue", cfg.Temporal.TaskQueue,
		"brreg_queue_path", sourceConfig.QueuePath,
		"sources", len(cfg.Sources),
		"endpoints", len(cfg.Endpoints),
	)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("failed to shutdown translator api", "error", err)
			os.Exit(1)
		}
	case err := <-serverErrors:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("translator api stopped", "error", err)
			os.Exit(1)
		}
	}
}

func norwayBRREGConfig(cfg config.Config) (config.SourceConfig, config.EndpointConfig, error) {
	sourceConfig, ok := cfg.Sources["norway_brreg"]
	if !ok {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("source norway_brreg is required")
	}
	if sourceConfig.QueuePath == "" {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("source norway_brreg queue_path is required")
	}
	if sourceConfig.EndpointID == "" {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("source norway_brreg endpoint_id is required")
	}

	endpointConfig, ok := cfg.Endpoints[sourceConfig.EndpointID]
	if !ok {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("endpoint %q is required", sourceConfig.EndpointID)
	}
	if cfg.ClickHouse.NativeURL == "" {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("clickhouse native_url is required")
	}
	if endpointConfig.BaseURL == "" {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("endpoint %q base_url is required", sourceConfig.EndpointID)
	}
	if endpointConfig.Model == "" {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("endpoint %q model is required", sourceConfig.EndpointID)
	}
	if endpointConfig.APIKey == "" {
		return config.SourceConfig{}, config.EndpointConfig{}, fmt.Errorf("endpoint %q api_key is required", sourceConfig.EndpointID)
	}

	return sourceConfig, endpointConfig, nil
}

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
	if len(cfg.Sources) == 0 {
		logger.Error("no translation sources configured")
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

	sourceNames := make([]string, 0, len(cfg.Sources))
	for name, sourceConfig := range cfg.Sources {
		endpointConfig, def, err := sourceSetup(cfg, name, sourceConfig)
		if err != nil {
			logger.Error("invalid translator source config", "source", name, "err", err)
			os.Exit(1)
		}

		provider, err := translation.Init(translation.Config{
			BaseURL:   endpointConfig.BaseURL,
			Model:     endpointConfig.Model,
			APIKey:    endpointConfig.APIKey,
			MaxTokens: endpointConfig.MaxTokens,
			ExtraBody: endpointConfig.ExtraBody,
			Logger:    logger,
			PromptData: translation.PromptData{
				SourceLanguage: def.SourceLanguageName,
				TargetLanguage: def.TargetLanguageName,
			},
		})
		if err != nil {
			logger.Error("failed to initialize translation provider", "source", name, "err", err)
			os.Exit(1)
		}

		sourceRuntime, err := engine.NewRuntime(ctx, engine.RuntimeConfig{
			QueuePath:    sourceConfig.QueuePath,
			Definition:   def,
			Source:       clickHouse,
			Translator:   provider,
			ProviderName: sourceConfig.EndpointID,
			Model:        endpointConfig.Model,
			Logger:       logger,
		})
		if err != nil {
			logger.Error("failed to initialize source runtime", "source", name, "err", err)
			os.Exit(1)
		}
		defer func(name string, r *engine.Runtime) {
			if err := r.Close(); err != nil {
				logger.Error("failed to close source runtime", "source", name, "err", err)
			}
		}(name, sourceRuntime)

		temporalWorker := worker.New(temporalClient, engine.TaskQueue(name), worker.Options{})
		if err := orchestration.RegisterSource(temporalWorker, name, sourceRuntime); err != nil {
			logger.Error("failed to register source workflow", "source", name, "err", err)
			os.Exit(1)
		}
		if err := temporalWorker.Start(); err != nil {
			logger.Error("failed to start temporal worker", "source", name, "err", err)
			os.Exit(1)
		}
		defer temporalWorker.Stop()

		logger.Info(
			"translator source registered",
			"source", name,
			"task_queue", engine.TaskQueue(name),
			"queue_path", sourceConfig.QueuePath,
			"definition_path", sourceConfig.DefinitionPath,
			"endpoint", sourceConfig.EndpointID,
		)
		sourceNames = append(sourceNames, name)
	}

	workflowStarter := orchestration.NewTemporalWorkflowStarter(
		temporalClient,
		sourceNames,
		cfg.Temporal.BatchSize,
		cfg.Temporal.TimeoutSeconds,
		cfg.Temporal.BatchesPerRun,
	)

	server := &http.Server{
		Addr:              cfg.Server.ListenAddress,
		Handler:           api.NewRouterWithLogger(workflowStarter, sourceNames, logger),
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
		"sources", len(sourceNames),
		"endpoints", len(cfg.Endpoints),
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

// sourceSetup validates one source's config, loads its definition, and checks
// the definition names the same source as the config key.
func sourceSetup(cfg config.Config, name string, sourceConfig config.SourceConfig) (config.EndpointConfig, engine.Definition, error) {
	if sourceConfig.QueuePath == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("source %s queue_path is required", name)
	}
	if sourceConfig.EndpointID == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("source %s endpoint_id is required", name)
	}
	if sourceConfig.DefinitionPath == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("source %s definition_path is required", name)
	}

	endpointConfig, ok := cfg.Endpoints[sourceConfig.EndpointID]
	if !ok {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q is required", sourceConfig.EndpointID)
	}
	if endpointConfig.BaseURL == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q base_url is required", sourceConfig.EndpointID)
	}
	if endpointConfig.Model == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q model is required", sourceConfig.EndpointID)
	}
	if endpointConfig.APIKey == "" {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf("endpoint %q api_key is required", sourceConfig.EndpointID)
	}

	def, err := engine.LoadDefinition(sourceConfig.DefinitionPath)
	if err != nil {
		return config.EndpointConfig{}, engine.Definition{}, err
	}
	if def.Source != name {
		return config.EndpointConfig{}, engine.Definition{}, fmt.Errorf(
			"definition source %q does not match config source %q", def.Source, name)
	}
	return endpointConfig, def, nil
}

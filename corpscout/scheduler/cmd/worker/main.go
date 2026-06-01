package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/pulsarpoint/corpscout/scheduler/internal/app"
	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/logging"
)

func main() {
	logging.ConfigureDefaultConsoleLogger("info")

	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logging.ConfigureDefaultConsoleLogger(cfg.LogLevel)
	slog.Debug("scheduler config loaded",
		"listen_addr", cfg.ListenAddr,
		"log_level", cfg.LogLevel,
		"temporal_host", cfg.TemporalHost,
	)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	server, err := app.NewServer(ctx, cfg)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	slog.Info("corpscout-scheduler listening", "addr", cfg.ListenAddr)
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}

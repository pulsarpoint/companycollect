package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/api"
	"github.com/pulsarpoint/corpscout/translator/internal/config"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	cfg, configPath, err := config.LoadFromEnvironment()
	if err != nil {
		logger.Error("failed to load translator config", "error", err)
		os.Exit(1)
	}

	server := &http.Server{
		Addr:              cfg.Server.ListenAddress,
		Handler:           api.NewRouter(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	logger.Info(
		"starting translator api",
		"addr", cfg.Server.ListenAddress,
		"config_path", configPath,
		"sources", len(cfg.Sources),
		"endpoints", len(cfg.Endpoints),
	)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("translator api stopped", "error", err)
		os.Exit(1)
	}
}

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

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/httpapi"
	"github.com/pulsarpoint/brreg-financial-service/internal/service"
)

func main() {
	listenAddr      := getEnv("BRREG_FINANCIAL_SERVICE_LISTEN_ADDR", ":8098")
	baseURL         := getEnv("BRREG_FINANCIAL_SERVICE_BASE_URL", "https://data.brreg.no")
	timeoutStr      := getEnv("BRREG_FINANCIAL_SERVICE_REQUEST_TIMEOUT", "30s")
	maxBatchSizeStr := getEnv("BRREG_FINANCIAL_SERVICE_MAX_BATCH_SIZE", "1000")

	requestTimeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		slog.Error("invalid BRREG_FINANCIAL_SERVICE_REQUEST_TIMEOUT", "value", timeoutStr)
		os.Exit(1)
	}

	var maxBatchSize int
	if _, err := fmt.Sscanf(maxBatchSizeStr, "%d", &maxBatchSize); err != nil {
		slog.Error("invalid BRREG_FINANCIAL_SERVICE_MAX_BATCH_SIZE", "value", maxBatchSizeStr)
		os.Exit(1)
	}

	client := brregclient.New(brregclient.Config{
		BaseURL:        baseURL,
		RequestTimeout: requestTimeout,
	})

	svc := service.New(client, service.Config{
		BaseURL:      baseURL,
		MaxBatchSize: maxBatchSize,
	})

	handler := httpapi.NewHandler(svc)
	server := &http.Server{
		Addr:         listenAddr,
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("brreg-financial-service starting", "addr", listenAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

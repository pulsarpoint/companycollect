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

	"github.com/pulsarpoint/currency-service/internal/httpapi"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/pulsarpoint/currency-service/internal/service"
)

func main() {
	listenAddr        := getEnv("CURRENCY_SERVICE_LISTEN_ADDR", ":8097")
	providerName      := getEnv("CURRENCY_SERVICE_PROVIDER", "ecb")
	todayTTLStr       := getEnv("CURRENCY_SERVICE_TODAY_TTL", "6h")
	requestTimeoutStr := getEnv("CURRENCY_SERVICE_REQUEST_TIMEOUT", "30s")
	maxBatchSizeStr   := getEnv("CURRENCY_SERVICE_MAX_BATCH_SIZE", "1000")

	todayTTL, err := time.ParseDuration(todayTTLStr)
	if err != nil {
		slog.Error("invalid CURRENCY_SERVICE_TODAY_TTL", "value", todayTTLStr)
		os.Exit(1)
	}

	requestTimeout, err := time.ParseDuration(requestTimeoutStr)
	if err != nil {
		slog.Error("invalid CURRENCY_SERVICE_REQUEST_TIMEOUT", "value", requestTimeoutStr)
		os.Exit(1)
	}

	var maxBatchSize int
	if _, err := fmt.Sscanf(maxBatchSizeStr, "%d", &maxBatchSize); err != nil {
		slog.Error("invalid CURRENCY_SERVICE_MAX_BATCH_SIZE", "value", maxBatchSizeStr)
		os.Exit(1)
	}

	var providers []rates.Provider
	switch providerName {
	case "ecb":
		providers = append(providers, rates.NewECBProvider(rates.ECBConfig{RequestTimeout: requestTimeout}))
	default:
		slog.Error("unknown provider", "provider", providerName)
		os.Exit(1)
	}

	svc := service.New(service.Config{
		Providers:    providers,
		TodayTTL:     todayTTL,
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
		slog.Info("currency-service starting", "addr", listenAddr, "provider", providerName)
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

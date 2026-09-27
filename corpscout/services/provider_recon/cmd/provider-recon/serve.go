package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"provider_recon/internal/feeds"
	"provider_recon/internal/runner"
	"provider_recon/internal/server"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// cmdServe runs the HTTP API until SIGINT/SIGTERM (or ctx is cancelled), then
// stops accepting requests and waits for a running collect to finish.
func cmdServe(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("definitions", envOr("PROVIDER_RECON_DEFINITIONS", "definitions"), "definitions directory")
	listen := fs.String("listen", envOr("PROVIDER_RECON_LISTEN", ":8095"), "listen address")
	outDir := fs.String("out", "", "use this local directory instead of S3 (testing)")
	if err := fs.Parse(args); err != nil {
		return 64
	}
	if _, err := runner.LoadDefinitions(*dir, registry); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	store, err := openStore(ctx, *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	logger := slog.New(slog.NewTextHandler(stderr, nil))
	api := server.New(runner.Config{DefinitionsDir: *dir, Registry: registry, Fetcher: feeds.NewFetcher(),
		Store: store, Concurrency: 4, Logger: logger})

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	httpServer := &http.Server{Addr: *listen, Handler: api.Handler(), ReadHeaderTimeout: 10 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- httpServer.ListenAndServe() }()
	logger.Info("provider-recon serving", "listen", *listen, "definitions", *dir)

	select {
	case err := <-errs:
		if !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(stderr, err)
			return 1
		}
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdown); err != nil {
		fmt.Fprintln(stderr, err)
	}
	api.Wait()
	fmt.Fprintln(stdout, "provider-recon stopped")
	return 0
}

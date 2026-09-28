package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"provider_recon/internal/detect/server"
	"provider_recon/internal/publish"
)

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// serveMain runs serve until SIGINT/SIGTERM.
func serveMain(args []string, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return serve(ctx, args, stderr)
}

// serve loads the knowledge from the provider-recon store (S3, or a local
// directory with -store for testing), then serves the resolver until ctx
// ends, reloading the knowledge every -reload interval.
func serve(ctx context.Context, args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	listen := fs.String("listen", envOr("DNS_DETECT_LISTEN", "127.0.0.1:8096"), "listen address")
	interval := fs.Duration("reload", 10*time.Minute, "how often to check provider-recon for a new publish")
	dir := fs.String("store", "", "read provider documents from this local directory instead of S3 (testing)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if v := os.Getenv("DNS_DETECT_RELOAD_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			fmt.Fprintf(stderr, "serve: DNS_DETECT_RELOAD_INTERVAL %q: %v\n", v, err)
			return 2
		}
		*interval = d
	}
	logger := slog.New(slog.NewJSONHandler(stderr, nil))
	var store publish.Store = publish.FSStore{Root: *dir}
	if *dir == "" {
		cfg, err := publish.S3ConfigFromEnv()
		if err != nil {
			fmt.Fprintf(stderr, "serve: %v\n", err)
			return 2
		}
		s3, err := publish.NewS3Store(ctx, cfg)
		if err != nil {
			fmt.Fprintf(stderr, "serve: %v\n", err)
			return 1
		}
		store = s3
	}
	srv := server.New(store, logger)
	if err := srv.Reload(ctx); err != nil {
		fmt.Fprintf(stderr, "serve: load knowledge: %v\n", err)
		return 1
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return 1
	}
	httpServer := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go srv.Run(ctx, *interval)
	errc := make(chan error, 1)
	go func() { errc <- httpServer.Serve(ln) }()
	logger.Info("dns-detect listening", "addr", ln.Addr().String())
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return 1
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(stderr, "serve: shutdown: %v\n", err)
		return 1
	}
	return 0
}

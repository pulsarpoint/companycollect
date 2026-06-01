package app

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-chi/chi/v5"
	pgx "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
)

type Server struct {
	http            *http.Server
	pool            *pgxpool.Pool
	river           *river.Client[pgx.Tx]
	temporal        client.Client
	temporalWorkers []temporalworker.Worker
	temporalDeps    *temporalWorkerResources
}

func NewServer(ctx context.Context, cfg config.Config) (*Server, error) {
	slog.Debug("scheduler server setup started", "listen_addr", cfg.ListenAddr, "log_level", cfg.LogLevel)
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "create pgx pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "ping database")
	}
	slog.Debug("scheduler database connection ready")

	queries := db.New(pool)
	llmCipher, err := llmProviderCipher(cfg)
	if err != nil {
		pool.Close()
		return nil, err
	}
	llmStore := llmproviders.NewStore(queries, llmCipher)

	s3, err := s3client.New(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket)
	if err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "create s3 client")
	}
	if err := s3.EnsureBucket(ctx); err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "ensure s3 bucket")
	}
	slog.Debug("scheduler object storage ready", "bucket", cfg.S3Bucket)

	riverClient, err := setupRiver(ctx, pool, queries, s3)
	if err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "setup river")
	}
	if err := riverClient.Start(ctx); err != nil {
		pool.Close()
		return nil, errors.Wrap(err, "start river client")
	}
	slog.Debug("scheduler river client started")

	temporalDeps, err := newTemporalWorkerResources(cfg, pool, llmStore, s3)
	if err != nil {
		_ = riverClient.Stop(ctx)
		pool.Close()
		return nil, errors.Wrap(err, "create temporal worker resources")
	}
	slog.Debug("scheduler temporal worker resources ready")

	temporalClient, err := client.Dial(client.Options{
		HostPort:  cfg.TemporalHost,
		Namespace: "corpscout",
	})
	if err != nil {
		temporalDeps.Close()
		_ = riverClient.Stop(ctx)
		pool.Close()
		return nil, errors.Wrap(err, "connect to temporal")
	}
	slog.Debug("scheduler temporal client connected", "host", cfg.TemporalHost, "namespace", "corpscout")

	temporalWorkers := newTemporalWorkers(temporalClient, temporalDeps)
	if err := startTemporalWorkers(temporalWorkers); err != nil {
		stopTemporalWorkers(temporalWorkers)
		temporalClient.Close()
		temporalDeps.Close()
		_ = riverClient.Stop(ctx)
		pool.Close()
		return nil, err
	}
	slog.Debug("scheduler temporal workers started", "worker_count", len(temporalWorkers))

	router := chi.NewRouter()
	api := httpapi.NewHandlers(queries, riverClient, pool, s3, cfg.PostgRESTURL, temporalClient, cfg.TemporalUIURL)
	api.ConfigureLLMProviders(
		llmStore,
		llmproviders.NewProbeClient(http.DefaultClient, llmCipher),
	)
	api.RegisterRoutes(router)
	slog.Debug("scheduler http routes registered")

	return &Server{
		http:            &http.Server{Addr: cfg.ListenAddr, Handler: router},
		pool:            pool,
		river:           riverClient,
		temporal:        temporalClient,
		temporalWorkers: temporalWorkers,
		temporalDeps:    temporalDeps,
	}, nil
}

func llmProviderCipher(cfg config.Config) (*llmproviders.Cipher, error) {
	key := strings.TrimSpace(cfg.LLMProviderKey)
	if key == "" {
		slog.Warn("llm provider encryption key not configured; provider creation with keys and provider enablement are disabled")
		return nil, nil
	}
	cipher, err := llmproviders.NewCipher(key)
	if err != nil {
		return nil, errors.Wrap(err, "configure llm provider encryption")
	}
	return cipher, nil
}

func (s *Server) ListenAndServe() error {
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) {
	slog.Debug("scheduler shutdown started")
	if s.http != nil {
		_ = s.http.Shutdown(ctx)
	}
	stopTemporalWorkers(s.temporalWorkers)
	if s.river != nil {
		_ = s.river.Stop(ctx)
	}
	if s.temporal != nil {
		s.temporal.Close()
	}
	if s.temporalDeps != nil {
		s.temporalDeps.Close()
	}
	if s.pool != nil {
		s.pool.Close()
	}
	slog.Debug("scheduler shutdown completed")
}

func startTemporalWorkers(workers []temporalworker.Worker) error {
	for index, worker := range workers {
		slog.Debug("starting temporal worker", "index", index)
		if err := worker.Start(); err != nil {
			return errors.Wrap(err, "start temporal worker")
		}
		slog.Debug("temporal worker started", "index", index)
	}
	return nil
}

func stopTemporalWorkers(workers []temporalworker.Worker) {
	for i := len(workers) - 1; i >= 0; i-- {
		if workers[i] != nil {
			slog.Debug("stopping temporal worker", "index", i)
			workers[i].Stop()
		}
	}
}

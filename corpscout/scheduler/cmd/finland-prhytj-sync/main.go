package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pulsarpoint/corpscout/countrydata/finland/prhytj"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	schedcountrydata "github.com/pulsarpoint/corpscout/scheduler/internal/countrydata"
)

const defaultSyncTimeout = 8 * time.Hour

type config struct {
	DatabaseURL    string
	EnvFile        string
	BaseURL        string
	DataDir        string
	UserAgent      string
	MaxPages       int
	ChunkSize      int
	PageDelay      time.Duration
	RequestTimeout time.Duration
	Timeout        time.Duration
}

type summary struct {
	Status     string                       `json:"status"`
	SourceSlug string                       `json:"source_slug"`
	Download   countryimport.DownloadResult `json:"download"`
	Process    countryimport.ProcessResult  `json:"process"`
	StartedAt  time.Time                    `json:"started_at"`
	FinishedAt time.Time                    `json:"finished_at"`
	DurationMS int64                        `json:"duration_ms"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(os.Args[1:], os.Stdout); err != nil {
		logger.Error("Finland PRH YTJ sync failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}
	if cfg.EnvFile != "" {
		if err := countryimport.LoadEnvFile(cfg.EnvFile); err != nil {
			return errors.Wrap(err, "load env file")
		}
	}
	cfg = cfg.withEnvDefaults(os.Getenv)
	if cfg.DatabaseURL == "" {
		return errors.New("DATABASE_URL or CORPSCOUT_DATABASE_URL is required")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultSyncTimeout
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = countryimport.DefaultChunkSize
	}

	sourceConfig := prhytj.ConfigFromEnv()
	if cfg.BaseURL == "" {
		cfg.BaseURL = sourceConfig.BaseURL
	}
	if cfg.DataDir == "" {
		cfg.DataDir = sourceConfig.DataDir
	}
	if cfg.PageDelay == 0 {
		cfg.PageDelay = sourceConfig.PageDelay
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = sourceConfig.RequestTimeout
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = sourceConfig.UserAgent
	}

	startedAt := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return errors.Wrap(err, "connect postgres")
	}
	defer pool.Close()

	if err := ensureFinlandPRHYTJSchema(ctx, pool); err != nil {
		return err
	}

	store := schedcountrydata.NewFinlandPRHYTJDBStore(pool)
	importer := schedcountrydata.FinlandPRHYTJImporter{}
	result, err := importer.Run(ctx, schedcountrydata.FinlandPRHYTJImportInput{
		BaseURL:        cfg.BaseURL,
		DataDir:        cfg.DataDir,
		MaxPages:       cfg.MaxPages,
		ChunkSize:      cfg.ChunkSize,
		PageDelay:      cfg.PageDelay,
		RequestTimeout: cfg.RequestTimeout,
		UserAgent:      cfg.UserAgent,
		MetadataStore:  store,
		StoreFunc:      store.StoreCompanies,
	})
	if err != nil {
		return errors.Wrap(err, "run Finland PRH YTJ sync")
	}

	finishedAt := time.Now().UTC()
	return printSummary(output, summary{
		Status:     "succeeded",
		SourceSlug: prhytj.SourceSlug,
		Download:   result.Download,
		Process:    result.Process,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
		DurationMS: finishedAt.Sub(startedAt).Milliseconds(),
	})
}

func parseConfig(args []string) (config, error) {
	cfg := config{
		EnvFile: defaultEnvFile(),
		Timeout: defaultSyncTimeout,
	}
	fs := flag.NewFlagSet("finland-prhytj-sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.EnvFile, "env", cfg.EnvFile, "optional .env file to load before reading defaults")
	fs.StringVar(&cfg.EnvFile, "env-file", cfg.EnvFile, "optional .env file to load before reading defaults")
	fs.StringVar(&cfg.DatabaseURL, "database-url", "", "Postgres DSN for Corpscout")
	fs.StringVar(&cfg.BaseURL, "base-url", "", "PRH YTJ API v3 companies base URL")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "local source data directory")
	fs.StringVar(&cfg.UserAgent, "user-agent", "", "HTTP user agent")
	fs.IntVar(&cfg.MaxPages, "max-pages", 0, "maximum API pages to download; 0 means all pages")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", countryimport.DefaultChunkSize, "records per process/store chunk")
	fs.DurationVar(&cfg.PageDelay, "page-delay", 0, "delay between PRH API pages")
	fs.DurationVar(&cfg.RequestTimeout, "request-timeout", 0, "per-request timeout")
	fs.DurationVar(&cfg.Timeout, "timeout", cfg.Timeout, "overall sync timeout")
	if err := fs.Parse(args); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) withEnvDefaults(getenv func(string) string) config {
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = envOr(getenv, "DATABASE_URL", envOr(getenv, "CORPSCOUT_DATABASE_URL", ""))
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = envOr(getenv, "PRH_YTJ_BASE_URL", "")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = envOr(getenv, "PRH_YTJ_DATA_DIR", "")
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = envOr(getenv, "PRH_YTJ_USER_AGENT", "")
	}
	if cfg.PageDelay == 0 {
		cfg.PageDelay = envDurationMillis(getenv, "PRH_YTJ_PAGE_DELAY_MS")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = envDurationSeconds(getenv, "PRH_YTJ_REQUEST_TIMEOUT_SECONDS")
	}
	return cfg
}

func ensureFinlandPRHYTJSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var regclass *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass('countrydata_finland_prh_ytj.raw_records')::text").Scan(&regclass); err != nil {
		return errors.Wrap(err, "check Finland PRH YTJ migration schema")
	}
	if regclass == nil {
		return errors.New("countrydata_finland_prh_ytj.raw_records is missing; run database migrations first")
	}
	return nil
}

func printSummary(output io.Writer, value summary) error {
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return errors.Wrap(err, "write sync summary")
	}
	return nil
}

func defaultEnvFile() string {
	for _, candidate := range []string{
		filepath.Join("..", ".env"),
		".env",
	} {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

func envOr(getenv func(string) string, key string, fallback string) string {
	if value := strings.TrimSpace(getenv(key)); value != "" {
		return value
	}
	return fallback
}

func envDurationMillis(getenv func(string) string, key string) time.Duration {
	var milliseconds int
	if _, err := fmt.Sscanf(strings.TrimSpace(getenv(key)), "%d", &milliseconds); err != nil || milliseconds <= 0 {
		return 0
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func envDurationSeconds(getenv func(string) string, key string) time.Duration {
	var seconds int
	if _, err := fmt.Sscanf(strings.TrimSpace(getenv(key)), "%d", &seconds); err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

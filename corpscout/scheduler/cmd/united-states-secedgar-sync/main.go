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
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
	"github.com/pulsarpoint/corpscout/countrydata/united_states/secedgar"
	schedcountrydata "github.com/pulsarpoint/corpscout/scheduler/internal/countrydata"
)

type config struct {
	DatabaseURL    string
	EnvFile        string
	DownloadURL    string
	DataDir        string
	UserAgent      string
	ChunkSize      int
	Limit          int64
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
		logger.Error("SEC EDGAR sync failed", "error", err)
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
		cfg.Timeout = 15 * time.Minute
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = countryimport.DefaultChunkSize
	}

	sourceConfig := secedgar.ConfigFromEnv()
	if cfg.DownloadURL == "" {
		cfg.DownloadURL = sourceConfig.DownloadURL
	}
	if cfg.DataDir == "" {
		cfg.DataDir = sourceConfig.DataDir
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

	if err := ensureUSSECEDGARSchema(ctx, pool); err != nil {
		return err
	}

	store := schedcountrydata.NewUSSECEDGARDBStore(pool)
	importer := schedcountrydata.UnitedStatesSECEDGARImporter{}
	result, err := importer.Run(ctx, schedcountrydata.UnitedStatesSECEDGARImportInput{
		DownloadURL:    cfg.DownloadURL,
		DataDir:        cfg.DataDir,
		ChunkSize:      cfg.ChunkSize,
		Limit:          cfg.Limit,
		RequestTimeout: cfg.RequestTimeout,
		UserAgent:      cfg.UserAgent,
		MetadataStore:  store,
		StoreFunc:      store.StoreCompanies,
	})
	if err != nil {
		return errors.Wrap(err, "run SEC EDGAR sync")
	}

	finishedAt := time.Now().UTC()
	return printSummary(output, summary{
		Status:     "succeeded",
		SourceSlug: secedgar.SourceSlug,
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
		Timeout: 15 * time.Minute,
	}
	fs := flag.NewFlagSet("united-states-secedgar-sync", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.EnvFile, "env", cfg.EnvFile, "optional .env file to load before reading defaults")
	fs.StringVar(&cfg.EnvFile, "env-file", cfg.EnvFile, "optional .env file to load before reading defaults")
	fs.StringVar(&cfg.DatabaseURL, "database-url", "", "Postgres DSN for Corpscout")
	fs.StringVar(&cfg.DownloadURL, "download-url", "", "SEC EDGAR company_tickers.json URL")
	fs.StringVar(&cfg.DataDir, "data-dir", "", "local source data directory")
	fs.StringVar(&cfg.UserAgent, "user-agent", "", "HTTP user agent (SEC requires app name + contact email)")
	fs.IntVar(&cfg.ChunkSize, "chunk-size", countryimport.DefaultChunkSize, "records per process/store chunk")
	fs.Int64Var(&cfg.Limit, "limit", 0, "maximum records to process; 0 means all")
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
	if cfg.DownloadURL == "" {
		cfg.DownloadURL = envOr(getenv, "SEC_EDGAR_DOWNLOAD_URL", "")
	}
	if cfg.DataDir == "" {
		cfg.DataDir = envOr(getenv, "SEC_EDGAR_DATA_DIR", "")
	}
	if cfg.UserAgent == "" {
		cfg.UserAgent = envOr(getenv, "SEC_EDGAR_USER_AGENT", "")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = envDurationSeconds(getenv, "SEC_EDGAR_REQUEST_TIMEOUT_SECONDS")
	}
	return cfg
}

func ensureUSSECEDGARSchema(ctx context.Context, pool *pgxpool.Pool) error {
	var regclass *string
	if err := pool.QueryRow(ctx, "SELECT to_regclass('countrydata_united_states_sec_edgar.raw_records')::text").Scan(&regclass); err != nil {
		return errors.Wrap(err, "check SEC EDGAR migration schema")
	}
	if regclass == nil {
		return errors.New("countrydata_united_states_sec_edgar.raw_records is missing; run database migrations first")
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

func envDurationSeconds(getenv func(string) string, key string) time.Duration {
	var seconds int
	if _, err := fmt.Sscanf(strings.TrimSpace(getenv(key)), "%d", &seconds); err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

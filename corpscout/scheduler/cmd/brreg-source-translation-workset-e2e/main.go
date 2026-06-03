package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type config struct {
	DatabaseURL     string
	NATSURL         string
	Provider        string
	Model           string
	PromptVersion   string
	WorksetPath     string
	EnvFile         string
	CompanyLimit    int32
	FieldLimit      int32
	MaxRequestChars int32
	MaxTerms        int32
	MaxAttempts     int32
	Timeout         time.Duration
	RequestTimeout  time.Duration
}

type summary struct {
	Status         string                                    `json:"status"`
	WorksetPath    string                                    `json:"workset_path"`
	Provider       string                                    `json:"provider"`
	Model          string                                    `json:"model,omitempty"`
	PromptVersion  string                                    `json:"prompt_version"`
	Built          companydata.BuildTranslationWorksetResult `json:"built"`
	Batches        int32                                     `json:"batches"`
	TermsClaimed   int32                                     `json:"terms_claimed"`
	TermsSucceeded int32                                     `json:"terms_succeeded"`
	TermsFailed    int32                                     `json:"terms_failed"`
	Applied        companydata.ApplyTranslationWorksetResult `json:"applied"`
	SQLiteFinal    sqliteWorksetStats                        `json:"sqlite_final"`
}

type sqliteWorksetStats struct {
	TermsSucceeded       int `json:"terms_succeeded"`
	TermsPending         int `json:"terms_pending"`
	TermsRunning         int `json:"terms_running"`
	TermsFailedRetryable int `json:"terms_failed_retryable"`
	TermsFailedTerminal  int `json:"terms_failed_terminal"`
	BindingsCached       int `json:"bindings_cached"`
	BindingsTranslated   int `json:"bindings_translated"`
	BindingsApplied      int `json:"bindings_applied"`
	BindingsFailed       int `json:"bindings_failed"`
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "brreg source translation workset e2e failed: %+v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := parseConfig()
	if cfg.EnvFile != "" {
		loadEnvFile(cfg.EnvFile)
		cfg = cfg.withEnvDefaults()
	}
	if cfg.DatabaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	if cfg.NATSURL == "" {
		return errors.New("CORPSCOUT_NATS_URL is required")
	}
	if cfg.WorksetPath == "" {
		cfg.WorksetPath = filepath.Join(
			os.TempDir(),
			"corpscout",
			"brreg-translation",
			fmt.Sprintf("real-workset-%d.sqlite", time.Now().UnixNano()),
		)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return errors.Wrap(err, "connect postgres")
	}
	defer pool.Close()

	translator, err := translationclient.NewNATSWithRequestTimeout(cfg.NATSURL, cfg.RequestTimeout)
	if err != nil {
		return errors.Wrap(err, "connect translation nats")
	}
	defer translator.Close()

	store := companydata.New(pool)
	built, err := store.BuildTranslationWorkset(ctx, companydata.BuildTranslationWorksetCommand{
		Path:          cfg.WorksetPath,
		PromptVersion: cfg.PromptVersion,
		CompanyLimit:  cfg.CompanyLimit,
		FieldLimit:    cfg.FieldLimit,
	})
	if err != nil {
		return errors.Wrap(err, "build translation workset")
	}
	out := summary{
		Status:        "running",
		WorksetPath:   cfg.WorksetPath,
		Provider:      cfg.Provider,
		Model:         cfg.Model,
		PromptVersion: cfg.PromptVersion,
		Built:         built,
	}
	if built.FieldsExported == 0 {
		out.Status = "drained"
		stats, statErr := loadSQLiteWorksetStats(ctx, cfg.WorksetPath)
		if statErr != nil {
			return statErr
		}
		out.SQLiteFinal = stats
		printJSON(out)
		return nil
	}

	for {
		claimed, err := companydata.ClaimTranslationWorksetBatch(ctx, companydata.ClaimTranslationWorksetBatchCommand{
			Path:            cfg.WorksetPath,
			MaxRequestChars: cfg.MaxRequestChars,
			MaxTerms:        cfg.MaxTerms,
			MaxAttempts:     cfg.MaxAttempts,
		})
		if err != nil {
			return errors.Wrap(err, "claim translation workset batch")
		}
		if claimed.Status == "drained" || len(claimed.Terms) == 0 {
			break
		}
		response, err := translator.TranslateBrregTerms(ctx, translationRequest(cfg, claimed.Terms))
		if err != nil {
			return errors.Wrap(err, "translate workset terms")
		}
		results := translationResults(cfg, response)
		saved, err := companydata.SaveTranslationWorksetBatch(ctx, companydata.SaveTranslationWorksetBatchCommand{
			Path:          cfg.WorksetPath,
			BatchID:       claimed.BatchID,
			Provider:      cfg.Provider,
			Model:         cfg.Model,
			PromptVersion: cfg.PromptVersion,
			Results:       results,
		})
		if err != nil {
			return errors.Wrap(err, "save translation workset batch")
		}
		out.Batches++
		out.TermsClaimed += int32(len(claimed.Terms))
		out.TermsSucceeded += saved.TermsSucceeded
		out.TermsFailed += saved.TermsFailed
	}

	applied, err := store.ApplyTranslationWorkset(ctx, companydata.ApplyTranslationWorksetCommand{
		Path:          cfg.WorksetPath,
		PromptVersion: cfg.PromptVersion,
	})
	if err != nil {
		return errors.Wrap(err, "apply translation workset")
	}
	out.Applied = applied
	stats, err := loadSQLiteWorksetStats(ctx, cfg.WorksetPath)
	if err != nil {
		return err
	}
	out.SQLiteFinal = stats
	if out.TermsFailed > 0 || stats.TermsPending > 0 || stats.TermsRunning > 0 || stats.TermsFailedRetryable > 0 || stats.TermsFailedTerminal > 0 {
		out.Status = "partial"
	} else {
		out.Status = "succeeded"
	}
	printJSON(out)
	return nil
}

func parseConfig() config {
	defaultEnvFile := filepath.Join("..", ".env")
	if _, err := os.Stat(defaultEnvFile); err != nil {
		defaultEnvFile = ""
	}
	cfg := config{}
	flag.StringVar(&cfg.EnvFile, "env-file", defaultEnvFile, "optional .env file to load before reading defaults")
	flag.StringVar(&cfg.DatabaseURL, "database-url", "", "Postgres DSN for Corpscout")
	flag.StringVar(&cfg.NATSURL, "nats-url", "", "NATS URL for translation service")
	flag.StringVar(&cfg.Provider, "provider", "", "LLM provider slug or default")
	flag.StringVar(&cfg.Model, "model", "", "optional model override")
	flag.StringVar(&cfg.PromptVersion, "prompt-version", "v1", "translation prompt version")
	flag.StringVar(&cfg.WorksetPath, "workset-path", "", "SQLite workset path")
	cfg.CompanyLimit = 1
	cfg.MaxRequestChars = 12000
	cfg.MaxTerms = 50
	cfg.MaxAttempts = 2
	flag.Var((*int32Flag)(&cfg.CompanyLimit), "company-limit", "number of companies to export; 0 means all eligible companies")
	flag.Var((*int32Flag)(&cfg.FieldLimit), "field-limit", "number of missing fields to export; 0 means no field cap")
	flag.Var((*int32Flag)(&cfg.MaxRequestChars), "max-request-chars", "max source characters per translation request")
	flag.Var((*int32Flag)(&cfg.MaxTerms), "max-terms", "max terms per translation request")
	flag.Var((*int32Flag)(&cfg.MaxAttempts), "max-attempts", "max retry attempts for retryable term failures")
	flag.DurationVar(&cfg.Timeout, "timeout", 10*time.Minute, "overall timeout")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", 3*time.Minute, "NATS request timeout per batch")
	flag.Parse()
	return cfg.withEnvDefaults()
}

func (cfg config) withEnvDefaults() config {
	if cfg.DatabaseURL == "" {
		cfg.DatabaseURL = envOr("DATABASE_URL", envOr("CORPSCOUT_DATABASE_URL", ""))
	}
	if cfg.NATSURL == "" {
		cfg.NATSURL = envOr("CORPSCOUT_NATS_URL", "")
	}
	if cfg.Provider == "" {
		cfg.Provider = envOr("BRREG_TRANSLATION_PROVIDER", "default")
	}
	if cfg.Model == "" {
		cfg.Model = envOr("BRREG_TRANSLATION_MODEL", "")
	}
	if cfg.PromptVersion == "" {
		cfg.PromptVersion = envOr("BRREG_TRANSLATION_PROMPT_VERSION", "v1")
	}
	return cfg
}

type int32Flag int32

func (v *int32Flag) String() string {
	return fmt.Sprintf("%d", *v)
}

func (v *int32Flag) Set(value string) error {
	var parsed int64
	_, err := fmt.Sscanf(value, "%d", &parsed)
	if err != nil {
		return err
	}
	*v = int32Flag(parsed)
	return nil
}

func loadEnvFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		key, value, _ := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key != "" && os.Getenv(key) == "" {
			_ = os.Setenv(key, value)
		}
	}
}

func translationRequest(cfg config, terms []companydata.TranslationWorksetTerm) translationclient.TermTranslationRequest {
	request := translationclient.TermTranslationRequest{
		RequestID:     fmt.Sprintf("brreg-workset-%d", time.Now().UnixNano()),
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		Provider:      cfg.Provider,
		Model:         cfg.Model,
		PromptVersion: cfg.PromptVersion,
		Terms:         make([]translationclient.TermTranslationRequestTerm, 0, len(terms)),
	}
	for _, term := range terms {
		request.Terms = append(request.Terms, translationclient.TermTranslationRequestTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
		})
	}
	return request
}

func translationResults(
	cfg config,
	response translationclient.TermTranslationResult,
) []companydata.TranslationTermResult {
	provider := defaultString(response.Provider, cfg.Provider)
	model := defaultString(response.Model, cfg.Model)
	promptVersion := defaultString(response.PromptVersion, cfg.PromptVersion)
	results := make([]companydata.TranslationTermResult, 0, len(response.Results)+len(response.Failures))
	for _, item := range response.Results {
		results = append(results, companydata.TranslationTermResult{
			TermKey:              item.TermKey,
			SourceText:           item.SourceText,
			SourceTextNormalized: item.SourceTextNormalized,
			TranslatedText:       item.TranslatedText,
			Status:               defaultString(item.Status, "succeeded"),
			Provider:             provider,
			Model:                model,
			PromptVersion:        promptVersion,
		})
	}
	for _, item := range response.Failures {
		results = append(results, companydata.TranslationTermResult{
			TermKey:              item.TermKey,
			SourceText:           item.SourceText,
			SourceTextNormalized: item.SourceTextNormalized,
			Status:               defaultString(item.Status, "failed_retryable"),
			Provider:             provider,
			Model:                model,
			PromptVersion:        promptVersion,
			Error:                item.Error,
			ErrorCode:            item.ErrorCode,
		})
	}
	return results
}

func loadSQLiteWorksetStats(ctx context.Context, path string) (sqliteWorksetStats, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return sqliteWorksetStats{}, errors.Wrap(err, "open sqlite workset")
	}
	defer db.Close()
	return sqliteWorksetStats{
		TermsSucceeded:       countSQLiteRows(ctx, db, "translation_terms", "status = 'succeeded'"),
		TermsPending:         countSQLiteRows(ctx, db, "translation_terms", "status = 'pending'"),
		TermsRunning:         countSQLiteRows(ctx, db, "translation_terms", "status = 'running'"),
		TermsFailedRetryable: countSQLiteRows(ctx, db, "translation_terms", "status = 'failed_retryable'"),
		TermsFailedTerminal:  countSQLiteRows(ctx, db, "translation_terms", "status = 'failed_terminal'"),
		BindingsCached:       countSQLiteRows(ctx, db, "translation_bindings", "status = 'cached'"),
		BindingsTranslated:   countSQLiteRows(ctx, db, "translation_bindings", "status = 'translated'"),
		BindingsApplied:      countSQLiteRows(ctx, db, "translation_bindings", "status = 'applied'"),
		BindingsFailed:       countSQLiteRows(ctx, db, "translation_bindings", "status = 'failed'"),
	}, nil
}

func countSQLiteRows(ctx context.Context, db *sql.DB, table string, where string) int {
	var count int
	_ = db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE "+where).Scan(&count)
	return count
}

func printJSON(value any) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	_ = encoder.Encode(value)
}

func envOr(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func defaultString(value string, fallback string) string {
	value = strings.TrimSpace(value)
	if value != "" {
		return value
	}
	return fallback
}

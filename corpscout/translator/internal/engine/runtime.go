package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pulsarpoint/corpscout/translator/internal/queue"
	"github.com/pulsarpoint/corpscout/translator/internal/translation"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type Runtime struct {
	queuePath    string
	db           *sql.DB
	queue        *queue.Queue
	source       ClickHouseSource
	translator   translation.Translator
	providerName string
	model        string
	logger       *slog.Logger
	closed       bool
}

type RuntimeConfig struct {
	QueuePath    string
	Source       ClickHouseSource
	Translator   translation.Translator
	ProviderName string
	Model        string
	Logger       *slog.Logger
}

type ProcessInput struct {
	BatchSize      int
	TimeoutSeconds int
}

type ProcessResult struct {
	TranslatedCount int
	PendingCount    int
	OutputCount     int
}

type UploadResult struct {
	RowsSeen     int
	RowsInserted int
}

func NewRuntime(ctx context.Context, config RuntimeConfig) (*Runtime, error) {
	if config.QueuePath == "" {
		return nil, errors.New("queue path is required")
	}
	if config.Source == nil {
		return nil, errors.New("clickhouse source is required")
	}
	if config.Translator == nil {
		return nil, errors.New("translator is required")
	}
	if config.ProviderName == "" {
		return nil, errors.New("provider name is required")
	}
	if config.Model == "" {
		return nil, errors.New("model is required")
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	logger = logger.With(
		"component", "translator_runtime",
		"queue_path", config.QueuePath,
	)

	created := false
	if _, err := os.Stat(config.QueuePath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("stat queue %q: %w", config.QueuePath, err)
		}
		created = true
	}
	if err := os.MkdirAll(filepath.Dir(config.QueuePath), 0o755); err != nil {
		return nil, fmt.Errorf("create queue directory: %w", err)
	}

	db, err := sql.Open("duckdb", config.QueuePath)
	if err != nil {
		return nil, fmt.Errorf("open queue duckdb: %w", err)
	}
	if err := createQueueTables(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	q, err := queue.New(db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	runtime := &Runtime{
		queuePath:    config.QueuePath,
		db:           db,
		queue:        q,
		source:       config.Source,
		translator:   config.Translator,
		providerName: config.ProviderName,
		model:        config.Model,
		logger:       logger,
	}
	logger.Info(
		"runtime initialized",
		"queue_created", created,
		"provider", config.ProviderName,
		"model", config.Model,
	)

	return runtime, nil
}

func (r *Runtime) ProcessOneBatch(ctx context.Context, input ProcessInput) (ProcessResult, error) {
	if r.closed {
		return ProcessResult{}, errors.New("translator runtime is closed")
	}

	start := time.Now()
	r.logger.Info(
		"process batch started",
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"provider", r.providerName,
		"model", r.model,
	)
	items, err := r.queue.GetBatch(ctx, input.BatchSize)
	if err != nil {
		r.logger.Error(
			"process batch failed",
			"err", err,
			"batch_size", input.BatchSize,
			"timeout_seconds", input.TimeoutSeconds,
			"duration_ms", elapsedMillis(start),
		)
		return ProcessResult{}, err
	}

	translatedCount := 0
	if len(items) > 0 {
		promptData := translation.PromptData{
			SourceLanguage: items[0].SourceLanguageName,
			TargetLanguage: items[0].TargetLanguageName,
		}
		output, failed, err := translation.TranslateItems(
			ctx,
			r.translator,
			items,
			input.TimeoutSeconds,
			r.providerName,
			r.model,
			promptData,
		)
		if err != nil {
			r.logger.Error(
				"process batch failed",
				"err", err,
				"batch_size", input.BatchSize,
				"timeout_seconds", input.TimeoutSeconds,
				"duration_ms", elapsedMillis(start),
			)
			return ProcessResult{}, err
		}
		if err := r.queue.SaveBatch(ctx, output); err != nil {
			r.logger.Error(
				"process batch failed",
				"err", err,
				"batch_size", input.BatchSize,
				"timeout_seconds", input.TimeoutSeconds,
				"duration_ms", elapsedMillis(start),
			)
			return ProcessResult{}, err
		}
		if err := r.queue.SaveFailed(ctx, failed); err != nil {
			r.logger.Error(
				"process batch failed",
				"err", err,
				"batch_size", input.BatchSize,
				"timeout_seconds", input.TimeoutSeconds,
				"duration_ms", elapsedMillis(start),
			)
			return ProcessResult{}, err
		}
		translatedCount = len(output)
	}
	counts, err := r.queueCounts(ctx)
	if err != nil {
		r.logger.Error("queue counts failed", "err", err, "duration_ms", elapsedMillis(start))
		return ProcessResult{}, err
	}
	r.logger.Info(
		"process batch completed",
		"translated_count", translatedCount,
		"input_count", counts.input,
		"output_count", counts.output,
		"pending_count", counts.pending,
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"duration_ms", elapsedMillis(start),
	)
	return ProcessResult{
		TranslatedCount: translatedCount,
		PendingCount:    counts.pending,
		OutputCount:     counts.output,
	}, nil
}

// FlushOutput inserts all queued output rows into ClickHouse and then, only
// after a successful insert, deletes the flushed rows from the DuckDB queue.
// Matched input_items are deleted first (while output_items still exists to
// join against), then all of output_items; failed_items is never touched.
func (r *Runtime) FlushOutput(ctx context.Context) (UploadResult, error) {
	if r.closed {
		return UploadResult{}, errors.New("translator runtime is closed")
	}

	start := time.Now()
	r.logger.Info("flush output started")
	translations, err := r.outputTranslations(ctx)
	if err != nil {
		r.logger.Error("flush output failed", "err", err, "duration_ms", elapsedMillis(start))
		return UploadResult{}, err
	}
	if len(translations) == 0 {
		r.logger.Info("flush output completed", "rows_seen", 0, "rows_inserted", 0, "duration_ms", elapsedMillis(start))
		return UploadResult{}, nil
	}

	inserted, err := r.source.InsertTextTranslations(ctx, translations)
	if err != nil {
		r.logger.Error(
			"flush output failed",
			"err", err,
			"rows_seen", len(translations),
			"duration_ms", elapsedMillis(start),
		)
		return UploadResult{RowsSeen: len(translations)}, err
	}

	if _, err := r.db.ExecContext(ctx, `
		delete from input_items
		where (source_table, source_column, source_text_hash, source_lang, target_lang) in (
			select source_table, source_column, source_text_hash, source_lang, target_lang
			from output_items
		)
	`); err != nil {
		r.logger.Error(
			"flush output failed",
			"err", err,
			"rows_seen", len(translations),
			"rows_inserted", inserted,
			"duration_ms", elapsedMillis(start),
		)
		return UploadResult{RowsSeen: len(translations), RowsInserted: inserted},
			fmt.Errorf("delete flushed input rows: %w", err)
	}
	if _, err := r.db.ExecContext(ctx, `delete from output_items`); err != nil {
		r.logger.Error(
			"flush output failed",
			"err", err,
			"rows_seen", len(translations),
			"rows_inserted", inserted,
			"duration_ms", elapsedMillis(start),
		)
		return UploadResult{RowsSeen: len(translations), RowsInserted: inserted},
			fmt.Errorf("delete flushed output rows: %w", err)
	}

	r.logger.Info(
		"flush output completed",
		"rows_seen", len(translations),
		"rows_inserted", inserted,
		"duration_ms", elapsedMillis(start),
	)
	return UploadResult{RowsSeen: len(translations), RowsInserted: inserted}, nil
}

func (r *Runtime) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	return r.db.Close()
}

func (r *Runtime) outputTranslations(ctx context.Context) ([]TextTranslation, error) {
	rows, err := r.db.QueryContext(ctx, `
		select
			source_table,
			source_column,
			source_text,
			source_text_hash::varchar,
			source_lang,
			target_lang,
			translated_text,
			provider,
			model
		from output_items
		order by source_table, source_column, source_text_hash, source_lang, target_lang
	`)
	if err != nil {
		return nil, fmt.Errorf("query output translations: %w", err)
	}
	defer rows.Close()

	version := time.Now().Unix()
	translations := make([]TextTranslation, 0)
	for rows.Next() {
		var row TextTranslation
		var sourceTextHash string
		if err := rows.Scan(
			&row.SourceTable,
			&row.SourceColumn,
			&row.SourceText,
			&sourceTextHash,
			&row.SourceLang,
			&row.TargetLang,
			&row.TranslatedText,
			&row.Provider,
			&row.Model,
		); err != nil {
			return nil, fmt.Errorf("scan output translation: %w", err)
		}
		hash, err := strconv.ParseUint(sourceTextHash, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse source_text_hash %q: %w", sourceTextHash, err)
		}
		row.SourceTextHash = hash
		row.Version = version
		translations = append(translations, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read output translations: %w", err)
	}
	return translations, nil
}

type queueCounts struct {
	input   int
	output  int
	pending int
}

func (r *Runtime) queueCounts(ctx context.Context) (queueCounts, error) {
	var counts queueCounts
	if err := r.db.QueryRowContext(ctx, "select count(*) from input_items").Scan(&counts.input); err != nil {
		return queueCounts{}, fmt.Errorf("count input_items: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, "select count(*) from output_items").Scan(&counts.output); err != nil {
		return queueCounts{}, fmt.Errorf("count output_items: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, `
		select count(*)
		from input_items as i
		where not exists (
			select 1
			from output_items as o
			where o.source_table = i.source_table
				and o.source_column = i.source_column
				and o.source_text_hash = i.source_text_hash
				and o.source_lang = i.source_lang
				and o.target_lang = i.target_lang
		)
			and not exists (
			select 1
			from failed_items as f
			where f.source_table = i.source_table
				and f.source_column = i.source_column
				and f.source_text_hash = i.source_text_hash
				and f.source_lang = i.source_lang
				and f.target_lang = i.target_lang
		)
	`).Scan(&counts.pending); err != nil {
		return queueCounts{}, fmt.Errorf("count pending queue items: %w", err)
	}
	return counts, nil
}

func elapsedMillis(start time.Time) int64 {
	return time.Since(start).Milliseconds()
}

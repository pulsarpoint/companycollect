package brreg

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
	requests chan runtimeRequest
	done     chan struct{}
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
}

type UploadResult struct {
	RowsSeen     int
	RowsInserted int
}

type runtimeRequestKind int

const (
	runtimeRequestLoad runtimeRequestKind = iota
	runtimeRequestProcess
	runtimeRequestUpload
	runtimeRequestClose
)

type runtimeRequest struct {
	ctx     context.Context
	kind    runtimeRequestKind
	process ProcessInput
	resp    chan runtimeResponse
}

type runtimeResponse struct {
	load    InitResult
	process ProcessResult
	upload  UploadResult
	err     error
}

type runtimeState struct {
	queuePath    string
	queueCreated bool
	db           *sql.DB
	queue        *queue.Queue
	source       ClickHouseSource
	providerName string
	model        string
	logger       *slog.Logger
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
		"component", "brreg_runtime",
		"source", "norway_brreg",
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

	q, err := queue.New(db, config.Translator)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	runtime := &Runtime{
		requests: make(chan runtimeRequest),
		done:     make(chan struct{}),
	}
	state := runtimeState{
		queuePath:    config.QueuePath,
		queueCreated: created,
		db:           db,
		queue:        q,
		source:       config.Source,
		providerName: config.ProviderName,
		model:        config.Model,
		logger:       logger,
	}
	logger.Info(
		"brreg runtime initialized",
		"queue_created", created,
		"provider", config.ProviderName,
		"model", config.Model,
	)
	go runtime.run(state)

	return runtime, nil
}

func (r *Runtime) LoadNewInput(ctx context.Context) (InitResult, error) {
	resp, err := r.call(ctx, runtimeRequest{ctx: ctx, kind: runtimeRequestLoad})
	if err != nil {
		return InitResult{}, err
	}
	return resp.load, resp.err
}

func (r *Runtime) ProcessOneBatch(ctx context.Context, input ProcessInput) (ProcessResult, error) {
	resp, err := r.call(ctx, runtimeRequest{ctx: ctx, kind: runtimeRequestProcess, process: input})
	if err != nil {
		return ProcessResult{}, err
	}
	return resp.process, resp.err
}

func (r *Runtime) UploadOutput(ctx context.Context) (UploadResult, error) {
	resp, err := r.call(ctx, runtimeRequest{ctx: ctx, kind: runtimeRequestUpload})
	if err != nil {
		return UploadResult{}, err
	}
	return resp.upload, resp.err
}

func (r *Runtime) Close(ctx context.Context) error {
	resp, err := r.call(ctx, runtimeRequest{ctx: ctx, kind: runtimeRequestClose})
	if err != nil {
		return err
	}
	<-r.done
	return resp.err
}

func (r *Runtime) call(ctx context.Context, request runtimeRequest) (runtimeResponse, error) {
	request.resp = make(chan runtimeResponse, 1)

	select {
	case r.requests <- request:
	case <-ctx.Done():
		return runtimeResponse{}, ctx.Err()
	case <-r.done:
		return runtimeResponse{}, errors.New("brreg runtime is closed")
	}

	select {
	case resp := <-request.resp:
		return resp, nil
	case <-ctx.Done():
		return runtimeResponse{}, ctx.Err()
	case <-r.done:
		return runtimeResponse{}, errors.New("brreg runtime is closed")
	}
}

func (r *Runtime) run(state runtimeState) {
	defer close(r.done)

	for request := range r.requests {
		switch request.kind {
		case runtimeRequestLoad:
			result, err := state.loadNewInput(request.ctx)
			request.resp <- runtimeResponse{load: result, err: err}
		case runtimeRequestProcess:
			result, err := state.processOneBatch(request.ctx, request.process)
			request.resp <- runtimeResponse{process: result, err: err}
		case runtimeRequestUpload:
			result, err := state.uploadOutput(request.ctx)
			request.resp <- runtimeResponse{upload: result, err: err}
		case runtimeRequestClose:
			err := state.db.Close()
			request.resp <- runtimeResponse{err: err}
			return
		default:
			request.resp <- runtimeResponse{err: fmt.Errorf("unknown runtime request kind: %d", request.kind)}
		}
	}
}

func (s *runtimeState) loadNewInput(ctx context.Context) (InitResult, error) {
	start := time.Now()
	s.logger.Info("brreg load input started")
	result, err := initializeTranslationWithDB(ctx, s.source, s.db, s.queuePath, s.queueCreated)
	s.queueCreated = false
	if err != nil {
		s.logger.Error("brreg load input failed", "err", err, "duration_ms", elapsedMillis(start))
		return result, err
	}
	s.logger.Info(
		"brreg load input completed",
		"rows_seen", result.RowsSeen,
		"rows_inserted", result.RowsInserted,
		"static_rows_seen", result.StaticRowsSeen,
		"static_flushed", result.StaticFlushed,
		"created", result.Created,
		"duration_ms", elapsedMillis(start),
	)
	return result, err
}

func (s *runtimeState) processOneBatch(ctx context.Context, input ProcessInput) (ProcessResult, error) {
	start := time.Now()
	s.logger.Info(
		"brreg process batch started",
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"provider", s.providerName,
		"model", s.model,
	)
	translatedCount, err := s.queue.ProcessBatch(
		ctx,
		input.BatchSize,
		input.TimeoutSeconds,
		s.providerName,
		s.model,
	)
	if err != nil {
		s.logger.Error(
			"brreg process batch failed",
			"err", err,
			"batch_size", input.BatchSize,
			"timeout_seconds", input.TimeoutSeconds,
			"duration_ms", elapsedMillis(start),
		)
		return ProcessResult{}, err
	}
	counts, err := s.queueCounts(ctx)
	if err != nil {
		s.logger.Error("brreg queue counts failed", "err", err, "duration_ms", elapsedMillis(start))
		return ProcessResult{}, err
	}
	s.logger.Info(
		"brreg process batch completed",
		"translated_count", translatedCount,
		"input_count", counts.input,
		"output_count", counts.output,
		"pending_count", counts.pending,
		"batch_size", input.BatchSize,
		"timeout_seconds", input.TimeoutSeconds,
		"duration_ms", elapsedMillis(start),
	)
	return ProcessResult{TranslatedCount: translatedCount}, nil
}

func (s *runtimeState) uploadOutput(ctx context.Context) (UploadResult, error) {
	start := time.Now()
	s.logger.Info("brreg upload output started")
	translations, err := s.outputTranslations(ctx)
	if err != nil {
		s.logger.Error("brreg upload output failed", "err", err, "duration_ms", elapsedMillis(start))
		return UploadResult{}, err
	}
	if len(translations) == 0 {
		s.logger.Info("brreg upload output completed", "rows_seen", 0, "rows_inserted", 0, "duration_ms", elapsedMillis(start))
		return UploadResult{}, nil
	}

	inserted, err := s.source.InsertTextTranslations(ctx, translations)
	if err != nil {
		s.logger.Error(
			"brreg upload output failed",
			"err", err,
			"rows_seen", len(translations),
			"duration_ms", elapsedMillis(start),
		)
		return UploadResult{RowsSeen: len(translations)}, err
	}
	s.logger.Info(
		"brreg upload output completed",
		"rows_seen", len(translations),
		"rows_inserted", inserted,
		"duration_ms", elapsedMillis(start),
	)
	return UploadResult{RowsSeen: len(translations), RowsInserted: inserted}, nil
}

func (s *runtimeState) outputTranslations(ctx context.Context) ([]TextTranslation, error) {
	rows, err := s.db.QueryContext(ctx, `
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

func (s *runtimeState) queueCounts(ctx context.Context) (queueCounts, error) {
	var counts queueCounts
	if err := s.db.QueryRowContext(ctx, "select count(*) from input_items").Scan(&counts.input); err != nil {
		return queueCounts{}, fmt.Errorf("count input_items: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, "select count(*) from output_items").Scan(&counts.output); err != nil {
		return queueCounts{}, fmt.Errorf("count output_items: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, `
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
	`).Scan(&counts.pending); err != nil {
		return queueCounts{}, fmt.Errorf("count pending queue items: %w", err)
	}
	return counts, nil
}

func elapsedMillis(start time.Time) int64 {
	return time.Since(start).Milliseconds()
}

package sedb

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type BeginWorkflowRunParams struct {
	Orchestrator      *string
	OrchestratorRunID string
	RunType           string
	Metadata          []byte
}

type FinishWorkflowRunParams struct {
	Status           string
	RecordsSeen      int32
	RecordsCompleted int32
	RecordsFailed    int32
	Error            *string
	ID               uuid.UUID
}

type CreateBulkSnapshotParams struct {
	WorkflowRunID uuid.UUID
	SnapshotKey   string
	SnapshotDate  time.Time
	Metadata      []byte
}

type MarkBulkSnapshotParsedParams struct {
	RecordsSeen    int32
	RecordsWritten int32
	Metadata       []byte
	ID             uuid.UUID
}

type RecordSourceFileParams struct {
	BulkSnapshotID     uuid.UUID
	DatasetKey         string
	SourceURL          string
	FileName           *string
	FileFormat         string
	ContentType        *string
	ContentLengthBytes *int64
	PayloadHash        *string
	RowsSeen           int32
	RowsWritten        int32
	Status             string
	Error              *string
	Metadata           []byte
}

func (g *Gateway) BeginWorkflowRun(ctx context.Context, params BeginWorkflowRunParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("se workflow database pool not available")
	}
	var id uuid.UUID
	if err := g.pool.QueryRow(ctx, `
		INSERT INTO se_workflow.workflow_runs (
			orchestrator, orchestrator_run_id, run_type, metadata
		)
		VALUES (COALESCE($1::text, 'temporal'), $2, $3, COALESCE($4::jsonb, '{}'::jsonb))
		ON CONFLICT (orchestrator_run_id) DO UPDATE
		SET
			orchestrator = EXCLUDED.orchestrator,
			run_type = EXCLUDED.run_type,
			status = 'running',
			started_at = now(),
			finished_at = NULL,
			error = NULL,
			metadata = EXCLUDED.metadata
		RETURNING id
	`, params.Orchestrator, params.OrchestratorRunID, params.RunType, jsonObject(params.Metadata)).Scan(&id); err != nil {
		return uuid.Nil, errors.Wrap(err, "begin se workflow run")
	}
	return id, nil
}

func (g *Gateway) FinishWorkflowRun(ctx context.Context, params FinishWorkflowRunParams) error {
	if g == nil || g.pool == nil {
		return errors.New("se workflow database pool not available")
	}
	if _, err := g.pool.Exec(ctx, `
		UPDATE se_workflow.workflow_runs
		SET
			status = $1,
			finished_at = now(),
			records_seen = $2,
			records_completed = $3,
			records_failed = $4,
			error = $5
		WHERE id = $6
	`, params.Status, params.RecordsSeen, params.RecordsCompleted, params.RecordsFailed, params.Error, params.ID); err != nil {
		return errors.Wrap(err, "finish se workflow run")
	}
	return nil
}

func (g *Gateway) CreateBulkSnapshot(ctx context.Context, params CreateBulkSnapshotParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("se workflow database pool not available")
	}
	var id uuid.UUID
	if err := g.pool.QueryRow(ctx, `
		INSERT INTO se_workflow.bulk_snapshots (
			workflow_run_id, snapshot_key, snapshot_date, status, downloaded_at, metadata
		)
		VALUES ($1, $2, $3, 'downloaded', now(), COALESCE($4::jsonb, '{}'::jsonb))
		RETURNING id
	`, params.WorkflowRunID, nullableText(params.SnapshotKey), nullableDate(params.SnapshotDate), jsonObject(params.Metadata)).Scan(&id); err != nil {
		return uuid.Nil, errors.Wrap(err, "create se bulk snapshot")
	}
	return id, nil
}

func (g *Gateway) MarkBulkSnapshotParsed(ctx context.Context, params MarkBulkSnapshotParsedParams) error {
	if g == nil || g.pool == nil {
		return errors.New("se workflow database pool not available")
	}
	if _, err := g.pool.Exec(ctx, `
		UPDATE se_workflow.bulk_snapshots
		SET
			status = 'parsed',
			parsed_at = now(),
			records_seen = $1,
			records_written = $2,
			metadata = COALESCE($3::jsonb, metadata)
		WHERE id = $4
	`, params.RecordsSeen, params.RecordsWritten, jsonObject(params.Metadata), params.ID); err != nil {
		return errors.Wrap(err, "mark se bulk snapshot parsed")
	}
	return nil
}

func (g *Gateway) RecordSourceFile(ctx context.Context, params RecordSourceFileParams) (uuid.UUID, error) {
	if g == nil || g.pool == nil {
		return uuid.Nil, errors.New("se workflow database pool not available")
	}
	var id uuid.UUID
	if err := g.pool.QueryRow(ctx, `
		INSERT INTO se_workflow.source_files (
			bulk_snapshot_id,
			dataset_key,
			source_url,
			file_name,
			file_format,
			content_type,
			content_length_bytes,
			payload_hash,
			rows_seen,
			rows_written,
			status,
			error,
			metadata
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, COALESCE($13::jsonb, '{}'::jsonb))
		ON CONFLICT (bulk_snapshot_id, dataset_key, source_url) DO UPDATE
		SET
			file_name = EXCLUDED.file_name,
			file_format = EXCLUDED.file_format,
			content_type = EXCLUDED.content_type,
			content_length_bytes = EXCLUDED.content_length_bytes,
			payload_hash = EXCLUDED.payload_hash,
			rows_seen = EXCLUDED.rows_seen,
			rows_written = EXCLUDED.rows_written,
			status = EXCLUDED.status,
			error = EXCLUDED.error,
			metadata = EXCLUDED.metadata,
			updated_at = now()
		RETURNING id
	`, params.BulkSnapshotID, params.DatasetKey, params.SourceURL, params.FileName, params.FileFormat,
		params.ContentType, params.ContentLengthBytes, params.PayloadHash, params.RowsSeen,
		params.RowsWritten, params.Status, params.Error, jsonObject(params.Metadata)).Scan(&id); err != nil {
		return uuid.Nil, errors.Wrap(err, "record se source file")
	}
	return id, nil
}

func (g *Gateway) GetProcessedSourceFileByHash(ctx context.Context, datasetKey string, payloadHash string) (ProcessedSourceFile, bool, error) {
	if g == nil || g.pool == nil {
		return ProcessedSourceFile{}, false, errors.New("se workflow database pool not available")
	}
	if strings.TrimSpace(datasetKey) == "" || strings.TrimSpace(payloadHash) == "" {
		return ProcessedSourceFile{}, false, nil
	}

	var file ProcessedSourceFile
	err := g.pool.QueryRow(ctx, `
		SELECT id, COALESCE(source_url, ''), COALESCE(payload_hash, ''), rows_seen, rows_written, metadata
		FROM se_workflow.source_files
		WHERE dataset_key = $1
		  AND payload_hash = $2
		  AND status = 'parsed'
		ORDER BY updated_at DESC, created_at DESC
		LIMIT 1
	`, datasetKey, payloadHash).Scan(&file.ID, &file.SourceURL, &file.PayloadHash, &file.RowsSeen, &file.RowsWritten, &file.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessedSourceFile{}, false, nil
	}
	if err != nil {
		return ProcessedSourceFile{}, false, errors.Wrap(err, "get processed se source file by hash")
	}
	return file, true, nil
}

func (g *Gateway) GetLatestParsedSourceFile(ctx context.Context, datasetKey string, sourceURL string) (ProcessedSourceFile, bool, error) {
	if g == nil || g.pool == nil {
		return ProcessedSourceFile{}, false, errors.New("se workflow database pool not available")
	}
	if strings.TrimSpace(datasetKey) == "" || strings.TrimSpace(sourceURL) == "" {
		return ProcessedSourceFile{}, false, nil
	}

	var file ProcessedSourceFile
	err := g.pool.QueryRow(ctx, `
		SELECT id, COALESCE(source_url, ''), COALESCE(payload_hash, ''), rows_seen, rows_written, metadata
		FROM se_workflow.source_files
		WHERE dataset_key = $1
		  AND source_url = $2
		  AND status = 'parsed'
		ORDER BY updated_at DESC, created_at DESC
		LIMIT 1
	`, datasetKey, sourceURL).Scan(&file.ID, &file.SourceURL, &file.PayloadHash, &file.RowsSeen, &file.RowsWritten, &file.Metadata)
	if errors.Is(err, pgx.ErrNoRows) {
		return ProcessedSourceFile{}, false, nil
	}
	if err != nil {
		return ProcessedSourceFile{}, false, errors.Wrap(err, "get latest parsed se source file")
	}
	return file, true, nil
}

func (g *Gateway) GetDataSourceConfig(ctx context.Context, sourceName string) ([]byte, bool, error) {
	if g == nil || g.pool == nil {
		return nil, false, errors.New("se workflow database pool not available")
	}
	sourceName = strings.TrimSpace(sourceName)
	if sourceName == "" {
		return nil, false, nil
	}

	var config []byte
	err := g.pool.QueryRow(ctx, `
		SELECT config
		FROM data_sources
		WHERE name = $1
	`, sourceName).Scan(&config)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.Wrap(err, "get se data source config")
	}
	return config, true, nil
}

func nullableText(value string) *string {
	trimmed := value
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func nullableDate(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

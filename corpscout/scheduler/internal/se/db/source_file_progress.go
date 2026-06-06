package sedb

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type SourceFileProgress struct {
	ID          uuid.UUID
	Status      string
	RowsSeen    int32
	RowsWritten int32
}

type UpdateSourceFileProgressParams struct {
	ID          uuid.UUID
	RowsSeen    int32
	RowsWritten int32
}

type ReattachSourceFileToSnapshotParams struct {
	ID                 uuid.UUID
	BulkSnapshotID     uuid.UUID
	SourceURL          string
	FileName           *string
	FileFormat         string
	ContentType        *string
	ContentLengthBytes *int64
	PayloadHash        *string
	RowsSeen           int32
	RowsWritten        int32
	Metadata           []byte
}

func (g *Gateway) GetSourceFileProgress(ctx context.Context, id uuid.UUID) (SourceFileProgress, bool, error) {
	if g == nil || g.pool == nil {
		return SourceFileProgress{}, false, errors.New("se workflow database pool not available")
	}
	if id == uuid.Nil {
		return SourceFileProgress{}, false, nil
	}

	var progress SourceFileProgress
	err := g.pool.QueryRow(ctx, `
		SELECT id, status, rows_seen, rows_written
		FROM se_workflow.source_files
		WHERE id = $1
	`, id).Scan(&progress.ID, &progress.Status, &progress.RowsSeen, &progress.RowsWritten)
	if errors.Is(err, pgx.ErrNoRows) {
		return SourceFileProgress{}, false, nil
	}
	if err != nil {
		return SourceFileProgress{}, false, errors.Wrap(err, "get se source file progress")
	}
	return progress, true, nil
}

func (g *Gateway) GetResumableSourceFileByHash(ctx context.Context, datasetKey string, payloadHash string) (SourceFileProgress, bool, error) {
	if g == nil || g.pool == nil {
		return SourceFileProgress{}, false, errors.New("se workflow database pool not available")
	}
	if strings.TrimSpace(datasetKey) == "" || strings.TrimSpace(payloadHash) == "" {
		return SourceFileProgress{}, false, nil
	}

	var progress SourceFileProgress
	err := g.pool.QueryRow(ctx, `
		WITH candidates AS (
			SELECT
				source_file.id,
				source_file.status,
				GREATEST(
					source_file.rows_seen,
					CASE source_file.dataset_key
						WHEN 'bolagsverket' THEN COALESCE((
							SELECT max(row_number)
							FROM se_workflow.bolagsverket_raw_records raw
							WHERE raw.source_file_id = source_file.id
						), 0)
						WHEN 'scb' THEN COALESCE((
							SELECT max(row_number)
							FROM se_workflow.scb_raw_records raw
							WHERE raw.source_file_id = source_file.id
						), 0)
						ELSE COALESCE((
							SELECT count(*)::integer
							FROM se_workflow.raw_records raw
							WHERE raw.source_file_id = source_file.id
						), 0)
					END
				)::integer AS rows_seen,
				GREATEST(
					source_file.rows_written,
					CASE source_file.dataset_key
						WHEN 'bolagsverket' THEN COALESCE((
							SELECT count(*)::integer
							FROM se_workflow.bolagsverket_raw_records raw
							WHERE raw.source_file_id = source_file.id
						), 0)
						WHEN 'scb' THEN COALESCE((
							SELECT count(*)::integer
							FROM se_workflow.scb_raw_records raw
							WHERE raw.source_file_id = source_file.id
						), 0)
						ELSE COALESCE((
							SELECT count(*)::integer
							FROM se_workflow.raw_records raw
							WHERE raw.source_file_id = source_file.id
						), 0)
					END
				)::integer AS rows_written,
				source_file.updated_at,
				source_file.created_at
			FROM se_workflow.source_files source_file
			WHERE source_file.dataset_key = $1
			  AND source_file.payload_hash = $2
			  AND source_file.status IN ('downloaded', 'failed')
		)
		SELECT id, status, rows_seen, rows_written
		FROM candidates
		WHERE rows_seen > 0
		ORDER BY rows_seen DESC, updated_at DESC, created_at DESC
		LIMIT 1
	`, datasetKey, payloadHash).Scan(&progress.ID, &progress.Status, &progress.RowsSeen, &progress.RowsWritten)
	if errors.Is(err, pgx.ErrNoRows) {
		return SourceFileProgress{}, false, nil
	}
	if err != nil {
		return SourceFileProgress{}, false, errors.Wrap(err, "get resumable se source file by hash")
	}
	return progress, true, nil
}

func (g *Gateway) UpdateSourceFileProgress(ctx context.Context, params UpdateSourceFileProgressParams) error {
	if g == nil || g.pool == nil {
		return errors.New("se workflow database pool not available")
	}
	if params.ID == uuid.Nil {
		return errors.New("se source file id is required")
	}
	if params.RowsSeen < 0 || params.RowsWritten < 0 {
		return errors.New("se source file progress cannot be negative")
	}
	if _, err := g.pool.Exec(ctx, `
		UPDATE se_workflow.source_files
		SET
			rows_seen = GREATEST(rows_seen, $2),
			rows_written = GREATEST(rows_written, $3),
			updated_at = now()
		WHERE id = $1
	`, params.ID, params.RowsSeen, params.RowsWritten); err != nil {
		return errors.Wrap(err, "update se source file progress")
	}
	return nil
}

func (g *Gateway) ReattachSourceFileToSnapshot(ctx context.Context, params ReattachSourceFileToSnapshotParams) error {
	if g == nil || g.pool == nil {
		return errors.New("se workflow database pool not available")
	}
	if params.ID == uuid.Nil {
		return errors.New("se source file id is required")
	}
	if params.BulkSnapshotID == uuid.Nil {
		return errors.New("se bulk snapshot id is required")
	}
	return g.withTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE se_workflow.source_files
			SET
				bulk_snapshot_id = $2,
				source_url = $3,
				file_name = $4,
				file_format = $5,
				content_type = $6,
				content_length_bytes = $7,
				payload_hash = $8,
				rows_seen = GREATEST(rows_seen, $9),
				rows_written = GREATEST(rows_written, $10),
				status = 'downloaded',
				error = NULL,
				metadata = COALESCE($11::jsonb, metadata),
				updated_at = now()
			WHERE id = $1
		`, params.ID, params.BulkSnapshotID, params.SourceURL, params.FileName, params.FileFormat,
			params.ContentType, params.ContentLengthBytes, params.PayloadHash, params.RowsSeen,
			params.RowsWritten, jsonObject(params.Metadata)); err != nil {
			return errors.Wrap(err, "reattach se source file")
		}
		for _, tableName := range []string{
			"se_workflow.raw_records",
			"se_workflow.bolagsverket_raw_records",
			"se_workflow.scb_raw_records",
		} {
			if _, err := tx.Exec(ctx, `
				UPDATE `+tableName+`
				SET bulk_snapshot_id = $2
				WHERE source_file_id = $1
			`, params.ID, params.BulkSnapshotID); err != nil {
				return errors.Wrapf(err, "reattach se source records in %s", tableName)
			}
		}
		return nil
	})
}

package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/activity"

	ariregisterbulk "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/bulk"
	ariregisterdb "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/db"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

const (
	defaultBulkIngestDBBatchSize int32 = 1000
	ariregisterBulkRunType             = "bulk_ingest"
	ariregisterBulkSource              = "ariregister_bulk"
)

type BulkIngestActions struct {
	gateway    *ariregisterdb.Gateway
	httpClient *http.Client
	sourceURL  string
}

func NewBulkIngestActions(gateway *ariregisterdb.Gateway, httpClient *http.Client, sourceURL string) *BulkIngestActions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &BulkIngestActions{gateway: gateway, httpClient: httpClient, sourceURL: strings.TrimSpace(sourceURL)}
}

type LoadAriregisterBulkRawRecordsActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	SourceURL          string `json:"source_url,omitempty"`
	Limit              int32  `json:"limit"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type LoadAriregisterBulkRawRecordsActivityResult struct {
	RowsSeen              int32  `json:"rows_seen"`
	RowsWritten           int32  `json:"rows_written"`
	RowsInsertedNew       int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged int32  `json:"rows_existing_unchanged"`
	RowsNewVersions       int32  `json:"rows_new_versions"`
	SnapshotID            string `json:"snapshot_id,omitempty"`
	SourceFileID          string `json:"source_file_id,omitempty"`
	FileName              string `json:"file_name,omitempty"`
}

func (a *BulkIngestActions) LoadAriregisterBulkRawRecords(
	ctx context.Context,
	input LoadAriregisterBulkRawRecordsActivityInput,
) (LoadAriregisterBulkRawRecordsActivityResult, error) {
	if a == nil || a.gateway == nil {
		return LoadAriregisterBulkRawRecordsActivityResult{}, errors.New("ariregister bulk ingest gateway not available")
	}
	sourceURL := strings.TrimSpace(input.SourceURL)
	if sourceURL == "" {
		sourceURL = a.sourceURL
	}
	if sourceURL == "" {
		sourceURL = ariregisterbulk.DefaultSourceURL
	}
	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBulkIngestDBBatchSize
	}
	metadata, err := metadataPayload(map[string]any{
		"source":               ariregisterBulkSource,
		"source_url":           sourceURL,
		"trigger":              input.Trigger,
		"temporal_workflow_id": input.TemporalWorkflowID,
	})
	if err != nil {
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	workflowRunID, err := a.gateway.BeginWorkflowRun(ctx, db.BeginAriregisterWorkflowRunParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		RunType:           ariregisterBulkRunType,
		Metadata:          metadata,
	})
	if err != nil {
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}

	var result LoadAriregisterBulkRawRecordsActivityResult
	status := "succeeded"
	var finishError *string
	defer func() {
		if finishErr := a.gateway.FinishWorkflowRun(ctx, db.FinishAriregisterWorkflowRunWithStatsParams{
			Status:           status,
			RecordsSeen:      result.RowsSeen,
			RecordsCompleted: result.RowsWritten,
			RecordsFailed:    result.RowsSeen - result.RowsWritten,
			Error:            finishError,
			ID:               workflowRunID,
		}); finishErr != nil {
			slog.ErrorContext(ctx, "finish ariregister bulk ingest workflow run", "error", finishErr, "workflow_run_id", workflowRunID)
		}
	}()

	staged, err := downloadPayloadWithProgress(ctx, a.httpClient, sourceURL, func(bytesDownloaded int64) {
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":            "downloading",
			"bytes_downloaded": bytesDownloaded,
			"source_url":       sourceURL,
		})
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	defer staged.Close()

	snapshotID, err := a.gateway.CreateBulkSnapshot(ctx, db.CreateAriregisterBulkSnapshotParams{
		WorkflowRunID:      pgUUID(workflowRunID),
		SourceUrl:          sourceURL,
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		Metadata:           metadata,
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	result.SnapshotID = snapshotID.String()

	sourceFileID, err := a.gateway.RecordSourceFile(ctx, db.RecordAriregisterSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         "basic",
		SourceUrl:          sourceURL,
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		Status:             "downloaded",
		Metadata:           metadata,
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	result.SourceFileID = sourceFileID.String()

	if _, err := staged.Reader.Seek(0, io.SeekStart); err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, errors.Wrap(err, "rewind ariregister bulk payload")
	}

	var batch []db.UpsertAriregisterWorkflowRawRecordParams
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ingested, err := a.gateway.IngestRawRecords(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "ingest ariregister bulk raw record batch")
		}
		result.RowsWritten += ingested.RowsWritten
		result.RowsInsertedNew += ingested.RowsInsertedNew
		result.RowsExistingUnchanged += ingested.RowsExistingUnchanged
		result.RowsNewVersions += ingested.RowsNewVersions
		batch = batch[:0]
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":                          "ingesting",
			"rows_seen":                      result.RowsSeen,
			"rows_written":                   result.RowsWritten,
			"rows_inserted_new":              result.RowsInsertedNew,
			"rows_existing_unchanged":        result.RowsExistingUnchanged,
			"rows_new_versions":              result.RowsNewVersions,
			"configured_record_limit":        input.Limit,
			"configured_database_batch_size": batchSize,
		})
		return nil
	}

	streamed, err := ariregisterbulk.StreamRecords(ctx, staged.Reader, input.Limit, func(record ariregisterbulk.Record) error {
		result.RowsSeen++
		batch = append(batch, record.UpsertParams(pgUUID(snapshotID), pgUUID(sourceFileID), metadata))
		if int32(len(batch)) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	result.FileName = streamed.FileName
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("ariregister bulk parser row count mismatch")
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	if err := flush(); err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	_, err = a.gateway.RecordSourceFile(ctx, db.RecordAriregisterSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         "basic",
		SourceUrl:          sourceURL,
		FileName:           optionalString(result.FileName),
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		RowsSeen:           result.RowsSeen,
		RowsWritten:        result.RowsWritten,
		Status:             "parsed",
		Metadata:           metadata,
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}
	if err := a.gateway.MarkBulkSnapshotParsed(ctx, db.MarkAriregisterBulkSnapshotParsedParams{
		Metadata: metadata,
		ID:       snapshotID,
	}); err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadAriregisterBulkRawRecordsActivityResult{}, err
	}

	return result, nil
}

type stagedPayload struct {
	Reader          *os.File
	Path            string
	BytesDownloaded int64
	PayloadHash     string
}

func (p *stagedPayload) Close() {
	if p == nil {
		return
	}
	if p.Reader != nil {
		_ = p.Reader.Close()
	}
	if p.Path != "" {
		_ = os.Remove(p.Path)
	}
}

func downloadPayloadWithProgress(ctx context.Context, httpClient *http.Client, sourceURL string, onProgress func(int64)) (*stagedPayload, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create ariregister bulk download request")
	}
	request.Header.Set("Accept", "*/*")
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, errors.Wrap(err, "download ariregister bulk data")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, errors.Newf("download ariregister bulk data returned status %d", response.StatusCode)
	}
	tempFile, err := os.CreateTemp("", "corpscout-ariregister-bulk-*")
	if err != nil {
		return nil, errors.Wrap(err, "create ariregister bulk temp file")
	}
	staged := &stagedPayload{Reader: tempFile, Path: tempFile.Name()}
	keep := false
	defer func() {
		if !keep {
			staged.Close()
		}
	}()
	hasher := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(tempFile, hasher), response.Body, onProgress)
	if err != nil {
		return nil, errors.Wrap(err, "download ariregister bulk payload body")
	}
	if response.ContentLength >= 0 && written != response.ContentLength {
		return nil, errors.Newf("download ariregister bulk payload incomplete: downloaded %d bytes, expected %d bytes", written, response.ContentLength)
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "rewind ariregister bulk temp file")
	}
	staged.BytesDownloaded = written
	staged.PayloadHash = hex.EncodeToString(hasher.Sum(nil))
	keep = true
	return staged, nil
}

func copyWithContext(ctx context.Context, dst io.Writer, src io.Reader, onProgress func(int64)) (int64, error) {
	var written int64
	buffer := make([]byte, 1024*1024)
	for {
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, readErr := src.Read(buffer)
		if n > 0 {
			writeN, writeErr := dst.Write(buffer[:n])
			written += int64(writeN)
			if writeErr != nil {
				return written, writeErr
			}
			if writeN != n {
				return written, io.ErrShortWrite
			}
			if onProgress != nil {
				onProgress(written)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return written, nil
			}
			return written, readErr
		}
	}
}

func metadataPayload(value map[string]any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "encode ariregister bulk metadata")
	}
	return payload, nil
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func optionalString(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

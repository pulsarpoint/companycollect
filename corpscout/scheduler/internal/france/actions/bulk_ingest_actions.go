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
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.temporal.io/sdk/activity"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	francebulk "github.com/pulsarpoint/corpscout/scheduler/internal/france/bulk"
	francedb "github.com/pulsarpoint/corpscout/scheduler/internal/france/db"
)

const (
	defaultBulkIngestDBBatchSize int32 = 1000
	franceBulkRunType                  = "bulk_ingest"
	franceBulkSource                   = "sirene_bulk"
)

type BulkIngestConfig struct {
	LegalUnitsURL     string
	EstablishmentsURL string
}

type BulkIngestActions struct {
	gateway    *francedb.Gateway
	httpClient *http.Client
	cfg        BulkIngestConfig
}

func NewBulkIngestActions(gateway *francedb.Gateway, httpClient *http.Client, cfg BulkIngestConfig) *BulkIngestActions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &BulkIngestActions{
		gateway:    gateway,
		httpClient: httpClient,
		cfg:        cfg.trimmed(),
	}
}

type LoadFranceBulkRawRecordsActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	LegalUnitsURL      string `json:"legal_units_url,omitempty"`
	EstablishmentsURL  string `json:"establishments_url,omitempty"`
	Limit              int32  `json:"limit"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type LoadFranceBulkRawRecordsActivityResult struct {
	RowsSeen                   int32  `json:"rows_seen"`
	RowsWritten                int32  `json:"rows_written"`
	RowsInsertedNew            int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged      int32  `json:"rows_existing_unchanged"`
	RowsNewVersions            int32  `json:"rows_new_versions"`
	LegalUnitsSeen             int32  `json:"legal_units_seen"`
	LegalUnitsWritten          int32  `json:"legal_units_written"`
	EstablishmentsSeen         int32  `json:"establishments_seen"`
	EstablishmentsWritten      int32  `json:"establishments_written"`
	WorkflowRunID              string `json:"workflow_run_id,omitempty"`
	SnapshotID                 string `json:"snapshot_id,omitempty"`
	LegalUnitsSourceFileID     string `json:"legal_units_source_file_id,omitempty"`
	EstablishmentsSourceFileID string `json:"establishments_source_file_id,omitempty"`
}

type resolvedBulkInput struct {
	LegalUnitsURL     string
	EstablishmentsURL string
	Limit             int32
	BatchSize         int32
}

func (a *BulkIngestActions) LoadFranceBulkRawRecords(
	ctx context.Context,
	input LoadFranceBulkRawRecordsActivityInput,
) (LoadFranceBulkRawRecordsActivityResult, error) {
	if a == nil || a.gateway == nil {
		return LoadFranceBulkRawRecordsActivityResult{}, errors.New("france bulk ingest gateway not available")
	}
	resolved := a.resolveInput(input)
	metadata, err := metadataPayload(map[string]any{
		"source":                franceBulkSource,
		"legal_units_url":       resolved.LegalUnitsURL,
		"establishments_url":    resolved.EstablishmentsURL,
		"record_limit_per_file": resolved.Limit,
		"database_batch_size":   resolved.BatchSize,
		"trigger":               input.Trigger,
		"temporal_workflow_id":  input.TemporalWorkflowID,
	})
	if err != nil {
		return LoadFranceBulkRawRecordsActivityResult{}, err
	}

	workflowRunID, err := a.gateway.BeginWorkflowRun(ctx, db.BeginFranceWorkflowRunParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		RunType:           franceBulkRunType,
		Metadata:          metadata,
	})
	if err != nil {
		return LoadFranceBulkRawRecordsActivityResult{}, err
	}

	var result LoadFranceBulkRawRecordsActivityResult
	result.WorkflowRunID = workflowRunID.String()
	status := "succeeded"
	var finishError *string
	defer func() {
		if finishErr := a.gateway.FinishWorkflowRun(ctx, db.FinishFranceWorkflowRunWithStatsParams{
			Status:           status,
			RecordsSeen:      result.RowsSeen,
			RecordsCompleted: result.RowsWritten,
			RecordsFailed:    result.RowsSeen - result.RowsWritten,
			Error:            finishError,
			ID:               workflowRunID,
		}); finishErr != nil {
			slog.ErrorContext(ctx, "finish france bulk ingest workflow run", "error", finishErr, "workflow_run_id", workflowRunID)
		}
	}()

	snapshotID, err := a.gateway.CreateBulkSnapshot(ctx, db.CreateFranceBulkSnapshotParams{
		WorkflowRunID:  pgUUID(workflowRunID),
		SnapshotDate:   pgDate(time.Now().UTC()),
		DatasetRelease: nil,
		Metadata:       metadata,
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadFranceBulkRawRecordsActivityResult{}, err
	}
	result.SnapshotID = snapshotID.String()

	slog.DebugContext(ctx, "loading france sirene bulk raw records",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"legal_units_url", resolved.LegalUnitsURL,
		"establishments_url", resolved.EstablishmentsURL,
		"limit", resolved.Limit,
		"batch_size", resolved.BatchSize,
		"trigger", input.Trigger,
	)

	legalResult, legalFileID, err := a.loadLegalUnits(ctx, snapshotID, resolved, metadata)
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadFranceBulkRawRecordsActivityResult{}, err
	}
	result.LegalUnitsSourceFileID = legalFileID.String()
	result.LegalUnitsSeen = legalResult.RowsSeen
	result.LegalUnitsWritten = legalResult.RowsWritten
	result.add(legalResult)

	establishmentResult, establishmentFileID, err := a.loadEstablishments(ctx, snapshotID, resolved, metadata)
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadFranceBulkRawRecordsActivityResult{}, err
	}
	result.EstablishmentsSourceFileID = establishmentFileID.String()
	result.EstablishmentsSeen = establishmentResult.RowsSeen
	result.EstablishmentsWritten = establishmentResult.RowsWritten
	result.add(establishmentResult)

	if err := a.gateway.MarkBulkSnapshotParsed(ctx, db.MarkFranceBulkSnapshotParsedParams{
		RecordsSeen:    result.RowsSeen,
		RecordsWritten: result.RowsWritten,
		Metadata:       metadata,
		ID:             snapshotID,
	}); err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadFranceBulkRawRecordsActivityResult{}, err
	}
	return result, nil
}

func (a *BulkIngestActions) loadLegalUnits(
	ctx context.Context,
	snapshotID uuid.UUID,
	input resolvedBulkInput,
	metadata []byte,
) (francedb.IngestRawRecordsResult, uuid.UUID, error) {
	staged, sourceFileID, err := a.downloadAndRecordSourceFile(
		ctx,
		snapshotID,
		francebulk.LegalUnitsDatasetKey,
		francebulk.LegalUnitsResourceID,
		input.LegalUnitsURL,
		metadata,
	)
	if err != nil {
		return francedb.IngestRawRecordsResult{}, uuid.Nil, err
	}
	defer staged.Close()

	var result francedb.IngestRawRecordsResult
	var batch []db.UpsertFranceWorkflowRawLegalUnitParams
	sourceFileUUID := pgUUID(sourceFileID)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ingested, err := a.gateway.IngestLegalUnits(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "ingest france legal unit batch")
		}
		addIngestResult(&result, ingested)
		batch = batch[:0]
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":                          "ingesting_legal_units",
			"legal_units_seen":               result.RowsSeen,
			"legal_units_written":            result.RowsWritten,
			"legal_units_inserted_new":       result.RowsInsertedNew,
			"legal_units_existing_unchanged": result.RowsExistingUnchanged,
			"legal_units_new_versions":       result.RowsNewVersions,
			"configured_record_limit":        input.Limit,
			"configured_database_batch_size": input.BatchSize,
		})
		return nil
	}
	streamed, err := francebulk.StreamLegalUnitsFile(ctx, staged.Path, input.Limit, func(record francebulk.LegalUnitRecord) error {
		batch = append(batch, record.UpsertParams(sourceFileUUID, metadata))
		if int32(len(batch)) >= input.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		_ = a.recordFailedSourceFile(ctx, snapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.LegalUnitsURL, staged, err, metadata)
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	if err := flush(); err != nil {
		_ = a.recordFailedSourceFile(ctx, snapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.LegalUnitsURL, staged, err, metadata)
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("france legal unit stream row count mismatch")
		_ = a.recordFailedSourceFile(ctx, snapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.LegalUnitsURL, staged, err, metadata)
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	if _, err := a.recordParsedSourceFile(ctx, snapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.LegalUnitsURL, staged, result, metadata); err != nil {
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	return result, sourceFileID, nil
}

func (a *BulkIngestActions) loadEstablishments(
	ctx context.Context,
	snapshotID uuid.UUID,
	input resolvedBulkInput,
	metadata []byte,
) (francedb.IngestRawRecordsResult, uuid.UUID, error) {
	staged, sourceFileID, err := a.downloadAndRecordSourceFile(
		ctx,
		snapshotID,
		francebulk.EstablishmentsDatasetKey,
		francebulk.EstablishmentsResourceID,
		input.EstablishmentsURL,
		metadata,
	)
	if err != nil {
		return francedb.IngestRawRecordsResult{}, uuid.Nil, err
	}
	defer staged.Close()

	var result francedb.IngestRawRecordsResult
	var batch []db.UpsertFranceWorkflowRawEstablishmentParams
	sourceFileUUID := pgUUID(sourceFileID)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ingested, err := a.gateway.IngestEstablishments(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "ingest france establishment batch")
		}
		addIngestResult(&result, ingested)
		batch = batch[:0]
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":                             "ingesting_establishments",
			"establishments_seen":               result.RowsSeen,
			"establishments_written":            result.RowsWritten,
			"establishments_inserted_new":       result.RowsInsertedNew,
			"establishments_existing_unchanged": result.RowsExistingUnchanged,
			"establishments_new_versions":       result.RowsNewVersions,
			"configured_record_limit":           input.Limit,
			"configured_database_batch_size":    input.BatchSize,
		})
		return nil
	}
	streamed, err := francebulk.StreamEstablishmentsFile(ctx, staged.Path, input.Limit, func(record francebulk.EstablishmentRecord) error {
		batch = append(batch, record.UpsertParams(sourceFileUUID, metadata))
		if int32(len(batch)) >= input.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		_ = a.recordFailedSourceFile(ctx, snapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.EstablishmentsURL, staged, err, metadata)
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	if err := flush(); err != nil {
		_ = a.recordFailedSourceFile(ctx, snapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.EstablishmentsURL, staged, err, metadata)
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("france establishment stream row count mismatch")
		_ = a.recordFailedSourceFile(ctx, snapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.EstablishmentsURL, staged, err, metadata)
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	if _, err := a.recordParsedSourceFile(ctx, snapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.EstablishmentsURL, staged, result, metadata); err != nil {
		return francedb.IngestRawRecordsResult{}, sourceFileID, err
	}
	return result, sourceFileID, nil
}

func (a *BulkIngestActions) downloadAndRecordSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	datasetKey string,
	resourceID string,
	sourceURL string,
	metadata []byte,
) (*stagedPayload, uuid.UUID, error) {
	staged, err := downloadPayloadWithProgress(ctx, a.httpClient, sourceURL, func(bytesDownloaded int64) {
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":            "downloading",
			"dataset_key":      datasetKey,
			"bytes_downloaded": bytesDownloaded,
			"source_url":       sourceURL,
		})
	})
	if err != nil {
		return nil, uuid.Nil, err
	}
	sourceFileID, err := a.gateway.RecordSourceFile(ctx, db.RecordFranceSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         datasetKey,
		ResourceID:         resourceID,
		StableUrl:          sourceURL,
		ResolvedUrl:        optionalString(staged.ResolvedURL),
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         "parquet",
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		ChecksumType:       optionalString("sha256"),
		ChecksumValue:      optionalString(staged.PayloadHash),
		Status:             "downloaded",
		Metadata:           metadata,
	})
	if err != nil {
		staged.Close()
		return nil, uuid.Nil, err
	}
	return staged, sourceFileID, nil
}

func (a *BulkIngestActions) recordParsedSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	datasetKey string,
	resourceID string,
	sourceURL string,
	staged *stagedPayload,
	result francedb.IngestRawRecordsResult,
	metadata []byte,
) (uuid.UUID, error) {
	return a.gateway.RecordSourceFile(ctx, db.RecordFranceSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         datasetKey,
		ResourceID:         resourceID,
		StableUrl:          sourceURL,
		ResolvedUrl:        optionalString(staged.ResolvedURL),
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         "parquet",
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		ChecksumType:       optionalString("sha256"),
		ChecksumValue:      optionalString(staged.PayloadHash),
		RowsSeen:           result.RowsSeen,
		RowsWritten:        result.RowsWritten,
		Status:             "parsed",
		Metadata:           metadata,
	})
}

func (a *BulkIngestActions) recordFailedSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	datasetKey string,
	resourceID string,
	sourceURL string,
	staged *stagedPayload,
	cause error,
	metadata []byte,
) error {
	message := cause.Error()
	_, err := a.gateway.RecordSourceFile(ctx, db.RecordFranceSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         datasetKey,
		ResourceID:         resourceID,
		StableUrl:          sourceURL,
		ResolvedUrl:        optionalString(staged.ResolvedURL),
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         "parquet",
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		ChecksumType:       optionalString("sha256"),
		ChecksumValue:      optionalString(staged.PayloadHash),
		Status:             "failed",
		Error:              &message,
		Metadata:           metadata,
	})
	return err
}

func (a *BulkIngestActions) resolveInput(input LoadFranceBulkRawRecordsActivityInput) resolvedBulkInput {
	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBulkIngestDBBatchSize
	}
	return resolvedBulkInput{
		LegalUnitsURL:     firstNonEmpty(input.LegalUnitsURL, a.cfg.LegalUnitsURL, francebulk.DefaultLegalUnitsSourceURL),
		EstablishmentsURL: firstNonEmpty(input.EstablishmentsURL, a.cfg.EstablishmentsURL, francebulk.DefaultEstablishmentsSourceURL),
		Limit:             input.Limit,
		BatchSize:         batchSize,
	}
}

type stagedPayload struct {
	Path            string
	BytesDownloaded int64
	PayloadHash     string
	ContentType     string
	ResolvedURL     string
}

func (p *stagedPayload) Close() {
	if p == nil || p.Path == "" {
		return
	}
	_ = os.Remove(p.Path)
}

func downloadPayloadWithProgress(ctx context.Context, httpClient *http.Client, sourceURL string, onProgress func(int64)) (*stagedPayload, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create france bulk download request")
	}
	request.Header.Set("Accept", "application/octet-stream,application/x-parquet,*/*")
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, errors.Wrap(err, "download france bulk data")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, errors.Newf("download france bulk data returned status %d", response.StatusCode)
	}

	tempFile, err := os.CreateTemp("", "corpscout-france-sirene-*.parquet")
	if err != nil {
		return nil, errors.Wrap(err, "create france bulk temp file")
	}
	staged := &stagedPayload{Path: tempFile.Name()}
	keep := false
	defer func() {
		_ = tempFile.Close()
		if !keep {
			staged.Close()
		}
	}()

	hasher := sha256.New()
	written, err := copyWithContext(ctx, io.MultiWriter(tempFile, hasher), response.Body, onProgress)
	if err != nil {
		return nil, errors.Wrap(err, "download france bulk payload body")
	}
	if response.ContentLength >= 0 && written != response.ContentLength {
		return nil, errors.Newf("download france bulk payload incomplete: downloaded %d bytes, expected %d bytes", written, response.ContentLength)
	}
	staged.BytesDownloaded = written
	staged.PayloadHash = hex.EncodeToString(hasher.Sum(nil))
	staged.ContentType = response.Header.Get("Content-Type")
	if response.Request != nil && response.Request.URL != nil {
		staged.ResolvedURL = response.Request.URL.String()
	}
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
		return nil, errors.Wrap(err, "encode france bulk metadata")
	}
	return payload, nil
}

func (r *LoadFranceBulkRawRecordsActivityResult) add(result francedb.IngestRawRecordsResult) {
	r.RowsSeen += result.RowsSeen
	r.RowsWritten += result.RowsWritten
	r.RowsInsertedNew += result.RowsInsertedNew
	r.RowsExistingUnchanged += result.RowsExistingUnchanged
	r.RowsNewVersions += result.RowsNewVersions
}

func addIngestResult(target *francedb.IngestRawRecordsResult, result francedb.IngestRawRecordsResult) {
	target.RowsSeen += result.RowsSeen
	target.RowsWritten += result.RowsWritten
	target.RowsInsertedNew += result.RowsInsertedNew
	target.RowsExistingUnchanged += result.RowsExistingUnchanged
	target.RowsNewVersions += result.RowsNewVersions
	target.RawRecordIDs = append(target.RawRecordIDs, result.RawRecordIDs...)
}

func (cfg BulkIngestConfig) trimmed() BulkIngestConfig {
	return BulkIngestConfig{
		LegalUnitsURL:     strings.TrimSpace(cfg.LegalUnitsURL),
		EstablishmentsURL: strings.TrimSpace(cfg.EstablishmentsURL),
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func pgUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func pgDate(value time.Time) pgtype.Date {
	return pgtype.Date{Time: value, Valid: true}
}

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

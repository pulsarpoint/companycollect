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
	"sync"
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
	defaultBulkIngestDBBatchSize     int32 = 5000
	franceBulkBatchHeartbeatInterval       = time.Minute
	defaultFranceBulkStagingRoot           = "/var/lib/corpscout/worksets/france-sirene"
	franceBulkRunType                      = "bulk_ingest"
	franceBulkSource                       = "sirene_bulk"
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

type StageFranceBulkRawFilesActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id,omitempty"`
	LegalUnitsURL      string `json:"legal_units_url,omitempty"`
	EstablishmentsURL  string `json:"establishments_url,omitempty"`
	Limit              int32  `json:"limit"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Trigger            string `json:"trigger,omitempty"`
}

type StageFranceBulkRawFilesActivityResult struct {
	WorkflowRunID                 string `json:"workflow_run_id,omitempty"`
	SnapshotID                    string `json:"snapshot_id,omitempty"`
	LegalUnitsSourceFileID        string `json:"legal_units_source_file_id,omitempty"`
	EstablishmentsSourceFileID    string `json:"establishments_source_file_id,omitempty"`
	LegalUnitsPath                string `json:"legal_units_path,omitempty"`
	EstablishmentsPath            string `json:"establishments_path,omitempty"`
	LegalUnitsURL                 string `json:"legal_units_url,omitempty"`
	EstablishmentsURL             string `json:"establishments_url,omitempty"`
	LegalUnitsResolvedURL         string `json:"legal_units_resolved_url,omitempty"`
	EstablishmentsResolvedURL     string `json:"establishments_resolved_url,omitempty"`
	LegalUnitsContentType         string `json:"legal_units_content_type,omitempty"`
	EstablishmentsContentType     string `json:"establishments_content_type,omitempty"`
	LegalUnitsBytesDownloaded     int64  `json:"legal_units_bytes_downloaded,omitempty"`
	EstablishmentsBytesDownloaded int64  `json:"establishments_bytes_downloaded,omitempty"`
	LegalUnitsChecksumValue       string `json:"legal_units_checksum_value,omitempty"`
	EstablishmentsChecksumValue   string `json:"establishments_checksum_value,omitempty"`
	Limit                         int32  `json:"limit"`
	BatchSize                     int32  `json:"batch_size,omitempty"`
	Metadata                      []byte `json:"metadata,omitempty"`
}

type ProcessFranceSireneDatasetResult struct {
	RowsSeen              int32 `json:"rows_seen"`
	RowsWritten           int32 `json:"rows_written"`
	RowsInsertedNew       int32 `json:"rows_inserted_new"`
	RowsExistingUnchanged int32 `json:"rows_existing_unchanged"`
	RowsNewVersions       int32 `json:"rows_new_versions"`
}

type ProcessFranceSireneStockUniteLegaleActivityInput struct {
	SnapshotID         string `json:"snapshot_id"`
	SourceFileID       string `json:"source_file_id"`
	SourcePath         string `json:"source_path"`
	SourceURL          string `json:"source_url"`
	ResolvedURL        string `json:"resolved_url,omitempty"`
	ContentType        string `json:"content_type,omitempty"`
	ContentLengthBytes int64  `json:"content_length_bytes,omitempty"`
	ChecksumValue      string `json:"checksum_value,omitempty"`
	Limit              int32  `json:"limit"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Metadata           []byte `json:"metadata,omitempty"`
}

type ProcessFranceSireneStockEtablissementActivityInput struct {
	SnapshotID         string `json:"snapshot_id"`
	SourceFileID       string `json:"source_file_id"`
	SourcePath         string `json:"source_path"`
	SourceURL          string `json:"source_url"`
	ResolvedURL        string `json:"resolved_url,omitempty"`
	ContentType        string `json:"content_type,omitempty"`
	ContentLengthBytes int64  `json:"content_length_bytes,omitempty"`
	ChecksumValue      string `json:"checksum_value,omitempty"`
	Limit              int32  `json:"limit"`
	BatchSize          int32  `json:"batch_size,omitempty"`
	Metadata           []byte `json:"metadata,omitempty"`
}

type FinishFranceBulkRawIngestActivityInput struct {
	WorkflowRunID string `json:"workflow_run_id"`
	SnapshotID    string `json:"snapshot_id"`

	RowsSeen              int32 `json:"rows_seen"`
	RowsWritten           int32 `json:"rows_written"`
	RowsInsertedNew       int32 `json:"rows_inserted_new"`
	RowsExistingUnchanged int32 `json:"rows_existing_unchanged"`
	RowsNewVersions       int32 `json:"rows_new_versions"`

	Metadata []byte `json:"metadata,omitempty"`
}

type MarkFranceBulkRawIngestFailedActivityInput struct {
	TemporalWorkflowID string `json:"temporal_workflow_id"`
	Error              string `json:"error,omitempty"`
}

type resolvedBulkInput struct {
	LegalUnitsURL     string
	EstablishmentsURL string
	Limit             int32
	BatchSize         int32
}

func (a *BulkIngestActions) StageFranceBulkRawFiles(
	ctx context.Context,
	input StageFranceBulkRawFilesActivityInput,
) (StageFranceBulkRawFilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return StageFranceBulkRawFilesActivityResult{}, errors.New("france bulk ingest gateway not available")
	}
	resolved := a.resolveInput(input)
	metadata, err := metadataPayload(map[string]any{
		"source":                franceBulkSource,
		"legal_units_url":       resolved.LegalUnitsURL,
		"establishments_url":    resolved.EstablishmentsURL,
		"staging_root":          defaultFranceBulkStagingRoot,
		"record_limit_per_file": resolved.Limit,
		"database_batch_size":   resolved.BatchSize,
		"trigger":               input.Trigger,
		"temporal_workflow_id":  input.TemporalWorkflowID,
	})
	if err != nil {
		return StageFranceBulkRawFilesActivityResult{}, err
	}

	workflowRunID, err := a.gateway.BeginWorkflowRun(ctx, db.BeginFranceWorkflowRunParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		RunType:           franceBulkRunType,
		Metadata:          metadata,
	})
	if err != nil {
		return StageFranceBulkRawFilesActivityResult{}, err
	}

	snapshotID, err := a.gateway.CreateBulkSnapshot(ctx, db.CreateFranceBulkSnapshotParams{
		WorkflowRunID:  pgUUID(workflowRunID),
		SnapshotDate:   pgDate(time.Now().UTC()),
		DatasetRelease: nil,
		Metadata:       metadata,
	})
	if err != nil {
		return StageFranceBulkRawFilesActivityResult{}, err
	}

	slog.DebugContext(ctx, "staging france sirene bulk raw files",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"legal_units_url", resolved.LegalUnitsURL,
		"establishments_url", resolved.EstablishmentsURL,
		"staging_root", defaultFranceBulkStagingRoot,
		"limit", resolved.Limit,
		"batch_size", resolved.BatchSize,
		"trigger", input.Trigger,
	)

	legalStaged, legalFileID, err := a.downloadAndRecordSourceFile(
		ctx,
		snapshotID,
		francebulk.LegalUnitsDatasetKey,
		francebulk.LegalUnitsResourceID,
		resolved.LegalUnitsURL,
		stagingPath(defaultFranceBulkStagingRoot, input.TemporalWorkflowID, francebulk.LegalUnitsDatasetKey),
		metadata,
	)
	if err != nil {
		return StageFranceBulkRawFilesActivityResult{}, err
	}

	establishmentStaged, establishmentFileID, err := a.downloadAndRecordSourceFile(
		ctx,
		snapshotID,
		francebulk.EstablishmentsDatasetKey,
		francebulk.EstablishmentsResourceID,
		resolved.EstablishmentsURL,
		stagingPath(defaultFranceBulkStagingRoot, input.TemporalWorkflowID, francebulk.EstablishmentsDatasetKey),
		metadata,
	)
	if err != nil {
		return StageFranceBulkRawFilesActivityResult{}, err
	}

	return StageFranceBulkRawFilesActivityResult{
		WorkflowRunID:                 workflowRunID.String(),
		SnapshotID:                    snapshotID.String(),
		LegalUnitsSourceFileID:        legalFileID.String(),
		EstablishmentsSourceFileID:    establishmentFileID.String(),
		LegalUnitsPath:                legalStaged.Path,
		EstablishmentsPath:            establishmentStaged.Path,
		LegalUnitsURL:                 resolved.LegalUnitsURL,
		EstablishmentsURL:             resolved.EstablishmentsURL,
		LegalUnitsResolvedURL:         legalStaged.ResolvedURL,
		EstablishmentsResolvedURL:     establishmentStaged.ResolvedURL,
		LegalUnitsContentType:         legalStaged.ContentType,
		EstablishmentsContentType:     establishmentStaged.ContentType,
		LegalUnitsBytesDownloaded:     legalStaged.BytesDownloaded,
		EstablishmentsBytesDownloaded: establishmentStaged.BytesDownloaded,
		LegalUnitsChecksumValue:       legalStaged.PayloadHash,
		EstablishmentsChecksumValue:   establishmentStaged.PayloadHash,
		Limit:                         resolved.Limit,
		BatchSize:                     resolved.BatchSize,
		Metadata:                      metadata,
	}, nil
}

func (a *BulkIngestActions) FinishFranceBulkRawIngest(ctx context.Context, input FinishFranceBulkRawIngestActivityInput) error {
	if a == nil || a.gateway == nil {
		return errors.New("france bulk ingest gateway not available")
	}
	workflowRunID, err := parseRequiredUUID(input.WorkflowRunID, "workflow_run_id")
	if err != nil {
		return err
	}
	snapshotID, err := parseRequiredUUID(input.SnapshotID, "snapshot_id")
	if err != nil {
		return err
	}
	if err := a.gateway.MarkBulkSnapshotParsed(ctx, db.MarkFranceBulkSnapshotParsedParams{
		RecordsSeen:    input.RowsSeen,
		RecordsWritten: input.RowsWritten,
		Metadata:       input.Metadata,
		ID:             snapshotID,
	}); err != nil {
		return err
	}
	if err := a.gateway.FinishWorkflowRun(ctx, db.FinishFranceWorkflowRunWithStatsParams{
		Status:           "succeeded",
		RecordsSeen:      input.RowsSeen,
		RecordsCompleted: input.RowsWritten,
		RecordsFailed:    input.RowsSeen - input.RowsWritten,
		Error:            nil,
		ID:               workflowRunID,
	}); err != nil {
		return err
	}
	return nil
}

func (a *BulkIngestActions) ProcessFranceSireneStockUniteLegale(
	ctx context.Context,
	input ProcessFranceSireneStockUniteLegaleActivityInput,
) (ProcessFranceSireneDatasetResult, error) {
	if a == nil || a.gateway == nil {
		return ProcessFranceSireneDatasetResult{}, errors.New("france bulk ingest gateway not available")
	}
	snapshotID, sourceFileID, err := parseProcessSourceFileIDs(input.SnapshotID, input.SourceFileID)
	if err != nil {
		return ProcessFranceSireneDatasetResult{}, err
	}
	result, err := a.processLegalUnits(ctx, processSourceFileInput{
		SnapshotID:         snapshotID,
		SourceFileID:       sourceFileID,
		SourcePath:         input.SourcePath,
		SourceURL:          input.SourceURL,
		ResolvedURL:        input.ResolvedURL,
		ContentType:        input.ContentType,
		ContentLengthBytes: input.ContentLengthBytes,
		ChecksumValue:      input.ChecksumValue,
		Limit:              input.Limit,
		BatchSize:          normalizeBatchSize(input.BatchSize),
		Metadata:           input.Metadata,
	})
	if err != nil {
		return ProcessFranceSireneDatasetResult{}, err
	}
	return datasetResult(result), nil
}

func (a *BulkIngestActions) ProcessFranceSireneStockEtablissement(
	ctx context.Context,
	input ProcessFranceSireneStockEtablissementActivityInput,
) (ProcessFranceSireneDatasetResult, error) {
	if a == nil || a.gateway == nil {
		return ProcessFranceSireneDatasetResult{}, errors.New("france bulk ingest gateway not available")
	}
	snapshotID, sourceFileID, err := parseProcessSourceFileIDs(input.SnapshotID, input.SourceFileID)
	if err != nil {
		return ProcessFranceSireneDatasetResult{}, err
	}
	result, err := a.processEstablishments(ctx, processSourceFileInput{
		SnapshotID:         snapshotID,
		SourceFileID:       sourceFileID,
		SourcePath:         input.SourcePath,
		SourceURL:          input.SourceURL,
		ResolvedURL:        input.ResolvedURL,
		ContentType:        input.ContentType,
		ContentLengthBytes: input.ContentLengthBytes,
		ChecksumValue:      input.ChecksumValue,
		Limit:              input.Limit,
		BatchSize:          normalizeBatchSize(input.BatchSize),
		Metadata:           input.Metadata,
	})
	if err != nil {
		return ProcessFranceSireneDatasetResult{}, err
	}
	return datasetResult(result), nil
}

func (a *BulkIngestActions) MarkFranceBulkRawIngestFailed(
	ctx context.Context,
	input MarkFranceBulkRawIngestFailedActivityInput,
) error {
	if a == nil || a.gateway == nil {
		return errors.New("france bulk ingest gateway not available")
	}
	if strings.TrimSpace(input.TemporalWorkflowID) == "" {
		return errors.New("temporal workflow id is required")
	}
	return a.gateway.FailWorkflowRunByOrchestrator(ctx, db.FailFranceWorkflowRunByOrchestratorParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		Error:             optionalString(input.Error),
	})
}

type processSourceFileInput struct {
	SnapshotID         uuid.UUID
	SourceFileID       uuid.UUID
	SourcePath         string
	SourceURL          string
	ResolvedURL        string
	ContentType        string
	ContentLengthBytes int64
	ChecksumValue      string
	Limit              int32
	BatchSize          int32
	Metadata           []byte
}

func (a *BulkIngestActions) processLegalUnits(
	ctx context.Context,
	input processSourceFileInput,
) (francedb.IngestRawRecordsResult, error) {
	staged := stagedPayloadFromProcessInput(input)

	var result francedb.IngestRawRecordsResult
	var batch []db.UpsertFranceWorkflowRawLegalUnitParams
	sourceFileUUID := pgUUID(input.SourceFileID)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		progress := result
		pendingBatchSize := int32(len(batch))
		if err := runWithPeriodicHeartbeat(ctx, franceBulkBatchHeartbeatInterval, func() {
			activity.RecordHeartbeat(ctx, legalUnitsHeartbeatDetails(input, progress, pendingBatchSize, "ingesting_legal_units_batch"))
		}, func() error {
			ingested, err := a.gateway.IngestLegalUnits(ctx, batch)
			if err != nil {
				return errors.Wrap(err, "ingest france legal unit batch")
			}
			addIngestResult(&result, ingested)
			batch = batch[:0]
			return nil
		}); err != nil {
			return err
		}
		activity.RecordHeartbeat(ctx, legalUnitsHeartbeatDetails(input, result, 0, "ingesting_legal_units"))
		return nil
	}
	streamed, err := francebulk.StreamLegalUnitsFile(ctx, input.SourcePath, input.Limit, func(record francebulk.LegalUnitRecord) error {
		batch = append(batch, record.UpsertParams(sourceFileUUID, input.Metadata))
		if int32(len(batch)) >= input.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		_ = a.recordFailedSourceFile(ctx, input.SnapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.SourceURL, staged, err, input.Metadata)
		return francedb.IngestRawRecordsResult{}, err
	}
	if err := flush(); err != nil {
		_ = a.recordFailedSourceFile(ctx, input.SnapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.SourceURL, staged, err, input.Metadata)
		return francedb.IngestRawRecordsResult{}, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("france legal unit stream row count mismatch")
		_ = a.recordFailedSourceFile(ctx, input.SnapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.SourceURL, staged, err, input.Metadata)
		return francedb.IngestRawRecordsResult{}, err
	}
	if _, err := a.recordParsedSourceFile(ctx, input.SnapshotID, francebulk.LegalUnitsDatasetKey, francebulk.LegalUnitsResourceID, input.SourceURL, staged, result, input.Metadata); err != nil {
		return francedb.IngestRawRecordsResult{}, err
	}
	return result, nil
}

func (a *BulkIngestActions) processEstablishments(
	ctx context.Context,
	input processSourceFileInput,
) (francedb.IngestRawRecordsResult, error) {
	staged := stagedPayloadFromProcessInput(input)
	var result francedb.IngestRawRecordsResult
	var batch []db.UpsertFranceWorkflowRawEstablishmentParams
	sourceFileUUID := pgUUID(input.SourceFileID)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		progress := result
		pendingBatchSize := int32(len(batch))
		if err := runWithPeriodicHeartbeat(ctx, franceBulkBatchHeartbeatInterval, func() {
			activity.RecordHeartbeat(ctx, establishmentsHeartbeatDetails(input, progress, pendingBatchSize, "ingesting_establishments_batch"))
		}, func() error {
			ingested, err := a.gateway.IngestEstablishments(ctx, batch)
			if err != nil {
				return errors.Wrap(err, "ingest france establishment batch")
			}
			addIngestResult(&result, ingested)
			batch = batch[:0]
			return nil
		}); err != nil {
			return err
		}
		activity.RecordHeartbeat(ctx, establishmentsHeartbeatDetails(input, result, 0, "ingesting_establishments"))
		return nil
	}
	streamed, err := francebulk.StreamEstablishmentsFile(ctx, input.SourcePath, input.Limit, func(record francebulk.EstablishmentRecord) error {
		batch = append(batch, record.UpsertParams(sourceFileUUID, input.Metadata))
		if int32(len(batch)) >= input.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		_ = a.recordFailedSourceFile(ctx, input.SnapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.SourceURL, staged, err, input.Metadata)
		return francedb.IngestRawRecordsResult{}, err
	}
	if err := flush(); err != nil {
		_ = a.recordFailedSourceFile(ctx, input.SnapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.SourceURL, staged, err, input.Metadata)
		return francedb.IngestRawRecordsResult{}, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("france establishment stream row count mismatch")
		_ = a.recordFailedSourceFile(ctx, input.SnapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.SourceURL, staged, err, input.Metadata)
		return francedb.IngestRawRecordsResult{}, err
	}
	if _, err := a.recordParsedSourceFile(ctx, input.SnapshotID, francebulk.EstablishmentsDatasetKey, francebulk.EstablishmentsResourceID, input.SourceURL, staged, result, input.Metadata); err != nil {
		return francedb.IngestRawRecordsResult{}, err
	}
	return result, nil
}

func (a *BulkIngestActions) downloadAndRecordSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	datasetKey string,
	resourceID string,
	sourceURL string,
	targetPath string,
	metadata []byte,
) (*stagedPayload, uuid.UUID, error) {
	staged, err := downloadPayloadWithProgress(ctx, a.httpClient, sourceURL, targetPath, func(bytesDownloaded int64) {
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
		Metadata:           sourceFileMetadata(metadata, staged.Path),
	})
	if err != nil {
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
		Metadata:           sourceFileMetadata(metadata, staged.Path),
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
		Metadata:           sourceFileMetadata(metadata, staged.Path),
	})
	return err
}

func (a *BulkIngestActions) resolveInput(input StageFranceBulkRawFilesActivityInput) resolvedBulkInput {
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

func downloadPayloadWithProgress(ctx context.Context, httpClient *http.Client, sourceURL string, targetPath string, onProgress func(int64)) (*stagedPayload, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	targetPath = strings.TrimSpace(targetPath)
	if targetPath == "" {
		return nil, errors.New("france bulk target path is required")
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

	targetDir := filepath.Dir(targetPath)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return nil, errors.Wrap(err, "create france bulk staging directory")
	}
	tempFile, err := os.CreateTemp(targetDir, "."+filepath.Base(targetPath)+".*.tmp")
	if err != nil {
		return nil, errors.Wrap(err, "create france bulk staging temp file")
	}
	tempPath := tempFile.Name()
	staged := &stagedPayload{Path: targetPath}
	keep := false
	defer func() {
		_ = tempFile.Close()
		if !keep {
			_ = os.Remove(tempPath)
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
	if err := tempFile.Close(); err != nil {
		return nil, errors.Wrap(err, "close france bulk staging temp file")
	}
	if err := os.Rename(tempPath, targetPath); err != nil {
		return nil, errors.Wrap(err, "move france bulk staging temp file")
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

func sourceFileMetadata(base []byte, localPath string) []byte {
	var metadata map[string]any
	if len(base) > 0 {
		if err := json.Unmarshal(base, &metadata); err != nil {
			metadata = nil
		}
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	if trimmed := strings.TrimSpace(localPath); trimmed != "" {
		metadata["local_path"] = trimmed
	}
	payload, err := json.Marshal(metadata)
	if err != nil {
		return base
	}
	return payload
}

func stagingPath(root string, workflowID string, datasetKey string) string {
	return filepath.Join(root, safePathComponent(workflowID, "france-bulk-ingest"), franceBulkFileName(datasetKey))
}

func franceBulkFileName(datasetKey string) string {
	switch datasetKey {
	case francebulk.LegalUnitsDatasetKey:
		return "stock_unite_legale.parquet"
	case francebulk.EstablishmentsDatasetKey:
		return "stock_etablissement.parquet"
	default:
		return safePathComponent(datasetKey, "source") + ".parquet"
	}
}

func safePathComponent(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		trimmed = fallback
	}
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(trimmed)
}

func parseProcessSourceFileIDs(snapshotIDValue string, sourceFileIDValue string) (uuid.UUID, uuid.UUID, error) {
	snapshotID, err := parseRequiredUUID(snapshotIDValue, "snapshot_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	sourceFileID, err := parseRequiredUUID(sourceFileIDValue, "source_file_id")
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}
	return snapshotID, sourceFileID, nil
}

func parseRequiredUUID(value string, fieldName string) (uuid.UUID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return uuid.Nil, errors.Newf("%s is required", fieldName)
	}
	parsed, err := uuid.Parse(trimmed)
	if err != nil {
		return uuid.Nil, errors.Wrapf(err, "parse %s", fieldName)
	}
	return parsed, nil
}

func normalizeBatchSize(batchSize int32) int32 {
	if batchSize <= 0 {
		return defaultBulkIngestDBBatchSize
	}
	return batchSize
}

func legalUnitsHeartbeatDetails(input processSourceFileInput, result francedb.IngestRawRecordsResult, pendingBatchSize int32, phase string) map[string]any {
	return map[string]any{
		"phase":                          phase,
		"legal_units_seen":               result.RowsSeen,
		"legal_units_written":            result.RowsWritten,
		"legal_units_inserted_new":       result.RowsInsertedNew,
		"legal_units_existing_unchanged": result.RowsExistingUnchanged,
		"legal_units_new_versions":       result.RowsNewVersions,
		"pending_batch_records":          pendingBatchSize,
		"configured_record_limit":        input.Limit,
		"configured_database_batch_size": input.BatchSize,
	}
}

func establishmentsHeartbeatDetails(input processSourceFileInput, result francedb.IngestRawRecordsResult, pendingBatchSize int32, phase string) map[string]any {
	return map[string]any{
		"phase":                             phase,
		"establishments_seen":               result.RowsSeen,
		"establishments_written":            result.RowsWritten,
		"establishments_inserted_new":       result.RowsInsertedNew,
		"establishments_existing_unchanged": result.RowsExistingUnchanged,
		"establishments_new_versions":       result.RowsNewVersions,
		"pending_batch_records":             pendingBatchSize,
		"configured_record_limit":           input.Limit,
		"configured_database_batch_size":    input.BatchSize,
	}
}

func runWithPeriodicHeartbeat(ctx context.Context, interval time.Duration, heartbeat func(), operation func() error) error {
	if heartbeat == nil {
		return operation()
	}
	if interval <= 0 {
		interval = franceBulkBatchHeartbeatInterval
	}
	heartbeat()
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				heartbeat()
			}
		}
	}()
	err := operation()
	close(done)
	wg.Wait()
	return err
}

func stagedPayloadFromProcessInput(input processSourceFileInput) *stagedPayload {
	return &stagedPayload{
		Path:            input.SourcePath,
		BytesDownloaded: input.ContentLengthBytes,
		PayloadHash:     input.ChecksumValue,
		ContentType:     input.ContentType,
		ResolvedURL:     input.ResolvedURL,
	}
}

func datasetResult(result francedb.IngestRawRecordsResult) ProcessFranceSireneDatasetResult {
	return ProcessFranceSireneDatasetResult{
		RowsSeen:              result.RowsSeen,
		RowsWritten:           result.RowsWritten,
		RowsInsertedNew:       result.RowsInsertedNew,
		RowsExistingUnchanged: result.RowsExistingUnchanged,
		RowsNewVersions:       result.RowsNewVersions,
	}
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

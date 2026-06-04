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
	"go.temporal.io/sdk/activity"

	sebulk "github.com/pulsarpoint/corpscout/scheduler/internal/se/bulk"
	sedb "github.com/pulsarpoint/corpscout/scheduler/internal/se/db"
)

const (
	defaultBulkIngestDBBatchSize int32 = 1000
	seBulkRunType                      = "bulk_ingest"
	seBulkSource                       = "se_hvd_bulk"
)

type HVDDatasetConfig struct {
	Dataset string `json:"dataset"`
	URL     string `json:"url"`
	Format  string `json:"format"`
}

type BulkIngestConfig struct {
	DatasetsJSON string
}

type BulkIngestActions struct {
	gateway    *sedb.Gateway
	httpClient *http.Client
	cfg        BulkIngestConfig
}

func NewBulkIngestActions(gateway *sedb.Gateway, httpClient *http.Client, cfg BulkIngestConfig) *BulkIngestActions {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &BulkIngestActions{
		gateway:    gateway,
		httpClient: httpClient,
		cfg:        cfg.trimmed(),
	}
}

type LoadSEBulkRawRecordsActivityInput struct {
	TemporalWorkflowID string             `json:"temporal_workflow_id,omitempty"`
	Datasets           []HVDDatasetConfig `json:"datasets,omitempty"`
	DatasetsJSON       string             `json:"datasets_json,omitempty"`
	Limit              int32              `json:"limit"`
	BatchSize          int32              `json:"batch_size,omitempty"`
	Trigger            string             `json:"trigger,omitempty"`
}

type LoadSEBulkRawRecordsActivityResult struct {
	RowsSeen              int32                         `json:"rows_seen"`
	RowsWritten           int32                         `json:"rows_written"`
	RowsInsertedNew       int32                         `json:"rows_inserted_new"`
	RowsExistingUnchanged int32                         `json:"rows_existing_unchanged"`
	RowsNewVersions       int32                         `json:"rows_new_versions"`
	WorkflowRunID         string                        `json:"workflow_run_id,omitempty"`
	SnapshotID            string                        `json:"snapshot_id,omitempty"`
	SourceFiles           []LoadSEBulkSourceFileResult  `json:"source_files,omitempty"`
	Datasets              []LoadSEBulkDatasetLoadResult `json:"datasets,omitempty"`
}

type LoadSEBulkSourceFileResult struct {
	Dataset               string `json:"dataset"`
	SourceFileID          string `json:"source_file_id"`
	FileName              string `json:"file_name,omitempty"`
	RowsSeen              int32  `json:"rows_seen"`
	RowsWritten           int32  `json:"rows_written"`
	RowsInsertedNew       int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged int32  `json:"rows_existing_unchanged"`
	RowsNewVersions       int32  `json:"rows_new_versions"`
}

type LoadSEBulkDatasetLoadResult struct {
	Dataset     string `json:"dataset"`
	RowsSeen    int32  `json:"rows_seen"`
	RowsWritten int32  `json:"rows_written"`
}

type resolvedBulkInput struct {
	Datasets  []HVDDatasetConfig
	Limit     int32
	BatchSize int32
}

func (a *BulkIngestActions) LoadSEBulkRawRecords(
	ctx context.Context,
	input LoadSEBulkRawRecordsActivityInput,
) (LoadSEBulkRawRecordsActivityResult, error) {
	if a == nil || a.gateway == nil {
		return LoadSEBulkRawRecordsActivityResult{}, errors.New("se bulk ingest gateway not available")
	}
	resolved, err := a.resolveInput(input)
	if err != nil {
		return LoadSEBulkRawRecordsActivityResult{}, err
	}
	metadata, err := metadataPayload(map[string]any{
		"source":                seBulkSource,
		"datasets":              resolved.Datasets,
		"record_limit_per_file": resolved.Limit,
		"database_batch_size":   resolved.BatchSize,
		"trigger":               input.Trigger,
		"temporal_workflow_id":  input.TemporalWorkflowID,
	})
	if err != nil {
		return LoadSEBulkRawRecordsActivityResult{}, err
	}

	workflowRunID, err := a.gateway.BeginWorkflowRun(ctx, sedb.BeginWorkflowRunParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		RunType:           seBulkRunType,
		Metadata:          metadata,
	})
	if err != nil {
		return LoadSEBulkRawRecordsActivityResult{}, err
	}

	var result LoadSEBulkRawRecordsActivityResult
	result.WorkflowRunID = workflowRunID.String()
	status := "succeeded"
	var finishError *string
	defer func() {
		if finishErr := a.gateway.FinishWorkflowRun(ctx, sedb.FinishWorkflowRunParams{
			Status:           status,
			RecordsSeen:      result.RowsSeen,
			RecordsCompleted: result.RowsWritten,
			RecordsFailed:    result.RowsSeen - result.RowsWritten,
			Error:            finishError,
			ID:               workflowRunID,
		}); finishErr != nil {
			slog.ErrorContext(ctx, "finish se bulk ingest workflow run", "error", finishErr, "workflow_run_id", workflowRunID)
		}
	}()

	snapshotID, err := a.gateway.CreateBulkSnapshot(ctx, sedb.CreateBulkSnapshotParams{
		WorkflowRunID: workflowRunID,
		SnapshotKey:   time.Now().UTC().Format("2006-01-02"),
		SnapshotDate:  time.Now().UTC(),
		Metadata:      metadata,
	})
	if err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadSEBulkRawRecordsActivityResult{}, err
	}
	result.SnapshotID = snapshotID.String()

	for _, dataset := range resolved.Datasets {
		fileResult, err := a.loadDataset(ctx, snapshotID, dataset, resolved, metadata, input.TemporalWorkflowID)
		if err != nil {
			status = "failed"
			message := err.Error()
			finishError = &message
			return LoadSEBulkRawRecordsActivityResult{}, err
		}
		result.RowsSeen += fileResult.RowsSeen
		result.RowsWritten += fileResult.RowsWritten
		result.RowsInsertedNew += fileResult.RowsInsertedNew
		result.RowsExistingUnchanged += fileResult.RowsExistingUnchanged
		result.RowsNewVersions += fileResult.RowsNewVersions
		result.SourceFiles = append(result.SourceFiles, fileResult)
		result.Datasets = append(result.Datasets, LoadSEBulkDatasetLoadResult{
			Dataset:     fileResult.Dataset,
			RowsSeen:    fileResult.RowsSeen,
			RowsWritten: fileResult.RowsWritten,
		})
	}
	if err := a.gateway.MarkBulkSnapshotParsed(ctx, sedb.MarkBulkSnapshotParsedParams{
		RecordsSeen:    result.RowsSeen,
		RecordsWritten: result.RowsWritten,
		Metadata:       metadata,
		ID:             snapshotID,
	}); err != nil {
		status = "failed"
		message := err.Error()
		finishError = &message
		return LoadSEBulkRawRecordsActivityResult{}, err
	}
	return result, nil
}

func (a *BulkIngestActions) loadDataset(
	ctx context.Context,
	snapshotID uuid.UUID,
	dataset HVDDatasetConfig,
	input resolvedBulkInput,
	metadata []byte,
	workflowID string,
) (LoadSEBulkSourceFileResult, error) {
	staged, err := downloadPayloadWithProgress(ctx, a.httpClient, dataset.URL, dataset.Format, func(bytesDownloaded int64) {
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":            "downloading",
			"dataset_key":      dataset.Dataset,
			"bytes_downloaded": bytesDownloaded,
			"source_url":       dataset.URL,
		})
	})
	if err != nil {
		return LoadSEBulkSourceFileResult{}, err
	}
	defer staged.Close()

	sourceFileID, err := a.gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         dataset.Dataset,
		SourceURL:          dataset.URL,
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         dataset.Format,
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		Status:             "downloaded",
		Metadata:           metadata,
	})
	if err != nil {
		return LoadSEBulkSourceFileResult{}, err
	}

	var result sedb.IngestRawRecordsResult
	var batch []sedb.RawRecord
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		ingested, err := a.gateway.IngestRawRecords(ctx, batch)
		if err != nil {
			return errors.Wrap(err, "ingest se bulk raw record batch")
		}
		addIngestResult(&result, ingested)
		batch = batch[:0]
		activity.RecordHeartbeat(ctx, map[string]any{
			"phase":                          "ingesting",
			"dataset_key":                    dataset.Dataset,
			"rows_seen":                      result.RowsSeen,
			"rows_written":                   result.RowsWritten,
			"rows_inserted_new":              result.RowsInsertedNew,
			"rows_existing_unchanged":        result.RowsExistingUnchanged,
			"rows_new_versions":              result.RowsNewVersions,
			"configured_record_limit":        input.Limit,
			"configured_database_batch_size": input.BatchSize,
		})
		return nil
	}

	streamed, err := sebulk.StreamRecordsFile(ctx, staged.Path, dataset.Format, input.Limit, func(record sebulk.Record) error {
		batch = append(batch, rawRecordFromBulk(record, sourceFileID, workflowID, metadata))
		if int32(len(batch)) >= input.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		_ = a.recordFailedSourceFile(ctx, snapshotID, dataset, staged, err, metadata)
		return LoadSEBulkSourceFileResult{}, err
	}
	if err := flush(); err != nil {
		_ = a.recordFailedSourceFile(ctx, snapshotID, dataset, staged, err, metadata)
		return LoadSEBulkSourceFileResult{}, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("se bulk parser row count mismatch")
		_ = a.recordFailedSourceFile(ctx, snapshotID, dataset, staged, err, metadata)
		return LoadSEBulkSourceFileResult{}, err
	}
	if _, err := a.recordParsedSourceFile(ctx, snapshotID, dataset, staged, result, metadata); err != nil {
		return LoadSEBulkSourceFileResult{}, err
	}
	return LoadSEBulkSourceFileResult{
		Dataset:               dataset.Dataset,
		SourceFileID:          sourceFileID.String(),
		FileName:              filepath.Base(staged.Path),
		RowsSeen:              result.RowsSeen,
		RowsWritten:           result.RowsWritten,
		RowsInsertedNew:       result.RowsInsertedNew,
		RowsExistingUnchanged: result.RowsExistingUnchanged,
		RowsNewVersions:       result.RowsNewVersions,
	}, nil
}

func rawRecordFromBulk(record sebulk.Record, sourceFileID uuid.UUID, workflowID string, metadata []byte) sedb.RawRecord {
	return sedb.RawRecord{
		SourceFileID:        sourceFileID,
		SourceNativeID:      record.OrganizationNumber,
		OrganizationNumber:  record.OrganizationNumber,
		OrganizationName:    record.OrganizationName,
		RegistrationStatus:  record.RegistrationStatus,
		LegalForm:           record.LegalForm,
		BusinessDescription: record.BusinessDescription,
		SNICodes:            record.SNICodes,
		PostalAddress:       record.PostalAddress,
		RawPayload:          record.RawPayload,
		PayloadHash:         record.PayloadHash,
		RunID:               workflowID,
		Metadata:            metadata,
	}
}

func (a *BulkIngestActions) recordParsedSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	dataset HVDDatasetConfig,
	staged *stagedPayload,
	result sedb.IngestRawRecordsResult,
	metadata []byte,
) (uuid.UUID, error) {
	return a.gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         dataset.Dataset,
		SourceURL:          dataset.URL,
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         dataset.Format,
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		RowsSeen:           result.RowsSeen,
		RowsWritten:        result.RowsWritten,
		Status:             "parsed",
		Metadata:           metadata,
	})
}

func (a *BulkIngestActions) recordFailedSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	dataset HVDDatasetConfig,
	staged *stagedPayload,
	cause error,
	metadata []byte,
) error {
	message := cause.Error()
	_, err := a.gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         dataset.Dataset,
		SourceURL:          dataset.URL,
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         dataset.Format,
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		Status:             "failed",
		Error:              &message,
		Metadata:           metadata,
	})
	return err
}

func (a *BulkIngestActions) resolveInput(input LoadSEBulkRawRecordsActivityInput) (resolvedBulkInput, error) {
	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBulkIngestDBBatchSize
	}
	datasets := input.Datasets
	if len(datasets) == 0 {
		rawConfig := firstNonEmpty(input.DatasetsJSON, a.cfg.DatasetsJSON)
		parsed, err := parseDatasetsJSON(rawConfig)
		if err != nil {
			return resolvedBulkInput{}, err
		}
		datasets = parsed
	}
	return resolvedBulkInput{
		Datasets:  datasets,
		Limit:     input.Limit,
		BatchSize: batchSize,
	}, nil
}

func parseDatasetsJSON(rawConfig string) ([]HVDDatasetConfig, error) {
	rawConfig = strings.TrimSpace(rawConfig)
	if rawConfig == "" {
		return nil, errors.New("SE_HVD_DATASETS_JSON is required for Sweden HVD downloads")
	}
	var datasets []HVDDatasetConfig
	if err := json.Unmarshal([]byte(rawConfig), &datasets); err != nil {
		return nil, errors.Wrap(err, "parse SE_HVD_DATASETS_JSON")
	}
	for i := range datasets {
		datasets[i] = datasets[i].trimmed()
		if datasets[i].Dataset == "" || datasets[i].URL == "" || datasets[i].Format == "" {
			return nil, errors.New("SE_HVD_DATASETS_JSON entries require dataset, url, and format")
		}
	}
	return datasets, nil
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

func downloadPayloadWithProgress(ctx context.Context, httpClient *http.Client, sourceURL string, format string, onProgress func(int64)) (*stagedPayload, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create se hvd download request")
	}
	request.Header.Set("Accept", "*/*")
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	response, err := httpClient.Do(request)
	if err != nil {
		return nil, errors.Wrap(err, "download se hvd data")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, errors.Newf("download se hvd data returned status %d", response.StatusCode)
	}

	tempFile, err := os.CreateTemp("", "corpscout-se-hvd-*."+safeFormatSuffix(format))
	if err != nil {
		return nil, errors.Wrap(err, "create se hvd temp file")
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
		return nil, errors.Wrap(err, "download se hvd payload body")
	}
	if response.ContentLength >= 0 && written != response.ContentLength {
		return nil, errors.Newf("download se hvd payload incomplete: downloaded %d bytes, expected %d bytes", written, response.ContentLength)
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
		return nil, errors.Wrap(err, "encode se bulk metadata")
	}
	return payload, nil
}

func addIngestResult(target *sedb.IngestRawRecordsResult, result sedb.IngestRawRecordsResult) {
	target.RowsSeen += result.RowsSeen
	target.RowsWritten += result.RowsWritten
	target.RowsInsertedNew += result.RowsInsertedNew
	target.RowsExistingUnchanged += result.RowsExistingUnchanged
	target.RowsNewVersions += result.RowsNewVersions
	target.RawRecordIDs = append(target.RawRecordIDs, result.RawRecordIDs...)
}

func (cfg BulkIngestConfig) trimmed() BulkIngestConfig {
	return BulkIngestConfig{DatasetsJSON: strings.TrimSpace(cfg.DatasetsJSON)}
}

func (cfg HVDDatasetConfig) trimmed() HVDDatasetConfig {
	return HVDDatasetConfig{
		Dataset: strings.TrimSpace(cfg.Dataset),
		URL:     strings.TrimSpace(cfg.URL),
		Format:  strings.TrimSpace(cfg.Format),
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

func optionalString(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func safeFormatSuffix(format string) string {
	format = strings.TrimSpace(format)
	if format == "" {
		return "json"
	}
	var b strings.Builder
	for _, r := range format {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '.', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

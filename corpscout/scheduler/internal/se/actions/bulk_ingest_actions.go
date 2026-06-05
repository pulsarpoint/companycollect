package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
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
	seDataSourceName                   = "se"
	defaultSEBulkStagingRoot           = "/tmp/corpscout-worksets/se-hvd"
	maxDiscoveryBodyBytes        int64 = 5 * 1024 * 1024
)

var (
	metadataAccessURLPattern = regexp.MustCompile(`(?is)<dcat:accessURL[^>]*\brdf:resource=["']([^"']+)["']`)
	metadataFormatPattern    = regexp.MustCompile(`(?is)<dcterms:format[^>]*>([^<]+)</dcterms:format>`)
	jsonAccessURLPattern     = regexp.MustCompile(`(?is)"access_?url"\s*:\s*"([^"]+)"`)
	htmlHrefPattern          = regexp.MustCompile(`(?is)\bhref\s*=\s*["']([^"']+)["']`)
)

type HVDDatasetConfig struct {
	Dataset string `json:"dataset"`
	URL     string `json:"url"`
	Format  string `json:"format"`
}

type BulkIngestConfig struct {
	DatasetsJSON string
	StagingRoot  string
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
	SkippedSourceFileID   string `json:"skipped_source_file_id,omitempty"`
	SourceURL             string `json:"source_url,omitempty"`
	FileName              string `json:"file_name,omitempty"`
	FileFormat            string `json:"file_format,omitempty"`
	Status                string `json:"status"`
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

type VerifySEBulkSourceFilesActivityInput struct {
	TemporalWorkflowID string             `json:"temporal_workflow_id,omitempty"`
	Datasets           []HVDDatasetConfig `json:"datasets,omitempty"`
	DatasetsJSON       string             `json:"datasets_json,omitempty"`
	Limit              int32              `json:"limit"`
	BatchSize          int32              `json:"batch_size,omitempty"`
	Trigger            string             `json:"trigger,omitempty"`
}

type VerifySEBulkSourceFilesActivityResult struct {
	WorkflowRunID string                     `json:"workflow_run_id,omitempty"`
	SnapshotID    string                     `json:"snapshot_id,omitempty"`
	Limit         int32                      `json:"limit"`
	BatchSize     int32                      `json:"batch_size,omitempty"`
	Metadata      []byte                     `json:"metadata,omitempty"`
	SourceFiles   []VerifiedSEBulkSourceFile `json:"source_files,omitempty"`
}

type VerifiedSEBulkSourceFile struct {
	Dataset            string `json:"dataset"`
	SourceURL          string `json:"source_url"`
	FileFormat         string `json:"file_format"`
	ContentType        string `json:"content_type,omitempty"`
	ContentLengthBytes int64  `json:"content_length_bytes,omitempty"`
	ETag               string `json:"etag,omitempty"`
	LastModified       string `json:"last_modified,omitempty"`
}

type CompareSEBulkSourceFilesActivityInput struct {
	WorkflowRunID string                     `json:"workflow_run_id,omitempty"`
	SnapshotID    string                     `json:"snapshot_id,omitempty"`
	Limit         int32                      `json:"limit"`
	BatchSize     int32                      `json:"batch_size,omitempty"`
	Metadata      []byte                     `json:"metadata,omitempty"`
	SourceFiles   []VerifiedSEBulkSourceFile `json:"source_files,omitempty"`
}

type CompareSEBulkSourceFilesActivityResult struct {
	WorkflowRunID string                     `json:"workflow_run_id,omitempty"`
	SnapshotID    string                     `json:"snapshot_id,omitempty"`
	Limit         int32                      `json:"limit"`
	BatchSize     int32                      `json:"batch_size,omitempty"`
	Metadata      []byte                     `json:"metadata,omitempty"`
	SourceFiles   []ComparedSEBulkSourceFile `json:"source_files,omitempty"`
}

type ComparedSEBulkSourceFile struct {
	VerifiedSEBulkSourceFile
	NeedsDownload        bool                       `json:"needs_download"`
	SkipReason           string                     `json:"skip_reason,omitempty"`
	ExistingSourceFileID string                     `json:"existing_source_file_id,omitempty"`
	ExistingPayloadHash  string                     `json:"existing_payload_hash,omitempty"`
	SourceFileResult     LoadSEBulkSourceFileResult `json:"source_file_result,omitempty"`
}

type DownloadSEBulkSourceFilesActivityInput struct {
	WorkflowRunID string                     `json:"workflow_run_id,omitempty"`
	SnapshotID    string                     `json:"snapshot_id,omitempty"`
	Limit         int32                      `json:"limit"`
	BatchSize     int32                      `json:"batch_size,omitempty"`
	Metadata      []byte                     `json:"metadata,omitempty"`
	SourceFiles   []ComparedSEBulkSourceFile `json:"source_files,omitempty"`
}

type DownloadSEBulkSourceFilesActivityResult struct {
	WorkflowRunID string                       `json:"workflow_run_id,omitempty"`
	SnapshotID    string                       `json:"snapshot_id,omitempty"`
	Limit         int32                        `json:"limit"`
	BatchSize     int32                        `json:"batch_size,omitempty"`
	Metadata      []byte                       `json:"metadata,omitempty"`
	SourceFiles   []DownloadedSEBulkSourceFile `json:"source_files,omitempty"`
}

type DownloadedSEBulkSourceFile struct {
	ComparedSEBulkSourceFile
	SourceFileID    string `json:"source_file_id,omitempty"`
	SourcePath      string `json:"source_path,omitempty"`
	PayloadHash     string `json:"payload_hash,omitempty"`
	BytesDownloaded int64  `json:"bytes_downloaded,omitempty"`
	Status          string `json:"status"`
}

type ProcessSEBulkRawRecordsActivityInput struct {
	WorkflowRunID      string                       `json:"workflow_run_id,omitempty"`
	SnapshotID         string                       `json:"snapshot_id,omitempty"`
	TemporalWorkflowID string                       `json:"temporal_workflow_id,omitempty"`
	Limit              int32                        `json:"limit"`
	BatchSize          int32                        `json:"batch_size,omitempty"`
	Metadata           []byte                       `json:"metadata,omitempty"`
	SourceFiles        []DownloadedSEBulkSourceFile `json:"source_files,omitempty"`
}

type ProcessSEBulkRawRecordsActivityResult = LoadSEBulkRawRecordsActivityResult

type resolvedBulkInput struct {
	Datasets  []HVDDatasetConfig
	Limit     int32
	BatchSize int32
}

type seDataSourceConfig struct {
	Datasets     []seDataSourceDatasetConfig `json:"datasets"`
	BulkDatasets []seDataSourceDatasetConfig `json:"bulk_datasets"`
	DatasetsJSON string                      `json:"datasets_json"`
}

type seDataSourceDatasetConfig struct {
	Dataset     string `json:"dataset"`
	Key         string `json:"key"`
	URL         string `json:"url"`
	StableURL   string `json:"stable_url"`
	SourceURL   string `json:"source_url"`
	DownloadURL string `json:"download_url"`
	Format      string `json:"format"`
	FileFormat  string `json:"file_format"`
}

func (a *BulkIngestActions) LoadSEBulkRawRecords(
	ctx context.Context,
	input LoadSEBulkRawRecordsActivityInput,
) (LoadSEBulkRawRecordsActivityResult, error) {
	verified, err := a.VerifySEBulkSourceFiles(ctx, VerifySEBulkSourceFilesActivityInput{
		TemporalWorkflowID: input.TemporalWorkflowID,
		Datasets:           input.Datasets,
		DatasetsJSON:       input.DatasetsJSON,
		Limit:              input.Limit,
		BatchSize:          input.BatchSize,
		Trigger:            input.Trigger,
	})
	if err != nil {
		return LoadSEBulkRawRecordsActivityResult{}, err
	}
	compared, err := a.CompareSEBulkSourceFiles(ctx, CompareSEBulkSourceFilesActivityInput{
		WorkflowRunID: verified.WorkflowRunID,
		SnapshotID:    verified.SnapshotID,
		Limit:         verified.Limit,
		BatchSize:     verified.BatchSize,
		Metadata:      verified.Metadata,
		SourceFiles:   verified.SourceFiles,
	})
	if err != nil {
		return LoadSEBulkRawRecordsActivityResult{}, err
	}
	downloaded, err := a.DownloadSEBulkSourceFiles(ctx, DownloadSEBulkSourceFilesActivityInput{
		WorkflowRunID: compared.WorkflowRunID,
		SnapshotID:    compared.SnapshotID,
		Limit:         compared.Limit,
		BatchSize:     compared.BatchSize,
		Metadata:      compared.Metadata,
		SourceFiles:   compared.SourceFiles,
	})
	if err != nil {
		return LoadSEBulkRawRecordsActivityResult{}, err
	}
	return a.ProcessSEBulkRawRecords(ctx, ProcessSEBulkRawRecordsActivityInput{
		WorkflowRunID:      downloaded.WorkflowRunID,
		SnapshotID:         downloaded.SnapshotID,
		TemporalWorkflowID: input.TemporalWorkflowID,
		Limit:              downloaded.Limit,
		BatchSize:          downloaded.BatchSize,
		Metadata:           downloaded.Metadata,
		SourceFiles:        downloaded.SourceFiles,
	})
}

func (a *BulkIngestActions) VerifySEBulkSourceFiles(
	ctx context.Context,
	input VerifySEBulkSourceFilesActivityInput,
) (VerifySEBulkSourceFilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return VerifySEBulkSourceFilesActivityResult{}, errors.New("se bulk ingest gateway not available")
	}
	resolved, err := a.resolveInput(ctx, LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: input.TemporalWorkflowID,
		Datasets:           input.Datasets,
		DatasetsJSON:       input.DatasetsJSON,
		Limit:              input.Limit,
		BatchSize:          input.BatchSize,
		Trigger:            input.Trigger,
	})
	if err != nil {
		return VerifySEBulkSourceFilesActivityResult{}, err
	}

	verified := make([]VerifiedSEBulkSourceFile, 0, len(resolved.Datasets))
	for _, dataset := range resolved.Datasets {
		remote, err := verifyRemoteSourceFile(ctx, a.httpClient, dataset.URL)
		if err != nil {
			return VerifySEBulkSourceFilesActivityResult{}, err
		}
		verified = append(verified, VerifiedSEBulkSourceFile{
			Dataset:            dataset.Dataset,
			SourceURL:          dataset.URL,
			FileFormat:         dataset.Format,
			ContentType:        remote.ContentType,
			ContentLengthBytes: remote.ContentLengthBytes,
			ETag:               remote.ETag,
			LastModified:       remote.LastModified,
		})
	}

	metadata, err := metadataPayload(map[string]any{
		"source":                seBulkSource,
		"datasets":              verified,
		"record_limit_per_file": resolved.Limit,
		"database_batch_size":   resolved.BatchSize,
		"trigger":               input.Trigger,
		"temporal_workflow_id":  input.TemporalWorkflowID,
	})
	if err != nil {
		return VerifySEBulkSourceFilesActivityResult{}, err
	}

	workflowRunID, err := a.gateway.BeginWorkflowRun(ctx, sedb.BeginWorkflowRunParams{
		OrchestratorRunID: input.TemporalWorkflowID,
		RunType:           seBulkRunType,
		Metadata:          metadata,
	})
	if err != nil {
		return VerifySEBulkSourceFilesActivityResult{}, err
	}

	snapshotID, err := a.gateway.CreateBulkSnapshot(ctx, sedb.CreateBulkSnapshotParams{
		WorkflowRunID: workflowRunID,
		SnapshotKey:   time.Now().UTC().Format("2006-01-02"),
		SnapshotDate:  time.Now().UTC(),
		Metadata:      metadata,
	})
	if err != nil {
		_ = a.finishWorkflowRun(ctx, workflowRunID.String(), "failed", LoadSEBulkRawRecordsActivityResult{}, err)
		return VerifySEBulkSourceFilesActivityResult{}, err
	}

	return VerifySEBulkSourceFilesActivityResult{
		WorkflowRunID: workflowRunID.String(),
		SnapshotID:    snapshotID.String(),
		Limit:         resolved.Limit,
		BatchSize:     resolved.BatchSize,
		Metadata:      metadata,
		SourceFiles:   verified,
	}, nil
}

func (a *BulkIngestActions) CompareSEBulkSourceFiles(
	ctx context.Context,
	input CompareSEBulkSourceFilesActivityInput,
) (CompareSEBulkSourceFilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return CompareSEBulkSourceFilesActivityResult{}, errors.New("se bulk ingest gateway not available")
	}
	compared := make([]ComparedSEBulkSourceFile, 0, len(input.SourceFiles))
	for _, sourceFile := range input.SourceFiles {
		processed, ok, err := a.gateway.GetLatestParsedSourceFile(ctx, sourceFile.Dataset, sourceFile.SourceURL)
		if err != nil {
			_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", LoadSEBulkRawRecordsActivityResult{}, err)
			return CompareSEBulkSourceFilesActivityResult{}, err
		}
		item := ComparedSEBulkSourceFile{
			VerifiedSEBulkSourceFile: sourceFile,
			NeedsDownload:            true,
		}
		if ok {
			item.ExistingSourceFileID = processed.ID.String()
			item.ExistingPayloadHash = processed.PayloadHash
			if sourceFile.strongETag() != "" && sourceFile.strongETag() == metadataString(processed.Metadata, "source_etag") {
				result, err := a.recordSkippedKnownSourceFile(ctx, input.SnapshotID, sourceFile, processed, input.Metadata, "etag_match")
				if err != nil {
					_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", LoadSEBulkRawRecordsActivityResult{}, err)
					return CompareSEBulkSourceFilesActivityResult{}, err
				}
				item.NeedsDownload = false
				item.SkipReason = "etag_match"
				item.SourceFileResult = result
			}
		}
		compared = append(compared, item)
	}
	return CompareSEBulkSourceFilesActivityResult{
		WorkflowRunID: input.WorkflowRunID,
		SnapshotID:    input.SnapshotID,
		Limit:         input.Limit,
		BatchSize:     input.BatchSize,
		Metadata:      input.Metadata,
		SourceFiles:   compared,
	}, nil
}

func (a *BulkIngestActions) DownloadSEBulkSourceFiles(
	ctx context.Context,
	input DownloadSEBulkSourceFilesActivityInput,
) (DownloadSEBulkSourceFilesActivityResult, error) {
	if a == nil || a.gateway == nil {
		return DownloadSEBulkSourceFilesActivityResult{}, errors.New("se bulk ingest gateway not available")
	}
	downloaded := make([]DownloadedSEBulkSourceFile, 0, len(input.SourceFiles))
	for _, sourceFile := range input.SourceFiles {
		if !sourceFile.NeedsDownload {
			downloaded = append(downloaded, DownloadedSEBulkSourceFile{
				ComparedSEBulkSourceFile: sourceFile,
				SourceFileID:             sourceFile.SourceFileResult.SourceFileID,
				Status:                   sourceFile.SourceFileResult.Status,
			})
			continue
		}
		staged, err := downloadPayloadToDirWithProgress(ctx, a.httpClient, sourceFile.SourceURL, sourceFile.FileFormat, a.cfg.StagingRoot, func(bytesDownloaded int64) {
			activity.RecordHeartbeat(ctx, map[string]any{
				"phase":            "downloading",
				"dataset_key":      sourceFile.Dataset,
				"bytes_downloaded": bytesDownloaded,
				"source_url":       sourceFile.SourceURL,
			})
		})
		if err != nil {
			_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", LoadSEBulkRawRecordsActivityResult{}, err)
			return DownloadSEBulkSourceFilesActivityResult{}, err
		}

		sourceFile.VerifiedSEBulkSourceFile.ContentType = staged.ContentType
		sourceFile.VerifiedSEBulkSourceFile.ContentLengthBytes = staged.BytesDownloaded

		if processed, ok, err := a.gateway.GetProcessedSourceFileByHash(ctx, sourceFile.Dataset, staged.PayloadHash); err != nil {
			staged.Close()
			_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", LoadSEBulkRawRecordsActivityResult{}, err)
			return DownloadSEBulkSourceFilesActivityResult{}, err
		} else if ok {
			result, err := a.recordSkippedDuplicateSourceFile(ctx, mustUUID(input.SnapshotID), datasetConfigFromVerified(sourceFile.VerifiedSEBulkSourceFile), staged, processed, sourceFileMetadata(input.Metadata, sourceFile.VerifiedSEBulkSourceFile, nil))
			staged.Close()
			if err != nil {
				_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", LoadSEBulkRawRecordsActivityResult{}, err)
				return DownloadSEBulkSourceFilesActivityResult{}, err
			}
			sourceFile.SourceFileResult = result
			sourceFile.ExistingSourceFileID = result.SkippedSourceFileID
			downloaded = append(downloaded, DownloadedSEBulkSourceFile{
				ComparedSEBulkSourceFile: sourceFile,
				SourceFileID:             result.SourceFileID,
				PayloadHash:              staged.PayloadHash,
				BytesDownloaded:          staged.BytesDownloaded,
				Status:                   "skipped_duplicate",
			})
			continue
		}

		sourceFileID, err := a.gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
			BulkSnapshotID:     mustUUID(input.SnapshotID),
			DatasetKey:         sourceFile.Dataset,
			SourceURL:          sourceFile.SourceURL,
			FileName:           optionalString(filepath.Base(staged.Path)),
			FileFormat:         sourceFile.FileFormat,
			ContentType:        optionalString(staged.ContentType),
			ContentLengthBytes: &staged.BytesDownloaded,
			PayloadHash:        &staged.PayloadHash,
			Status:             "downloaded",
			Metadata:           sourceFileMetadata(input.Metadata, sourceFile.VerifiedSEBulkSourceFile, nil),
		})
		if err != nil {
			staged.Close()
			_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", LoadSEBulkRawRecordsActivityResult{}, err)
			return DownloadSEBulkSourceFilesActivityResult{}, err
		}
		downloaded = append(downloaded, DownloadedSEBulkSourceFile{
			ComparedSEBulkSourceFile: sourceFile,
			SourceFileID:             sourceFileID.String(),
			SourcePath:               staged.Path,
			PayloadHash:              staged.PayloadHash,
			BytesDownloaded:          staged.BytesDownloaded,
			Status:                   "downloaded",
		})
	}
	return DownloadSEBulkSourceFilesActivityResult{
		WorkflowRunID: input.WorkflowRunID,
		SnapshotID:    input.SnapshotID,
		Limit:         input.Limit,
		BatchSize:     input.BatchSize,
		Metadata:      input.Metadata,
		SourceFiles:   downloaded,
	}, nil
}

func (a *BulkIngestActions) ProcessSEBulkRawRecords(
	ctx context.Context,
	input ProcessSEBulkRawRecordsActivityInput,
) (ProcessSEBulkRawRecordsActivityResult, error) {
	if a == nil || a.gateway == nil {
		return ProcessSEBulkRawRecordsActivityResult{}, errors.New("se bulk ingest gateway not available")
	}
	result := ProcessSEBulkRawRecordsActivityResult{
		WorkflowRunID: input.WorkflowRunID,
		SnapshotID:    input.SnapshotID,
	}
	for _, sourceFile := range input.SourceFiles {
		if sourceFile.Status != "downloaded" {
			fileResult := sourceFile.SourceFileResult
			if fileResult.Dataset == "" {
				fileResult = LoadSEBulkSourceFileResult{
					Dataset:             sourceFile.Dataset,
					SourceFileID:        sourceFile.SourceFileID,
					SkippedSourceFileID: sourceFile.ExistingSourceFileID,
					SourceURL:           sourceFile.SourceURL,
					FileFormat:          sourceFile.FileFormat,
					Status:              sourceFile.Status,
				}
			}
			result.SourceFiles = append(result.SourceFiles, fileResult)
			result.Datasets = append(result.Datasets, LoadSEBulkDatasetLoadResult{Dataset: fileResult.Dataset})
			continue
		}
		fileResult, err := a.processDownloadedSourceFile(ctx, input, sourceFile)
		if err != nil {
			_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", result, err)
			return ProcessSEBulkRawRecordsActivityResult{}, err
		}
		addFileResult(&result, fileResult)
		_ = os.Remove(sourceFile.SourcePath)
	}
	if err := a.gateway.MarkBulkSnapshotParsed(ctx, sedb.MarkBulkSnapshotParsedParams{
		RecordsSeen:    result.RowsSeen,
		RecordsWritten: result.RowsWritten,
		Metadata:       input.Metadata,
		ID:             mustUUID(input.SnapshotID),
	}); err != nil {
		_ = a.finishWorkflowRun(ctx, input.WorkflowRunID, "failed", result, err)
		return ProcessSEBulkRawRecordsActivityResult{}, err
	}
	if err := a.finishWorkflowRun(ctx, input.WorkflowRunID, "succeeded", result, nil); err != nil {
		return ProcessSEBulkRawRecordsActivityResult{}, err
	}
	return result, nil
}

func (a *BulkIngestActions) finishWorkflowRun(
	ctx context.Context,
	workflowRunID string,
	status string,
	result LoadSEBulkRawRecordsActivityResult,
	cause error,
) error {
	id, err := uuid.Parse(strings.TrimSpace(workflowRunID))
	if err != nil {
		return errors.Wrap(err, "parse se workflow run id")
	}
	var message *string
	if cause != nil {
		text := cause.Error()
		message = &text
	}
	failed := result.RowsSeen - result.RowsWritten
	if failed < 0 {
		failed = 0
	}
	if err := a.gateway.FinishWorkflowRun(ctx, sedb.FinishWorkflowRunParams{
		Status:           status,
		RecordsSeen:      result.RowsSeen,
		RecordsCompleted: result.RowsWritten,
		RecordsFailed:    failed,
		Error:            message,
		ID:               id,
	}); err != nil {
		slog.ErrorContext(ctx, "finish se bulk ingest workflow run", "error", err, "workflow_run_id", workflowRunID)
		return err
	}
	return nil
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

	if processed, ok, err := a.gateway.GetProcessedSourceFileByHash(ctx, dataset.Dataset, staged.PayloadHash); err != nil {
		return LoadSEBulkSourceFileResult{}, err
	} else if ok {
		return a.recordSkippedDuplicateSourceFile(ctx, snapshotID, dataset, staged, processed, metadata)
	}

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
		SourceURL:             dataset.URL,
		FileName:              filepath.Base(staged.Path),
		FileFormat:            dataset.Format,
		Status:                "parsed",
		RowsSeen:              result.RowsSeen,
		RowsWritten:           result.RowsWritten,
		RowsInsertedNew:       result.RowsInsertedNew,
		RowsExistingUnchanged: result.RowsExistingUnchanged,
		RowsNewVersions:       result.RowsNewVersions,
	}, nil
}

func (a *BulkIngestActions) recordSkippedDuplicateSourceFile(
	ctx context.Context,
	snapshotID uuid.UUID,
	dataset HVDDatasetConfig,
	staged *stagedPayload,
	processed sedb.ProcessedSourceFile,
	metadata []byte,
) (LoadSEBulkSourceFileResult, error) {
	sourceFileID, err := a.gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID:     snapshotID,
		DatasetKey:         dataset.Dataset,
		SourceURL:          dataset.URL,
		FileName:           optionalString(filepath.Base(staged.Path)),
		FileFormat:         dataset.Format,
		ContentType:        optionalString(staged.ContentType),
		ContentLengthBytes: &staged.BytesDownloaded,
		PayloadHash:        &staged.PayloadHash,
		Status:             "skipped_duplicate",
		Metadata: metadataPayloadOrEmpty(map[string]any{
			"duplicate_of_source_file_id": processed.ID.String(),
			"duplicate_rows_seen":         processed.RowsSeen,
			"duplicate_rows_written":      processed.RowsWritten,
		}, metadata),
	})
	if err != nil {
		return LoadSEBulkSourceFileResult{}, err
	}
	activity.RecordHeartbeat(ctx, map[string]any{
		"phase":                      "skipped_duplicate",
		"dataset_key":                dataset.Dataset,
		"payload_hash":               staged.PayloadHash,
		"source_file_id":             sourceFileID.String(),
		"duplicate_source_file_id":   processed.ID.String(),
		"duplicate_source_rows_seen": processed.RowsSeen,
	})
	return LoadSEBulkSourceFileResult{
		Dataset:             dataset.Dataset,
		SourceFileID:        sourceFileID.String(),
		SkippedSourceFileID: processed.ID.String(),
		SourceURL:           dataset.URL,
		FileName:            filepath.Base(staged.Path),
		FileFormat:          dataset.Format,
		Status:              "skipped_duplicate",
	}, nil
}

func (a *BulkIngestActions) recordSkippedKnownSourceFile(
	ctx context.Context,
	snapshotID string,
	sourceFile VerifiedSEBulkSourceFile,
	processed sedb.ProcessedSourceFile,
	metadata []byte,
	reason string,
) (LoadSEBulkSourceFileResult, error) {
	payloadHash := processed.PayloadHash
	sourceFileID, err := a.gateway.RecordSourceFile(ctx, sedb.RecordSourceFileParams{
		BulkSnapshotID:     mustUUID(snapshotID),
		DatasetKey:         sourceFile.Dataset,
		SourceURL:          sourceFile.SourceURL,
		FileName:           optionalString(filepath.Base(sourceFile.SourceURL)),
		FileFormat:         sourceFile.FileFormat,
		ContentType:        optionalString(sourceFile.ContentType),
		ContentLengthBytes: optionalInt64(sourceFile.ContentLengthBytes),
		PayloadHash:        optionalString(payloadHash),
		Status:             "skipped_duplicate",
		Metadata: sourceFileMetadata(metadata, sourceFile, map[string]any{
			"duplicate_of_source_file_id": processed.ID.String(),
			"duplicate_rows_seen":         processed.RowsSeen,
			"duplicate_rows_written":      processed.RowsWritten,
			"skip_reason":                 reason,
		}),
	})
	if err != nil {
		return LoadSEBulkSourceFileResult{}, err
	}
	return LoadSEBulkSourceFileResult{
		Dataset:             sourceFile.Dataset,
		SourceFileID:        sourceFileID.String(),
		SkippedSourceFileID: processed.ID.String(),
		SourceURL:           sourceFile.SourceURL,
		FileName:            filepath.Base(sourceFile.SourceURL),
		FileFormat:          sourceFile.FileFormat,
		Status:              "skipped_duplicate",
	}, nil
}

func (a *BulkIngestActions) processDownloadedSourceFile(
	ctx context.Context,
	input ProcessSEBulkRawRecordsActivityInput,
	sourceFile DownloadedSEBulkSourceFile,
) (LoadSEBulkSourceFileResult, error) {
	sourceFileID, err := uuid.Parse(sourceFile.SourceFileID)
	if err != nil {
		return LoadSEBulkSourceFileResult{}, errors.Wrap(err, "parse se source file id")
	}
	dataset := datasetConfigFromVerified(sourceFile.VerifiedSEBulkSourceFile)
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
			"dataset_key":                    sourceFile.Dataset,
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

	streamed, err := sebulk.StreamRecordsFile(ctx, sourceFile.SourcePath, sourceFile.FileFormat, input.Limit, func(record sebulk.Record) error {
		batch = append(batch, rawRecordFromBulk(record, sourceFileID, input.TemporalWorkflowID, input.Metadata))
		if int32(len(batch)) >= input.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		_ = a.recordFailedSourceFile(ctx, mustUUID(input.SnapshotID), dataset, stagedPayloadFromDownload(sourceFile), err, input.Metadata)
		return LoadSEBulkSourceFileResult{}, err
	}
	if err := flush(); err != nil {
		_ = a.recordFailedSourceFile(ctx, mustUUID(input.SnapshotID), dataset, stagedPayloadFromDownload(sourceFile), err, input.Metadata)
		return LoadSEBulkSourceFileResult{}, err
	}
	if streamed.RowsSeen != result.RowsSeen {
		err := errors.New("se bulk parser row count mismatch")
		_ = a.recordFailedSourceFile(ctx, mustUUID(input.SnapshotID), dataset, stagedPayloadFromDownload(sourceFile), err, input.Metadata)
		return LoadSEBulkSourceFileResult{}, err
	}
	if _, err := a.recordParsedSourceFile(ctx, mustUUID(input.SnapshotID), dataset, stagedPayloadFromDownload(sourceFile), result, sourceFileMetadata(input.Metadata, sourceFile.VerifiedSEBulkSourceFile, nil)); err != nil {
		return LoadSEBulkSourceFileResult{}, err
	}
	return LoadSEBulkSourceFileResult{
		Dataset:               sourceFile.Dataset,
		SourceFileID:          sourceFile.SourceFileID,
		SourceURL:             sourceFile.SourceURL,
		FileName:              filepath.Base(sourceFile.SourcePath),
		FileFormat:            sourceFile.FileFormat,
		Status:                "parsed",
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

func (a *BulkIngestActions) resolveInput(ctx context.Context, input LoadSEBulkRawRecordsActivityInput) (resolvedBulkInput, error) {
	batchSize := input.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBulkIngestDBBatchSize
	}
	datasets := input.Datasets
	if len(datasets) == 0 {
		if rawConfig := strings.TrimSpace(input.DatasetsJSON); rawConfig != "" {
			parsed, err := parseDatasetsJSON(rawConfig)
			if err != nil {
				return resolvedBulkInput{}, err
			}
			datasets = parsed
		}
	}
	if len(datasets) == 0 {
		configured, err := a.datasetsFromDataSourceConfig(ctx)
		if err != nil {
			return resolvedBulkInput{}, err
		}
		datasets = configured
	}
	if len(datasets) == 0 {
		if rawConfig := strings.TrimSpace(a.cfg.DatasetsJSON); rawConfig != "" {
			parsed, err := parseDatasetsJSON(rawConfig)
			if err != nil {
				return resolvedBulkInput{}, err
			}
			datasets = parsed
		}
	}
	if len(datasets) == 0 {
		return resolvedBulkInput{}, errors.New("Sweden HVD source config requires config.datasets entries with dataset, url, and format")
	}
	resolvedDatasets, err := a.resolveDatasetDownloads(ctx, datasets)
	if err != nil {
		return resolvedBulkInput{}, err
	}
	return resolvedBulkInput{
		Datasets:  resolvedDatasets,
		Limit:     input.Limit,
		BatchSize: batchSize,
	}, nil
}

func parseDatasetsJSON(rawConfig string) ([]HVDDatasetConfig, error) {
	rawConfig = strings.TrimSpace(rawConfig)
	if rawConfig == "" {
		return nil, errors.New("Sweden HVD dataset JSON is empty")
	}
	var datasets []HVDDatasetConfig
	if err := json.Unmarshal([]byte(rawConfig), &datasets); err != nil {
		return nil, errors.Wrap(err, "parse Sweden HVD dataset JSON")
	}
	return validateHVDDatasets(datasets, "Sweden HVD dataset JSON")
}

func (a *BulkIngestActions) datasetsFromDataSourceConfig(ctx context.Context) ([]HVDDatasetConfig, error) {
	config, ok, err := a.gateway.GetDataSourceConfig(ctx, seDataSourceName)
	if err != nil {
		return nil, err
	}
	if !ok || len(config) == 0 {
		return nil, nil
	}
	return parseDatasetsFromDataSourceConfig(config)
}

func parseDatasetsFromDataSourceConfig(config []byte) ([]HVDDatasetConfig, error) {
	var sourceConfig seDataSourceConfig
	if err := json.Unmarshal(config, &sourceConfig); err != nil {
		return nil, errors.Wrap(err, "decode Sweden HVD source config")
	}
	if strings.TrimSpace(sourceConfig.DatasetsJSON) != "" {
		return parseDatasetsJSON(sourceConfig.DatasetsJSON)
	}

	configuredDatasets := sourceConfig.BulkDatasets
	if len(configuredDatasets) == 0 {
		configuredDatasets = sourceConfig.Datasets
	}
	if len(configuredDatasets) == 0 {
		return nil, nil
	}

	datasets := make([]HVDDatasetConfig, 0, len(configuredDatasets))
	for _, configured := range configuredDatasets {
		datasets = append(datasets, HVDDatasetConfig{
			Dataset: firstNonEmpty(configured.Dataset, configured.Key),
			URL:     firstNonEmpty(configured.URL, configured.StableURL, configured.SourceURL, configured.DownloadURL),
			Format:  firstNonEmpty(configured.Format, configured.FileFormat),
		})
	}
	return validateHVDDatasets(datasets, "Sweden HVD source config datasets")
}

func validateHVDDatasets(datasets []HVDDatasetConfig, label string) ([]HVDDatasetConfig, error) {
	for i := range datasets {
		datasets[i] = datasets[i].trimmed()
		if datasets[i].Dataset == "" || datasets[i].URL == "" || datasets[i].Format == "" {
			return nil, errors.Newf("%s entries require dataset, url, and format", label)
		}
	}
	return datasets, nil
}

func (a *BulkIngestActions) resolveDatasetDownloads(ctx context.Context, datasets []HVDDatasetConfig) ([]HVDDatasetConfig, error) {
	resolved := make([]HVDDatasetConfig, 0, len(datasets))
	for _, dataset := range datasets {
		download, err := a.resolveDatasetDownload(ctx, dataset)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, download)
	}
	return resolved, nil
}

func (a *BulkIngestActions) resolveDatasetDownload(ctx context.Context, dataset HVDDatasetConfig) (HVDDatasetConfig, error) {
	dataset = dataset.trimmed()
	if !isMetadataFormat(dataset.Format) {
		return dataset, nil
	}

	metadataBody, err := fetchDiscoveryText(ctx, a.httpClient, dataset.URL, "application/rdf+xml, application/xml, application/json, text/xml, */*")
	if err != nil {
		return HVDDatasetConfig{}, errors.Wrapf(err, "resolve Sweden HVD metadata URL %s", dataset.URL)
	}
	accessURL := extractMetadataAccessURL(metadataBody)
	if accessURL == "" {
		return HVDDatasetConfig{}, errors.Newf("Sweden HVD metadata URL %s did not expose a dcat access URL", dataset.URL)
	}
	distributionFormat := extractMetadataFormat(metadataBody)
	if looksLikeDownloadURL(accessURL) {
		dataset.URL = accessURL
		dataset.Format = inferDatasetFormat(accessURL, distributionFormat)
		return dataset, nil
	}

	landingBody, err := fetchDiscoveryText(ctx, a.httpClient, accessURL, "text/html, application/xhtml+xml, */*")
	if err != nil {
		return HVDDatasetConfig{}, errors.Wrapf(err, "resolve Sweden HVD landing page %s", accessURL)
	}
	downloadURL := selectDownloadURL(extractDownloadLinks(landingBody, accessURL))
	if downloadURL == "" {
		return HVDDatasetConfig{}, errors.Newf("Sweden HVD landing page %s did not contain a zip/json download link; update se source config datasets.url to the direct HVD file URL", accessURL)
	}
	dataset.URL = downloadURL
	dataset.Format = inferDatasetFormat(downloadURL, distributionFormat)
	return dataset, nil
}

func fetchDiscoveryText(ctx context.Context, httpClient *http.Client, sourceURL string, accept string) (string, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return "", errors.Wrap(err, "create Sweden HVD discovery request")
	}
	request.Header.Set("Accept", accept)
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")

	response, err := httpClient.Do(request)
	if err != nil {
		return "", errors.Wrap(err, "fetch Sweden HVD discovery document")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", errors.Newf("fetch Sweden HVD discovery document returned status %d", response.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, maxDiscoveryBodyBytes+1))
	if err != nil {
		return "", errors.Wrap(err, "read Sweden HVD discovery document")
	}
	if int64(len(body)) > maxDiscoveryBodyBytes {
		return "", errors.New("Sweden HVD discovery document is too large")
	}
	return string(body), nil
}

type remoteSourceFileMetadata struct {
	ContentType        string
	ContentLengthBytes int64
	ETag               string
	LastModified       string
}

func verifyRemoteSourceFile(ctx context.Context, httpClient *http.Client, sourceURL string) (remoteSourceFileMetadata, error) {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, sourceURL, nil)
	if err != nil {
		return remoteSourceFileMetadata{}, errors.Wrap(err, "create se hvd verify request")
	}
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	response, err := httpClient.Do(request)
	if err == nil && response != nil && response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices {
		if response.Body != nil {
			_ = response.Body.Close()
		}
		return remoteMetadataFromResponse(response), nil
	}
	if err == nil && response != nil && response.Body != nil {
		_ = response.Body.Close()
	}

	request, err = http.NewRequestWithContext(ctx, http.MethodGet, sourceURL, nil)
	if err != nil {
		return remoteSourceFileMetadata{}, errors.Wrap(err, "create se hvd verify fallback request")
	}
	request.Header.Set("Range", "bytes=0-0")
	request.Header.Set("User-Agent", "corpscout-scheduler/1.0")
	response, err = httpClient.Do(request)
	if err != nil {
		return remoteSourceFileMetadata{}, errors.Wrap(err, "verify se hvd source file")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return remoteSourceFileMetadata{}, errors.Newf("verify se hvd source file returned status %d", response.StatusCode)
	}
	return remoteMetadataFromResponse(response), nil
}

func remoteMetadataFromResponse(response *http.Response) remoteSourceFileMetadata {
	var length int64
	if response != nil {
		length = response.ContentLength
	}
	if length < 0 {
		length = 0
	}
	return remoteSourceFileMetadata{
		ContentType:        response.Header.Get("Content-Type"),
		ContentLengthBytes: length,
		ETag:               response.Header.Get("ETag"),
		LastModified:       response.Header.Get("Last-Modified"),
	}
}

func extractMetadataAccessURL(body string) string {
	if match := metadataAccessURLPattern.FindStringSubmatch(body); len(match) == 2 {
		return strings.TrimSpace(html.UnescapeString(match[1]))
	}
	if match := jsonAccessURLPattern.FindStringSubmatch(body); len(match) == 2 {
		return strings.TrimSpace(html.UnescapeString(match[1]))
	}
	return ""
}

func extractMetadataFormat(body string) string {
	if match := metadataFormatPattern.FindStringSubmatch(body); len(match) == 2 {
		return strings.TrimSpace(html.UnescapeString(match[1]))
	}
	return ""
}

func extractDownloadLinks(body string, baseURL string) []string {
	matches := htmlHrefPattern.FindAllStringSubmatch(body, -1)
	if len(matches) == 0 {
		return nil
	}
	links := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) != 2 {
			continue
		}
		resolved := resolveLinkURL(baseURL, match[1])
		if resolved == "" || !looksLikeDownloadURL(resolved) {
			continue
		}
		links = append(links, resolved)
	}
	return links
}

func selectDownloadURL(links []string) string {
	for _, link := range links {
		if containsAnyFold(link, "organisation", "organisationer", "organization", "foretag", "företag", "grundlaggande", "grundläggande") {
			return link
		}
	}
	if len(links) > 0 {
		return links[0]
	}
	return ""
}

func resolveLinkURL(baseURL string, rawHref string) string {
	base, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	href, err := url.Parse(strings.TrimSpace(html.UnescapeString(rawHref)))
	if err != nil {
		return ""
	}
	return base.ResolveReference(href).String()
}

func looksLikeDownloadURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	path := strings.ToLower(parsed.Path)
	return strings.HasSuffix(path, ".zip") ||
		strings.HasSuffix(path, ".json") ||
		strings.HasSuffix(path, ".json.gz") ||
		strings.HasSuffix(path, ".gz")
}

func inferDatasetFormat(rawURL string, fallback string) string {
	parsed, err := url.Parse(rawURL)
	if err == nil {
		path := strings.ToLower(parsed.Path)
		switch {
		case strings.HasSuffix(path, ".zip"):
			return "zip"
		case strings.HasSuffix(path, ".json.gz"), strings.HasSuffix(path, ".gz"):
			return "gzip"
		case strings.HasSuffix(path, ".json"):
			return "json"
		}
	}
	normalizedFallback := strings.ToLower(strings.TrimSpace(fallback))
	switch {
	case strings.Contains(normalizedFallback, "zip"):
		return "zip"
	case strings.Contains(normalizedFallback, "gzip"), strings.Contains(normalizedFallback, "gz"):
		return "gzip"
	case strings.Contains(normalizedFallback, "json"):
		return "json"
	case normalizedFallback != "" && normalizedFallback != "metadata":
		return normalizedFallback
	default:
		return "zip"
	}
}

func isMetadataFormat(format string) bool {
	normalized := strings.ToLower(strings.TrimSpace(format))
	return normalized == "metadata" ||
		normalized == "rdf" ||
		normalized == "rdf+xml" ||
		normalized == "application/rdf+xml"
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
	return downloadPayloadToDirWithProgress(ctx, httpClient, sourceURL, format, "", onProgress)
}

func downloadPayloadToDirWithProgress(ctx context.Context, httpClient *http.Client, sourceURL string, format string, stagingDir string, onProgress func(int64)) (*stagedPayload, error) {
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

	stagingDir = strings.TrimSpace(stagingDir)
	if stagingDir != "" {
		if err := os.MkdirAll(stagingDir, 0o750); err != nil {
			return nil, errors.Wrap(err, "create se hvd staging directory")
		}
	}
	tempFile, err := os.CreateTemp(stagingDir, "corpscout-se-hvd-*."+safeFormatSuffix(format))
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

func metadataPayloadOrEmpty(extra map[string]any, fallback []byte) []byte {
	var base map[string]any
	if len(fallback) > 0 && json.Valid(fallback) {
		_ = json.Unmarshal(fallback, &base)
	}
	if base == nil {
		base = make(map[string]any)
	}
	for key, value := range extra {
		base[key] = value
	}
	payload, err := json.Marshal(base)
	if err != nil {
		return fallback
	}
	return payload
}

func sourceFileMetadata(base []byte, sourceFile VerifiedSEBulkSourceFile, extra map[string]any) []byte {
	values := map[string]any{
		"source_etag":                 sourceFile.strongETag(),
		"source_last_modified":        strings.TrimSpace(sourceFile.LastModified),
		"source_content_type":         strings.TrimSpace(sourceFile.ContentType),
		"source_content_length_bytes": sourceFile.ContentLengthBytes,
	}
	for key, value := range extra {
		values[key] = value
	}
	return metadataPayloadOrEmpty(values, base)
}

func metadataString(raw []byte, key string) string {
	if len(raw) == 0 || !json.Valid(raw) {
		return ""
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return ""
	}
	value, ok := decoded[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func (f VerifiedSEBulkSourceFile) strongETag() string {
	value := strings.TrimSpace(f.ETag)
	if value == "" || strings.HasPrefix(strings.ToLower(value), "w/") {
		return ""
	}
	return value
}

func addIngestResult(target *sedb.IngestRawRecordsResult, result sedb.IngestRawRecordsResult) {
	target.RowsSeen += result.RowsSeen
	target.RowsWritten += result.RowsWritten
	target.RowsInsertedNew += result.RowsInsertedNew
	target.RowsExistingUnchanged += result.RowsExistingUnchanged
	target.RowsNewVersions += result.RowsNewVersions
	target.RawRecordIDs = append(target.RawRecordIDs, result.RawRecordIDs...)
}

func addFileResult(target *LoadSEBulkRawRecordsActivityResult, fileResult LoadSEBulkSourceFileResult) {
	target.RowsSeen += fileResult.RowsSeen
	target.RowsWritten += fileResult.RowsWritten
	target.RowsInsertedNew += fileResult.RowsInsertedNew
	target.RowsExistingUnchanged += fileResult.RowsExistingUnchanged
	target.RowsNewVersions += fileResult.RowsNewVersions
	target.SourceFiles = append(target.SourceFiles, fileResult)
	target.Datasets = append(target.Datasets, LoadSEBulkDatasetLoadResult{
		Dataset:     fileResult.Dataset,
		RowsSeen:    fileResult.RowsSeen,
		RowsWritten: fileResult.RowsWritten,
	})
}

func (cfg BulkIngestConfig) trimmed() BulkIngestConfig {
	stagingRoot := strings.TrimSpace(cfg.StagingRoot)
	if stagingRoot == "" {
		stagingRoot = defaultSEBulkStagingRoot
	}
	return BulkIngestConfig{
		DatasetsJSON: strings.TrimSpace(cfg.DatasetsJSON),
		StagingRoot:  stagingRoot,
	}
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

func containsAnyFold(value string, needles ...string) bool {
	normalized := strings.ToLower(value)
	for _, needle := range needles {
		if strings.Contains(normalized, strings.ToLower(needle)) {
			return true
		}
	}
	return false
}

func datasetConfigFromVerified(sourceFile VerifiedSEBulkSourceFile) HVDDatasetConfig {
	return HVDDatasetConfig{
		Dataset: sourceFile.Dataset,
		URL:     sourceFile.SourceURL,
		Format:  sourceFile.FileFormat,
	}
}

func stagedPayloadFromDownload(sourceFile DownloadedSEBulkSourceFile) *stagedPayload {
	return &stagedPayload{
		Path:            sourceFile.SourcePath,
		PayloadHash:     sourceFile.PayloadHash,
		ContentType:     sourceFile.ContentType,
		BytesDownloaded: sourceFile.BytesDownloaded,
		ResolvedURL:     sourceFile.SourceURL,
	}
}

func mustUUID(value string) uuid.UUID {
	id, err := uuid.Parse(strings.TrimSpace(value))
	if err != nil {
		return uuid.Nil
	}
	return id
}

func optionalInt64(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	return &value
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

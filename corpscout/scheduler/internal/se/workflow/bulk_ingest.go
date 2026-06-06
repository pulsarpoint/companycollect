package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/se/actions"
)

const (
	LoadSEBulkRawRecordsTaskQueue    = "se-bulk-ingest"
	LoadSEBulkRawRecordsWorkflowName = "LoadSEBulkRawRecords"

	verifySEBulkSourceFilesActivity   = "VerifySEBulkSourceFilesActivity"
	compareSEBulkSourceFilesActivity  = "CompareSEBulkSourceFilesActivity"
	downloadSEBulkSourceFilesActivity = "DownloadSEBulkSourceFilesActivity"
	processSEBulkRawRecordsActivity   = "ProcessSEBulkRawRecordsActivity"

	seBulkActivityStartToCloseTimeout = 8 * time.Hour
	seBulkActivityHeartbeatTimeout    = 10 * time.Minute
)

type HVDDatasetConfig = actions.HVDDatasetConfig
type LoadSEBulkRawRecordsActivityInput = actions.LoadSEBulkRawRecordsActivityInput
type LoadSEBulkRawRecordsActivityResult = actions.LoadSEBulkRawRecordsActivityResult
type LoadSEBulkSourceFileResult = actions.LoadSEBulkSourceFileResult
type LoadSEBulkDatasetLoadResult = actions.LoadSEBulkDatasetLoadResult
type SEBulkProcessingIssue = actions.SEBulkProcessingIssue
type VerifySEBulkSourceFilesActivityInput = actions.VerifySEBulkSourceFilesActivityInput
type VerifySEBulkSourceFilesActivityResult = actions.VerifySEBulkSourceFilesActivityResult
type VerifiedSEBulkSourceFile = actions.VerifiedSEBulkSourceFile
type CompareSEBulkSourceFilesActivityInput = actions.CompareSEBulkSourceFilesActivityInput
type CompareSEBulkSourceFilesActivityResult = actions.CompareSEBulkSourceFilesActivityResult
type ComparedSEBulkSourceFile = actions.ComparedSEBulkSourceFile
type DownloadSEBulkSourceFilesActivityInput = actions.DownloadSEBulkSourceFilesActivityInput
type DownloadSEBulkSourceFilesActivityResult = actions.DownloadSEBulkSourceFilesActivityResult
type DownloadedSEBulkSourceFile = actions.DownloadedSEBulkSourceFile
type ProcessSEBulkRawRecordsActivityInput = actions.ProcessSEBulkRawRecordsActivityInput
type ProcessSEBulkRawRecordsActivityResult = actions.ProcessSEBulkRawRecordsActivityResult

type LoadSEBulkRawRecordsInput struct {
	Datasets     []HVDDatasetConfig `json:"datasets,omitempty"`
	DatasetsJSON string             `json:"datasets_json,omitempty"`
	Limit        int                `json:"limit,omitempty"`
	BatchSize    int                `json:"batch_size,omitempty"`
	Trigger      string             `json:"trigger,omitempty"`
}

type LoadSEBulkRawRecordsResult struct {
	Status                string                        `json:"status"`
	RowsSeen              int32                         `json:"rows_seen"`
	RowsWritten           int32                         `json:"rows_written"`
	RowsInsertedNew       int32                         `json:"rows_inserted_new"`
	RowsExistingUnchanged int32                         `json:"rows_existing_unchanged"`
	RowsNewVersions       int32                         `json:"rows_new_versions"`
	WorkflowRunID         string                        `json:"workflow_run_id,omitempty"`
	SnapshotID            string                        `json:"snapshot_id,omitempty"`
	SourceFiles           []LoadSEBulkSourceFileResult  `json:"source_files,omitempty"`
	Datasets              []LoadSEBulkDatasetLoadResult `json:"datasets,omitempty"`
	ProcessingIssues      []SEBulkProcessingIssue       `json:"processing_issues,omitempty"`
}

func LoadSEBulkRawRecords(
	ctx temporalworkflow.Context,
	input LoadSEBulkRawRecordsInput,
) (LoadSEBulkRawRecordsResult, error) {
	input = normalizeLoadSEBulkRawRecordsInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: seBulkActivityStartToCloseTimeout,
		HeartbeatTimeout:    seBulkActivityHeartbeatTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    30 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("se bulk raw ingest workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"datasets_count", len(input.Datasets),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"trigger", input.Trigger,
	)

	var verified VerifySEBulkSourceFilesActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, verifySEBulkSourceFilesActivity, VerifySEBulkSourceFilesActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		Datasets:           input.Datasets,
		DatasetsJSON:       input.DatasetsJSON,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		Trigger:            input.Trigger,
	}).Get(ctx, &verified); err != nil {
		return LoadSEBulkRawRecordsResult{}, errors.Wrap(err, "verify se bulk source files")
	}

	var compared CompareSEBulkSourceFilesActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, compareSEBulkSourceFilesActivity, CompareSEBulkSourceFilesActivityInput{
		WorkflowRunID: verified.WorkflowRunID,
		SnapshotID:    verified.SnapshotID,
		Limit:         verified.Limit,
		BatchSize:     verified.BatchSize,
		Metadata:      verified.Metadata,
		SourceFiles:   verified.SourceFiles,
	}).Get(ctx, &compared); err != nil {
		return LoadSEBulkRawRecordsResult{}, errors.Wrap(err, "compare se bulk source file hashes")
	}

	var downloaded DownloadSEBulkSourceFilesActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, downloadSEBulkSourceFilesActivity, DownloadSEBulkSourceFilesActivityInput{
		WorkflowRunID: compared.WorkflowRunID,
		SnapshotID:    compared.SnapshotID,
		Limit:         compared.Limit,
		BatchSize:     compared.BatchSize,
		Metadata:      compared.Metadata,
		SourceFiles:   compared.SourceFiles,
	}).Get(ctx, &downloaded); err != nil {
		return LoadSEBulkRawRecordsResult{}, errors.Wrap(err, "download se bulk source files")
	}

	var activityResult ProcessSEBulkRawRecordsActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, processSEBulkRawRecordsActivity, ProcessSEBulkRawRecordsActivityInput{
		WorkflowRunID:      downloaded.WorkflowRunID,
		SnapshotID:         downloaded.SnapshotID,
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		Limit:              downloaded.Limit,
		BatchSize:          downloaded.BatchSize,
		Metadata:           downloaded.Metadata,
		SourceFiles:        downloaded.SourceFiles,
	}).Get(ctx, &activityResult); err != nil {
		return LoadSEBulkRawRecordsResult{}, errors.Wrap(err, "process se bulk raw records")
	}
	logger.Debug("se bulk raw ingest workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"rows_seen", activityResult.RowsSeen,
		"rows_written", activityResult.RowsWritten,
	)

	return LoadSEBulkRawRecordsResult{
		Status:                "succeeded",
		RowsSeen:              activityResult.RowsSeen,
		RowsWritten:           activityResult.RowsWritten,
		RowsInsertedNew:       activityResult.RowsInsertedNew,
		RowsExistingUnchanged: activityResult.RowsExistingUnchanged,
		RowsNewVersions:       activityResult.RowsNewVersions,
		WorkflowRunID:         activityResult.WorkflowRunID,
		SnapshotID:            activityResult.SnapshotID,
		SourceFiles:           activityResult.SourceFiles,
		Datasets:              activityResult.Datasets,
		ProcessingIssues:      activityResult.ProcessingIssues,
	}, nil
}

func normalizeLoadSEBulkRawRecordsInput(input LoadSEBulkRawRecordsInput) LoadSEBulkRawRecordsInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

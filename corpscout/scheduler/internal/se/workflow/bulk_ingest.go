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

	loadSEBulkRawRecordsActivity = "LoadSEBulkRawRecordsActivity"
)

type HVDDatasetConfig = actions.HVDDatasetConfig
type LoadSEBulkRawRecordsActivityInput = actions.LoadSEBulkRawRecordsActivityInput
type LoadSEBulkRawRecordsActivityResult = actions.LoadSEBulkRawRecordsActivityResult
type LoadSEBulkSourceFileResult = actions.LoadSEBulkSourceFileResult
type LoadSEBulkDatasetLoadResult = actions.LoadSEBulkDatasetLoadResult

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
}

func LoadSEBulkRawRecords(
	ctx temporalworkflow.Context,
	input LoadSEBulkRawRecordsInput,
) (LoadSEBulkRawRecordsResult, error) {
	input = normalizeLoadSEBulkRawRecordsInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 8 * time.Hour,
		HeartbeatTimeout:    2 * time.Minute,
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

	var activityResult LoadSEBulkRawRecordsActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, loadSEBulkRawRecordsActivity, LoadSEBulkRawRecordsActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		Datasets:           input.Datasets,
		DatasetsJSON:       input.DatasetsJSON,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return LoadSEBulkRawRecordsResult{}, errors.Wrap(err, "load se bulk raw records")
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
	}, nil
}

func normalizeLoadSEBulkRawRecordsInput(input LoadSEBulkRawRecordsInput) LoadSEBulkRawRecordsInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	LoadBrregBulkRawRecordsTaskQueue    = "brreg-bulk-ingest"
	LoadBrregBulkRawRecordsWorkflowName = "LoadBrregBulkRawRecords"

	loadBrregBulkRawRecordsActivity = "LoadBrregBulkRawRecordsActivity"
)

type LoadBrregBulkRawRecordsActivityInput = actions.LoadBrregBulkRawRecordsActivityInput
type LoadBrregBulkRawRecordsActivityResult = actions.LoadBrregBulkRawRecordsActivityResult

type LoadBrregBulkRawRecordsInput struct {
	SourceURL string `json:"source_url,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
}

type LoadBrregBulkRawRecordsResult struct {
	Status                string `json:"status"`
	RowsSeen              int32  `json:"rows_seen"`
	RowsWritten           int32  `json:"rows_written"`
	RowsInsertedNew       int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged int32  `json:"rows_existing_unchanged"`
	RowsNewVersions       int32  `json:"rows_new_versions"`
}

func LoadBrregBulkRawRecords(
	ctx temporalworkflow.Context,
	input LoadBrregBulkRawRecordsInput,
) (LoadBrregBulkRawRecordsResult, error) {
	input = normalizeLoadBrregBulkRawRecordsInput(input)
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
	logger.Debug("brreg bulk raw ingest workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"source_url", input.SourceURL,
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"trigger", input.Trigger,
	)

	var activityResult LoadBrregBulkRawRecordsActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, loadBrregBulkRawRecordsActivity, LoadBrregBulkRawRecordsActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		SourceURL:          input.SourceURL,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return LoadBrregBulkRawRecordsResult{}, errors.Wrap(err, "load brreg bulk raw records")
	}
	logger.Debug("brreg bulk raw ingest workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"rows_seen", activityResult.RowsSeen,
		"rows_written", activityResult.RowsWritten,
		"rows_inserted_new", activityResult.RowsInsertedNew,
		"rows_existing_unchanged", activityResult.RowsExistingUnchanged,
		"rows_new_versions", activityResult.RowsNewVersions,
	)

	return LoadBrregBulkRawRecordsResult{
		Status:                "succeeded",
		RowsSeen:              activityResult.RowsSeen,
		RowsWritten:           activityResult.RowsWritten,
		RowsInsertedNew:       activityResult.RowsInsertedNew,
		RowsExistingUnchanged: activityResult.RowsExistingUnchanged,
		RowsNewVersions:       activityResult.RowsNewVersions,
	}, nil
}

func normalizeLoadBrregBulkRawRecordsInput(input LoadBrregBulkRawRecordsInput) LoadBrregBulkRawRecordsInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

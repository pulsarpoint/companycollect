package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/cvr/actions"
)

const (
	LoadCVRRawRecordsTaskQueue    = "cvr-raw-ingest"
	LoadCVRRawRecordsWorkflowName = "LoadCVRRawRecords"

	loadCVRRawRecordsActivity = "LoadCVRRawRecordsActivity"
)

type LoadCVRRawRecordsActivityInput = actions.LoadCVRRawRecordsActivityInput
type LoadCVRRawRecordsActivityResult = actions.LoadCVRRawRecordsActivityResult

type LoadCVRRawRecordsInput struct {
	SourceURL string `json:"source_url,omitempty"`
	ScrollURL string `json:"scroll_url,omitempty"`
	Scroll    string `json:"scroll,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	PageSize  int    `json:"page_size,omitempty"`
	BatchSize int    `json:"batch_size,omitempty"`
	Trigger   string `json:"trigger,omitempty"`
}

type LoadCVRRawRecordsResult struct {
	Status                string `json:"status"`
	RowsSeen              int32  `json:"rows_seen"`
	RowsWritten           int32  `json:"rows_written"`
	RowsInsertedNew       int32  `json:"rows_inserted_new"`
	RowsExistingUnchanged int32  `json:"rows_existing_unchanged"`
	RowsNewVersions       int32  `json:"rows_new_versions"`
	WorkflowRunID         string `json:"workflow_run_id,omitempty"`
	ScrollSessionID       string `json:"scroll_session_id,omitempty"`
}

func LoadCVRRawRecords(
	ctx temporalworkflow.Context,
	input LoadCVRRawRecordsInput,
) (LoadCVRRawRecordsResult, error) {
	input = normalizeLoadCVRRawRecordsInput(input)
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
	logger.Debug("cvr raw ingest workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"source_url", input.SourceURL,
		"scroll_url", input.ScrollURL,
		"scroll", input.Scroll,
		"limit", input.Limit,
		"page_size", input.PageSize,
		"batch_size", input.BatchSize,
		"trigger", input.Trigger,
	)

	var activityResult LoadCVRRawRecordsActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, loadCVRRawRecordsActivity, LoadCVRRawRecordsActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		SourceURL:          input.SourceURL,
		ScrollURL:          input.ScrollURL,
		Scroll:             input.Scroll,
		Limit:              int32(input.Limit),
		PageSize:           int32(input.PageSize),
		BatchSize:          int32(input.BatchSize),
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return LoadCVRRawRecordsResult{}, errors.Wrap(err, "load cvr raw records")
	}
	logger.Debug("cvr raw ingest workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"rows_seen", activityResult.RowsSeen,
		"rows_written", activityResult.RowsWritten,
		"rows_inserted_new", activityResult.RowsInsertedNew,
		"rows_existing_unchanged", activityResult.RowsExistingUnchanged,
		"rows_new_versions", activityResult.RowsNewVersions,
	)

	return LoadCVRRawRecordsResult{
		Status:                "succeeded",
		RowsSeen:              activityResult.RowsSeen,
		RowsWritten:           activityResult.RowsWritten,
		RowsInsertedNew:       activityResult.RowsInsertedNew,
		RowsExistingUnchanged: activityResult.RowsExistingUnchanged,
		RowsNewVersions:       activityResult.RowsNewVersions,
		WorkflowRunID:         activityResult.WorkflowRunID,
		ScrollSessionID:       activityResult.ScrollSessionID,
	}, nil
}

func normalizeLoadCVRRawRecordsInput(input LoadCVRRawRecordsInput) LoadCVRRawRecordsInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

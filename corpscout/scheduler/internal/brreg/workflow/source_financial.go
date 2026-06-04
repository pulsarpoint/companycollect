package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	FetchBrregSourceFinancialStatementsTaskQueue    = "brreg-source-financial"
	FetchBrregSourceFinancialStatementsWorkflowName = "FetchBrregSourceFinancialStatements"

	fetchBrregSourceFinancialStatementsActivity = "FetchBrregSourceFinancialStatementsActivity"
)

type FetchBrregSourceFinancialStatementsActivityInput = actions.FetchBrregSourceFinancialStatementsActivityInput
type FetchBrregSourceFinancialStatementsActivityResult = actions.FetchBrregSourceFinancialStatementsActivityResult

type FetchBrregSourceFinancialStatementsInput struct {
	Limit            int    `json:"limit,omitempty"`
	BatchSize        int    `json:"batch_size,omitempty"`
	MaxParallelTasks int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds     int    `json:"lease_seconds,omitempty"`
	MaxAttempts      int    `json:"max_attempts,omitempty"`
	Trigger          string `json:"trigger,omitempty"`
}

type FetchBrregSourceFinancialStatementsResult struct {
	Status             string `json:"status"`
	RecordsClaimed     int32  `json:"records_claimed"`
	RecordsCompleted   int32  `json:"records_completed"`
	RecordsSkipped     int32  `json:"records_skipped"`
	RecordsFailed      int32  `json:"records_failed"`
	StatementsUpserted int32  `json:"statements_upserted"`
	StatusRowsInserted int32  `json:"status_rows_inserted"`
	BatchesProcessed   int32  `json:"batches_processed"`
	StoppedReason      string `json:"stopped_reason"`
}

func FetchBrregSourceFinancialStatements(
	ctx temporalworkflow.Context,
	input FetchBrregSourceFinancialStatementsInput,
) (FetchBrregSourceFinancialStatementsResult, error) {
	input = normalizeFetchBrregSourceFinancialStatementsInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    30 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    10,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("brreg source financial workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"trigger", input.Trigger,
	)

	var activityResult FetchBrregSourceFinancialStatementsActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, fetchBrregSourceFinancialStatementsActivity, FetchBrregSourceFinancialStatementsActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		MaxParallelTasks:   int32(input.MaxParallelTasks),
		LeaseSeconds:       int32(input.LeaseSeconds),
		MaxAttempts:        int32(input.MaxAttempts),
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return FetchBrregSourceFinancialStatementsResult{}, errors.Wrap(err, "fetch brreg source financial statements")
	}
	logger.Debug("brreg source financial workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"records_claimed", activityResult.RecordsClaimed,
		"records_completed", activityResult.RecordsCompleted,
		"records_skipped", activityResult.RecordsSkipped,
		"records_failed", activityResult.RecordsFailed,
		"statements_upserted", activityResult.StatementsUpserted,
		"status_rows_inserted", activityResult.StatusRowsInserted,
		"batches_processed", activityResult.BatchesProcessed,
		"stopped_reason", activityResult.StoppedReason,
	)
	status := "succeeded"
	if activityResult.RecordsClaimed > 0 && activityResult.RecordsCompleted == 0 && activityResult.RecordsSkipped == 0 && activityResult.RecordsFailed > 0 {
		status = "failed"
	}
	result := FetchBrregSourceFinancialStatementsResult{
		Status:             status,
		RecordsClaimed:     activityResult.RecordsClaimed,
		RecordsCompleted:   activityResult.RecordsCompleted,
		RecordsSkipped:     activityResult.RecordsSkipped,
		RecordsFailed:      activityResult.RecordsFailed,
		StatementsUpserted: activityResult.StatementsUpserted,
		StatusRowsInserted: activityResult.StatusRowsInserted,
		BatchesProcessed:   activityResult.BatchesProcessed,
		StoppedReason:      activityResult.StoppedReason,
	}
	if status == "failed" {
		return result, errors.New("all brreg source financial records failed")
	}
	return result, nil
}

func normalizeFetchBrregSourceFinancialStatementsInput(input FetchBrregSourceFinancialStatementsInput) FetchBrregSourceFinancialStatementsInput {
	if input.BatchSize <= 0 {
		input.BatchSize = 10
	}
	if input.MaxParallelTasks <= 0 {
		input.MaxParallelTasks = 25
	}
	if input.LeaseSeconds <= 0 {
		input.LeaseSeconds = 900
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = 3
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

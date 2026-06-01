package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	TranslateBrregRawInputsTaskQueue    = "brreg-translation"
	TranslateBrregRawInputsWorkflowName = "TranslateBrregRawInputs"

	prepareBrregTranslationWorkflowActivity    = "PrepareBrregTranslationWorkflow"
	claimBrregTranslationBatchActivity         = "ClaimBrregTranslationBatch"
	translateBrregBatchActivity                = "TranslateBrregBatch"
	submitBrregTranslationBatchActivity        = "SubmitBrregTranslationBatch"
	failRunningBrregTranslationTasksActivity   = "FailRunningBrregTranslationTasksForWorkflow"
	finishBrregTranslationWorkflowActivity     = "FinishBrregTranslationWorkflow"
	defaultTranslationLimit                    = 1000
	defaultTranslationBatchSize                = 50
	defaultTranslationMaxAttempts              = 3
	defaultTranslationLeaseSeconds             = 900
	defaultTranslationMaxParallelTasks         = 50
	defaultTranslationServiceRetries           = 2
	defaultTranslationMutationActivityAttempts = 1
	defaultTranslationServiceActivityAttempts  = 3
)

type PrepareBrregTranslationWorkflowInput = actions.PrepareBrregTranslationWorkflowInput
type PrepareBrregTranslationWorkflowResult = actions.PrepareBrregTranslationWorkflowResult
type FinishBrregTranslationWorkflowInput = actions.FinishBrregTranslationWorkflowInput
type FinishBrregTranslationWorkflowResult = actions.FinishBrregTranslationWorkflowResult
type ClaimBrregTranslationBatchInput = actions.ClaimBrregTranslationBatchInput
type ClaimBrregTranslationBatchResult = actions.ClaimBrregTranslationBatchResult
type ClaimedTranslationRecord = actions.ClaimedTranslationRecord
type TranslateBrregBatchInput = actions.TranslateBrregBatchInput
type TranslateBrregBatchResult = actions.TranslateBrregBatchResult
type TranslationRecordResult = actions.TranslationRecordResult
type SubmitBrregTranslationBatchInput = actions.SubmitBrregTranslationBatchInput
type SubmitBrregTranslationBatchResult = actions.SubmitBrregTranslationBatchResult
type FailRunningBrregTranslationTasksForWorkflowInput = actions.FailRunningBrregTranslationTasksForWorkflowInput
type FailRunningBrregTranslationTasksForWorkflowResult = actions.FailRunningBrregTranslationTasksForWorkflowResult

type TranslateBrregRawInputsInput struct {
	IDs         []string          `json:"ids,omitempty"`
	Filters     map[string]string `json:"filters,omitempty"`
	Limit       int               `json:"limit,omitempty"`
	BatchSize   int               `json:"batch_size,omitempty"`
	MaxAttempts int               `json:"max_attempts,omitempty"`
	Trigger     string            `json:"trigger,omitempty"`

	MaxParallelTasks  int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds      int    `json:"lease_seconds,omitempty"`
	Provider          string `json:"provider,omitempty"`
	Model             string `json:"model,omitempty"`
	PromptVersion     string `json:"prompt_version,omitempty"`
	SourceLang        string `json:"source_lang,omitempty"`
	TargetLang        string `json:"target_lang,omitempty"`
	MaxServiceRetries int    `json:"max_service_retries,omitempty"`
}

type TranslateBrregRawInputsResult struct {
	Status           string `json:"status"`
	WorkflowRunID    string `json:"workflow_run_id,omitempty"`
	SelectionHash    string `json:"selection_hash,omitempty"`
	RecordsSelected  int32  `json:"records_selected"`
	RecordsClaimed   int32  `json:"records_claimed"`
	RecordsCompleted int32  `json:"records_completed"`
	RecordsFailed    int32  `json:"records_failed"`
	RecordsSkipped   int32  `json:"records_skipped"`
	BatchesProcessed int32  `json:"batches_processed"`
}

func TranslateBrregRawInputs(ctx temporalworkflow.Context, input TranslateBrregRawInputsInput) (TranslateBrregRawInputsResult, error) {
	input = normalizeTranslateBrregInput(input)
	ctx = brregTranslationActivityContext(ctx)
	workflowInfo := temporalworkflow.GetInfo(ctx)

	var prepared PrepareBrregTranslationWorkflowResult
	if err := temporalworkflow.ExecuteActivity(ctx, prepareBrregTranslationWorkflowActivity, PrepareBrregTranslationWorkflowInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		IDs:                input.IDs,
		Filters:            input.Filters,
		Limit:              int32(input.Limit),
		BatchSize:          int32(input.BatchSize),
		MaxAttempts:        int32(input.MaxAttempts),
		Trigger:            input.Trigger,
	}).Get(ctx, &prepared); err != nil {
		return TranslateBrregRawInputsResult{}, errors.Wrap(err, "prepare brreg translation workflow")
	}

	result := TranslateBrregRawInputsResult{
		Status:          "running",
		WorkflowRunID:   prepared.WorkflowRunID,
		SelectionHash:   prepared.SelectionHash,
		RecordsSelected: prepared.RecordsSelected,
	}
	finished := false
	defer func() {
		if finished || result.WorkflowRunID == "" {
			return
		}
		disconnectedCtx, cancel := temporalworkflow.NewDisconnectedContext(ctx)
		defer cancel()
		_ = temporalworkflow.ExecuteActivity(disconnectedCtx, failRunningBrregTranslationTasksActivity, FailRunningBrregTranslationTasksForWorkflowInput{
			WorkflowRunID: result.WorkflowRunID,
			MaxAttempts:   int32(input.MaxAttempts),
			Error:         "translation workflow failed before all claimed records were submitted",
		}).Get(disconnectedCtx, nil)
		_ = temporalworkflow.ExecuteActivity(disconnectedCtx, finishBrregTranslationWorkflowActivity, FinishBrregTranslationWorkflowInput{
			WorkflowRunID:    result.WorkflowRunID,
			Status:           "failed",
			RecordsSeen:      result.RecordsClaimed,
			RecordsCompleted: result.RecordsCompleted,
			RecordsFailed:    result.RecordsFailed,
			Error:            "translation workflow failed",
		}).Get(disconnectedCtx, nil)
	}()

	if prepared.RecordsSelected == 0 {
		result.Status = "drained"
		if err := finishBrregTranslationWorkflow(ctx, result, "succeeded", ""); err != nil {
			return result, err
		}
		finished = true
		return result, nil
	}

	for {
		var claimed ClaimBrregTranslationBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, claimBrregTranslationBatchActivity, ClaimBrregTranslationBatchInput{
			WorkflowRunID:    prepared.WorkflowRunID,
			SelectionHash:    prepared.SelectionHash,
			BatchSize:        prepared.BatchSize,
			MaxParallelTasks: int32(input.MaxParallelTasks),
			LeaseSeconds:     int32(input.LeaseSeconds),
			MaxAttempts:      prepared.MaxAttempts,
			WorkerID:         workflowInfo.WorkflowExecution.ID,
		}).Get(ctx, &claimed); err != nil {
			return result, errors.Wrap(err, "claim brreg translation batch")
		}
		if len(claimed.Records) == 0 {
			break
		}

		result.BatchesProcessed++
		result.RecordsClaimed += int32(len(claimed.Records))

		var translated TranslateBrregBatchResult
		serviceCtx := brregTranslationServiceActivityContext(ctx, input.LeaseSeconds)
		if err := temporalworkflow.ExecuteActivity(serviceCtx, translateBrregBatchActivity, TranslateBrregBatchInput{
			Records:       claimed.Records,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
			SourceLang:    input.SourceLang,
			TargetLang:    input.TargetLang,
			MaxRetries:    input.MaxServiceRetries,
		}).Get(serviceCtx, &translated); err != nil {
			return result, errors.Wrap(err, "translate brreg batch")
		}

		var submitted SubmitBrregTranslationBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, submitBrregTranslationBatchActivity, SubmitBrregTranslationBatchInput{
			Results:     translated.Results,
			MaxAttempts: prepared.MaxAttempts,
		}).Get(ctx, &submitted); err != nil {
			return result, errors.Wrap(err, "submit brreg translation batch")
		}

		result.RecordsCompleted += submitted.RecordsCompleted
		result.RecordsFailed += submitted.RecordsFailed
		result.RecordsSkipped += submitted.RecordsSkipped
	}

	result.Status = "succeeded"
	if err := finishBrregTranslationWorkflow(ctx, result, "succeeded", ""); err != nil {
		return result, err
	}
	finished = true
	return result, nil
}

func normalizeTranslateBrregInput(input TranslateBrregRawInputsInput) TranslateBrregRawInputsInput {
	if input.Limit <= 0 {
		input.Limit = defaultTranslationLimit
	}
	if input.BatchSize <= 0 {
		input.BatchSize = defaultTranslationBatchSize
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = defaultTranslationMaxAttempts
	}
	if input.MaxParallelTasks <= 0 {
		input.MaxParallelTasks = defaultTranslationMaxParallelTasks
	}
	if input.LeaseSeconds <= 0 {
		input.LeaseSeconds = defaultTranslationLeaseSeconds
	}
	if input.MaxServiceRetries <= 0 {
		input.MaxServiceRetries = defaultTranslationServiceRetries
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func brregTranslationActivityContext(ctx temporalworkflow.Context) temporalworkflow.Context {
	return temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    defaultTranslationMutationActivityAttempts,
		},
	})
}

func brregTranslationServiceActivityContext(ctx temporalworkflow.Context, leaseSeconds int) temporalworkflow.Context {
	timeout := time.Duration(leaseSeconds) * time.Second
	if timeout < 10*time.Minute {
		timeout = 10 * time.Minute
	}
	return temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: timeout,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    defaultTranslationServiceActivityAttempts,
		},
	})
}

func finishBrregTranslationWorkflow(
	ctx temporalworkflow.Context,
	result TranslateBrregRawInputsResult,
	status string,
	errorMessage string,
) error {
	if err := temporalworkflow.ExecuteActivity(ctx, finishBrregTranslationWorkflowActivity, FinishBrregTranslationWorkflowInput{
		WorkflowRunID:    result.WorkflowRunID,
		Status:           status,
		RecordsSeen:      result.RecordsClaimed,
		RecordsCompleted: result.RecordsCompleted,
		RecordsFailed:    result.RecordsFailed,
		Error:            errorMessage,
	}).Get(ctx, nil); err != nil {
		return errors.Wrap(err, "finish brreg translation workflow")
	}
	return nil
}

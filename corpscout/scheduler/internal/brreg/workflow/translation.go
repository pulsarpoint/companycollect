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
type TranslationError = actions.TranslationError
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
	logger := temporalworkflow.GetLogger(ctx)

	logger.Debug("brreg translation workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"max_attempts", input.MaxAttempts,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
		"source_lang", input.SourceLang,
		"target_lang", input.TargetLang,
		"max_service_retries", input.MaxServiceRetries,
		"trigger", input.Trigger,
	)

	var prepared PrepareBrregTranslationWorkflowResult
	logger.Debug("brreg translation workflow preparing selection",
		"activity", prepareBrregTranslationWorkflowActivity,
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
	)
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
	logger.Debug("brreg translation workflow selection prepared",
		"activity", prepareBrregTranslationWorkflowActivity,
		"workflow_run_id", prepared.WorkflowRunID,
		"selection_hash", prepared.SelectionHash,
		"records_selected", prepared.RecordsSelected,
		"batch_size", prepared.BatchSize,
		"max_attempts", prepared.MaxAttempts,
	)

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
		logger.Debug("brreg translation workflow cleanup started",
			"workflow_run_id", result.WorkflowRunID,
			"records_claimed", result.RecordsClaimed,
			"records_completed", result.RecordsCompleted,
			"records_failed", result.RecordsFailed,
			"records_skipped", result.RecordsSkipped,
			"batches_processed", result.BatchesProcessed,
		)
		disconnectedCtx, cancel := temporalworkflow.NewDisconnectedContext(ctx)
		defer cancel()
		var failedRunning FailRunningBrregTranslationTasksForWorkflowResult
		if err := temporalworkflow.ExecuteActivity(disconnectedCtx, failRunningBrregTranslationTasksActivity, FailRunningBrregTranslationTasksForWorkflowInput{
			WorkflowRunID: result.WorkflowRunID,
			MaxAttempts:   int32(input.MaxAttempts),
			Error:         "translation workflow failed before all claimed records were submitted",
		}).Get(disconnectedCtx, &failedRunning); err != nil {
			logger.Warn("brreg translation workflow cleanup failed to release running tasks",
				"workflow_run_id", result.WorkflowRunID,
				"error", err,
			)
		} else {
			logger.Debug("brreg translation workflow cleanup released running tasks",
				"workflow_run_id", result.WorkflowRunID,
				"failed_tasks", failedRunning.FailedTasks,
			)
		}
		if err := temporalworkflow.ExecuteActivity(disconnectedCtx, finishBrregTranslationWorkflowActivity, FinishBrregTranslationWorkflowInput{
			WorkflowRunID:    result.WorkflowRunID,
			Status:           "failed",
			RecordsSeen:      result.RecordsClaimed,
			RecordsCompleted: result.RecordsCompleted,
			RecordsFailed:    result.RecordsFailed,
			Error:            "translation workflow failed",
		}).Get(disconnectedCtx, nil); err != nil {
			logger.Warn("brreg translation workflow cleanup failed to finish audit run",
				"workflow_run_id", result.WorkflowRunID,
				"error", err,
			)
		} else {
			logger.Debug("brreg translation workflow cleanup finished audit run",
				"workflow_run_id", result.WorkflowRunID,
				"status", "failed",
			)
		}
	}()

	if prepared.RecordsSelected == 0 {
		result.Status = "drained"
		logger.Debug("brreg translation workflow drained before claiming records",
			"workflow_run_id", prepared.WorkflowRunID,
			"selection_hash", prepared.SelectionHash,
		)
		if err := finishBrregTranslationWorkflow(ctx, result, "succeeded", ""); err != nil {
			return result, err
		}
		finished = true
		return result, nil
	}

	for {
		var claimed ClaimBrregTranslationBatchResult
		logger.Debug("brreg translation workflow claiming batch",
			"activity", claimBrregTranslationBatchActivity,
			"workflow_run_id", prepared.WorkflowRunID,
			"selection_hash", prepared.SelectionHash,
			"batch_size", prepared.BatchSize,
			"max_parallel_tasks", input.MaxParallelTasks,
			"lease_seconds", input.LeaseSeconds,
			"max_attempts", prepared.MaxAttempts,
			"batches_processed", result.BatchesProcessed,
		)
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
		logger.Debug("brreg translation workflow claimed batch",
			"activity", claimBrregTranslationBatchActivity,
			"workflow_run_id", prepared.WorkflowRunID,
			"records_claimed", len(claimed.Records),
			"batches_processed", result.BatchesProcessed,
		)
		if len(claimed.Records) == 0 {
			logger.Debug("brreg translation workflow no more claimable records",
				"workflow_run_id", prepared.WorkflowRunID,
				"records_claimed_total", result.RecordsClaimed,
				"records_completed", result.RecordsCompleted,
				"records_failed", result.RecordsFailed,
				"records_skipped", result.RecordsSkipped,
				"batches_processed", result.BatchesProcessed,
			)
			break
		}

		result.BatchesProcessed++
		result.RecordsClaimed += int32(len(claimed.Records))

		var translated TranslateBrregBatchResult
		serviceCtx := brregTranslationServiceActivityContext(ctx, input.LeaseSeconds)
		logger.Debug("brreg translation workflow translating batch",
			"activity", translateBrregBatchActivity,
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"records_count", len(claimed.Records),
			"provider", input.Provider,
			"model", input.Model,
			"prompt_version", input.PromptVersion,
			"source_lang", input.SourceLang,
			"target_lang", input.TargetLang,
			"max_service_retries", input.MaxServiceRetries,
		)
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
		logger.Debug("brreg translation workflow translated batch",
			"activity", translateBrregBatchActivity,
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"status", translated.Status,
			"records_seen", translated.RecordsSeen,
			"records_completed", translated.RecordsCompleted,
			"records_failed", translated.RecordsFailed,
			"records_skipped", translated.RecordsSkipped,
			"results_count", len(translated.Results),
			"duration_ms", translated.DurationMS,
			"provider", translated.Provider,
			"model", translated.Model,
			"prompt_version", translated.PromptVersion,
		)

		var submitted SubmitBrregTranslationBatchResult
		logger.Debug("brreg translation workflow submitting batch",
			"activity", submitBrregTranslationBatchActivity,
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"results_count", len(translated.Results),
			"max_attempts", prepared.MaxAttempts,
		)
		if err := temporalworkflow.ExecuteActivity(ctx, submitBrregTranslationBatchActivity, SubmitBrregTranslationBatchInput{
			Results:     translated.Results,
			MaxAttempts: prepared.MaxAttempts,
		}).Get(ctx, &submitted); err != nil {
			return result, errors.Wrap(err, "submit brreg translation batch")
		}
		logger.Debug("brreg translation workflow submitted batch",
			"activity", submitBrregTranslationBatchActivity,
			"workflow_run_id", prepared.WorkflowRunID,
			"batch_number", result.BatchesProcessed,
			"records_submitted", submitted.RecordsSubmitted,
			"records_completed", submitted.RecordsCompleted,
			"records_failed", submitted.RecordsFailed,
			"records_skipped", submitted.RecordsSkipped,
		)

		result.RecordsCompleted += submitted.RecordsCompleted
		result.RecordsFailed += submitted.RecordsFailed
		result.RecordsSkipped += submitted.RecordsSkipped
	}

	if result.RecordsClaimed > 0 && result.RecordsCompleted == 0 && result.RecordsFailed > 0 {
		result.Status = "failed"
		finalError := "all translation records failed"
		logger.Warn("brreg translation workflow failed all claimed records",
			"workflow_run_id", result.WorkflowRunID,
			"records_selected", result.RecordsSelected,
			"records_claimed", result.RecordsClaimed,
			"records_failed", result.RecordsFailed,
			"records_skipped", result.RecordsSkipped,
			"batches_processed", result.BatchesProcessed,
		)
		if err := finishBrregTranslationWorkflow(ctx, result, "failed", finalError); err != nil {
			return result, err
		}
		finished = true
		return result, errors.New(finalError)
	}

	result.Status = "succeeded"
	logger.Debug("brreg translation workflow finishing",
		"workflow_run_id", result.WorkflowRunID,
		"status", result.Status,
		"records_selected", result.RecordsSelected,
		"records_claimed", result.RecordsClaimed,
		"records_completed", result.RecordsCompleted,
		"records_failed", result.RecordsFailed,
		"records_skipped", result.RecordsSkipped,
		"batches_processed", result.BatchesProcessed,
	)
	if err := finishBrregTranslationWorkflow(ctx, result, "succeeded", ""); err != nil {
		return result, err
	}
	finished = true
	logger.Debug("brreg translation workflow finished",
		"workflow_run_id", result.WorkflowRunID,
		"status", result.Status,
		"records_selected", result.RecordsSelected,
		"records_claimed", result.RecordsClaimed,
		"records_completed", result.RecordsCompleted,
		"records_failed", result.RecordsFailed,
		"records_skipped", result.RecordsSkipped,
		"batches_processed", result.BatchesProcessed,
	)
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
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("brreg translation workflow finishing audit run",
		"activity", finishBrregTranslationWorkflowActivity,
		"workflow_run_id", result.WorkflowRunID,
		"status", status,
		"records_seen", result.RecordsClaimed,
		"records_completed", result.RecordsCompleted,
		"records_failed", result.RecordsFailed,
		"has_error", errorMessage != "",
	)
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
	logger.Debug("brreg translation workflow finished audit run",
		"activity", finishBrregTranslationWorkflowActivity,
		"workflow_run_id", result.WorkflowRunID,
		"status", status,
	)
	return nil
}

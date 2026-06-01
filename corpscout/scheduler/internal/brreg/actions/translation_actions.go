package actions

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"

	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

const (
	defaultTranslationPromptVersion = "v1"
	defaultTranslationSourceLang    = "no"
	defaultTranslationTargetLang    = "en"
	defaultTranslationLimit         = 1000
	defaultTranslationBatchSize     = 50
	defaultTranslationMaxAttempts   = 3
)

type TranslationActions struct {
	gateway    *brregdb.Gateway
	translator *translationclient.Client
}

func NewTranslationActions(gateway *brregdb.Gateway, translator *translationclient.Client) *TranslationActions {
	return &TranslationActions{gateway: gateway, translator: translator}
}

type PrepareBrregTranslationWorkflowInput struct {
	TemporalWorkflowID string            `json:"temporal_workflow_id"`
	IDs                []string          `json:"ids,omitempty"`
	Filters            map[string]string `json:"filters,omitempty"`
	Limit              int32             `json:"limit"`
	BatchSize          int32             `json:"batch_size"`
	MaxAttempts        int32             `json:"max_attempts"`
	Trigger            string            `json:"trigger,omitempty"`
}

type PrepareBrregTranslationWorkflowResult struct {
	WorkflowRunID   string `json:"workflow_run_id"`
	SelectionHash   string `json:"selection_hash"`
	RecordsSelected int32  `json:"records_selected"`
	BatchSize       int32  `json:"batch_size"`
	MaxAttempts     int32  `json:"max_attempts"`
}

func (a *TranslationActions) PrepareBrregTranslationWorkflow(ctx context.Context, input PrepareBrregTranslationWorkflowInput) (PrepareBrregTranslationWorkflowResult, error) {
	if a == nil || a.gateway == nil {
		return PrepareBrregTranslationWorkflowResult{}, errors.New("brreg translation gateway not available")
	}
	slog.DebugContext(ctx, "preparing brreg translation workflow",
		"temporal_workflow_id", input.TemporalWorkflowID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"max_attempts", input.MaxAttempts,
		"trigger", input.Trigger,
	)
	prepared, err := a.gateway.PrepareWorkflow(ctx, prepareTranslationWorkflowCommandFromInput(input))
	if err != nil {
		return PrepareBrregTranslationWorkflowResult{}, errors.Wrap(err, "prepare brreg translation workflow")
	}
	slog.DebugContext(ctx, "prepared brreg translation workflow",
		"workflow_run_id", prepared.WorkflowRunID.String(),
		"selection_hash", prepared.SelectionHash,
		"records_selected", prepared.RecordsSelected,
		"batch_size", prepared.BatchSize,
		"max_attempts", prepared.MaxAttempts,
	)
	return PrepareBrregTranslationWorkflowResult{
		WorkflowRunID:   prepared.WorkflowRunID.String(),
		SelectionHash:   prepared.SelectionHash,
		RecordsSelected: prepared.RecordsSelected,
		BatchSize:       prepared.BatchSize,
		MaxAttempts:     prepared.MaxAttempts,
	}, nil
}

func prepareTranslationWorkflowCommandFromInput(input PrepareBrregTranslationWorkflowInput) brregdb.PrepareWorkflowCommand {
	return brregdb.PrepareWorkflowCommand{
		Source:             "brreg",
		Action:             "translate",
		TaskType:           brregdb.TaskTypeTranslate,
		Trigger:            input.Trigger,
		WorkflowID:         input.TemporalWorkflowID,
		IDs:                input.IDs,
		Filters:            input.Filters,
		Limit:              input.Limit,
		BatchSize:          input.BatchSize,
		MaxAttempts:        input.MaxAttempts,
		DefaultLimit:       defaultTranslationLimit,
		DefaultBatchSize:   defaultTranslationBatchSize,
		DefaultMaxAttempts: defaultTranslationMaxAttempts,
	}
}

type FinishBrregTranslationWorkflowInput struct {
	WorkflowRunID    string `json:"workflow_run_id"`
	Status           string `json:"status"`
	RecordsSeen      int32  `json:"records_seen"`
	RecordsCompleted int32  `json:"records_completed"`
	RecordsFailed    int32  `json:"records_failed"`
	Error            string `json:"error,omitempty"`
}

type FinishBrregTranslationWorkflowResult struct{}

func (a *TranslationActions) FinishBrregTranslationWorkflow(ctx context.Context, input FinishBrregTranslationWorkflowInput) (FinishBrregTranslationWorkflowResult, error) {
	if a == nil || a.gateway == nil {
		return FinishBrregTranslationWorkflowResult{}, errors.New("brreg translation gateway not available")
	}
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return FinishBrregTranslationWorkflowResult{}, errors.Wrap(err, "parse brreg translation workflow run id")
	}
	var workflowError *string
	if input.Error != "" {
		workflowError = &input.Error
	}
	slog.DebugContext(ctx, "finishing brreg translation workflow",
		"workflow_run_id", input.WorkflowRunID,
		"status", input.Status,
		"records_seen", input.RecordsSeen,
		"records_completed", input.RecordsCompleted,
		"records_failed", input.RecordsFailed,
		"has_error", workflowError != nil,
	)
	if err := a.gateway.FinishWorkflowRun(ctx, finishTranslationWorkflowCommandFromInput(input, workflowRunID, workflowError)); err != nil {
		return FinishBrregTranslationWorkflowResult{}, errors.Wrap(err, "finish brreg translation workflow")
	}
	slog.DebugContext(ctx, "finished brreg translation workflow",
		"workflow_run_id", input.WorkflowRunID,
		"status", input.Status,
		"records_seen", input.RecordsSeen,
		"records_completed", input.RecordsCompleted,
		"records_failed", input.RecordsFailed,
	)
	return FinishBrregTranslationWorkflowResult{}, nil
}

func finishTranslationWorkflowCommandFromInput(
	input FinishBrregTranslationWorkflowInput,
	workflowRunID uuid.UUID,
	workflowError *string,
) brregdb.FinishWorkflowRunCommand {
	return brregdb.FinishWorkflowRunCommand{
		WorkflowRunID:    workflowRunID,
		Status:           brregdb.WorkflowRunStatus(input.Status),
		RecordsSeen:      input.RecordsSeen,
		RecordsCompleted: input.RecordsCompleted,
		RecordsFailed:    input.RecordsFailed,
		Error:            workflowError,
	}
}

type FailRunningBrregTranslationTasksForWorkflowInput struct {
	WorkflowRunID string `json:"workflow_run_id"`
	MaxAttempts   int32  `json:"max_attempts"`
	Error         string `json:"error"`
}

type FailRunningBrregTranslationTasksForWorkflowResult struct {
	FailedTasks int32 `json:"failed_tasks"`
}

func (a *TranslationActions) FailRunningBrregTranslationTasksForWorkflow(ctx context.Context, input FailRunningBrregTranslationTasksForWorkflowInput) (FailRunningBrregTranslationTasksForWorkflowResult, error) {
	if a == nil || a.gateway == nil {
		return FailRunningBrregTranslationTasksForWorkflowResult{}, errors.New("brreg translation gateway not available")
	}
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return FailRunningBrregTranslationTasksForWorkflowResult{}, errors.Wrap(err, "parse brreg translation workflow run id")
	}
	errorMessage := input.Error
	slog.DebugContext(ctx, "failing running brreg translation tasks for workflow",
		"workflow_run_id", input.WorkflowRunID,
		"max_attempts", input.MaxAttempts,
	)
	failedTasks, err := a.gateway.FailRunningTasksForWorkflowRun(ctx, brregdb.FinishWorkflowRunCommand{
		WorkflowRunID: workflowRunID,
		MaxAttempts:   input.MaxAttempts,
		Error:         &errorMessage,
	})
	if err != nil {
		return FailRunningBrregTranslationTasksForWorkflowResult{}, errors.Wrap(err, "fail running brreg translation tasks for workflow")
	}
	slog.DebugContext(ctx, "failed running brreg translation tasks for workflow",
		"workflow_run_id", input.WorkflowRunID,
		"failed_tasks", failedTasks,
	)
	return FailRunningBrregTranslationTasksForWorkflowResult{FailedTasks: failedTasks}, nil
}

type ClaimBrregTranslationBatchInput struct {
	WorkflowRunID    string          `json:"workflow_run_id"`
	SelectionHash    string          `json:"selection_hash"`
	BatchSize        int32           `json:"batch_size"`
	MaxParallelTasks int32           `json:"max_parallel_tasks"`
	LeaseSeconds     int32           `json:"lease_seconds"`
	MaxAttempts      int32           `json:"max_attempts"`
	WorkerID         string          `json:"worker_id,omitempty"`
	Metadata         json.RawMessage `json:"metadata,omitempty"`
}

type ClaimBrregTranslationBatchResult struct {
	Records []ClaimedTranslationRecord `json:"records"`
}

type ClaimedTranslationRecord struct {
	RawRecordID        string          `json:"raw_record_id"`
	TaskAttemptID      string          `json:"task_attempt_id"`
	OrganizationNumber string          `json:"organization_number"`
	OrganizationName   string          `json:"organization_name,omitempty"`
	RawPayload         json.RawMessage `json:"raw_payload"`
	Attempt            int32           `json:"attempt"`
}

func (a *TranslationActions) ClaimBrregTranslationBatch(ctx context.Context, input ClaimBrregTranslationBatchInput) (ClaimBrregTranslationBatchResult, error) {
	if a == nil || a.gateway == nil {
		return ClaimBrregTranslationBatchResult{}, errors.New("brreg translation gateway not available")
	}
	slog.DebugContext(ctx, "claiming brreg translation batch",
		"workflow_run_id", input.WorkflowRunID,
		"selection_hash", input.SelectionHash,
		"batch_size", input.BatchSize,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"worker_id", input.WorkerID,
	)
	workflowRunID, err := uuid.Parse(input.WorkflowRunID)
	if err != nil {
		return ClaimBrregTranslationBatchResult{}, errors.Wrap(err, "parse brreg translation workflow run id")
	}
	command := claimTranslationCommandFromInput(input, workflowRunID)
	rows, err := a.gateway.ClaimTranslationBatch(ctx, command)
	if err != nil {
		return ClaimBrregTranslationBatchResult{}, errors.Wrap(err, "claim brreg translation batch")
	}
	slog.DebugContext(ctx, "claimed brreg translation batch",
		"workflow_run_id", input.WorkflowRunID,
		"records_count", len(rows),
		"selection_hash", input.SelectionHash,
		"first_raw_record_id", firstClaimedTranslationRawRecordID(rows),
		"first_attempt", firstClaimedTranslationAttempt(rows),
	)
	return ClaimBrregTranslationBatchResult{Records: claimedTranslationRecordsFromRows(rows)}, nil
}

func claimTranslationCommandFromInput(input ClaimBrregTranslationBatchInput, workflowRunID uuid.UUID) brregdb.ClaimTaskBatchCommand {
	command := brregdb.ClaimTaskBatchCommand{
		WorkflowRunID:    &workflowRunID,
		SelectionHash:    input.SelectionHash,
		BatchSize:        input.BatchSize,
		MaxParallelTasks: input.MaxParallelTasks,
		LeaseSeconds:     input.LeaseSeconds,
		MaxAttempts:      input.MaxAttempts,
		Metadata:         input.Metadata,
	}
	if input.WorkerID != "" {
		command.WorkerID = &input.WorkerID
	}
	return command
}

func claimedTranslationRecordsFromRows(rows []db.ClaimBrregWorkflowTaskSelectionBatchRow) []ClaimedTranslationRecord {
	records := make([]ClaimedTranslationRecord, 0, len(rows))
	for _, row := range rows {
		record := ClaimedTranslationRecord{
			RawRecordID:        row.RawRecordID.String(),
			TaskAttemptID:      row.TaskAttemptID.String(),
			OrganizationNumber: row.OrganizationNumber,
			RawPayload:         row.RawPayload,
			Attempt:            row.Attempt,
		}
		if row.OrganizationName != nil {
			record.OrganizationName = *row.OrganizationName
		}
		records = append(records, record)
	}
	return records
}

type TranslateBrregBatchInput struct {
	Records       []ClaimedTranslationRecord `json:"records"`
	Provider      string                     `json:"provider,omitempty"`
	Model         string                     `json:"model,omitempty"`
	PromptVersion string                     `json:"prompt_version,omitempty"`
	SourceLang    string                     `json:"source_lang,omitempty"`
	TargetLang    string                     `json:"target_lang,omitempty"`
	MaxRetries    int                        `json:"max_retries,omitempty"`
}

type TranslateBrregBatchResult struct {
	Status           string                    `json:"status"`
	Provider         string                    `json:"provider,omitempty"`
	Model            string                    `json:"model,omitempty"`
	PromptVersion    string                    `json:"prompt_version,omitempty"`
	RecordsSeen      int                       `json:"records_seen"`
	RecordsCompleted int                       `json:"records_completed"`
	RecordsFailed    int                       `json:"records_failed"`
	RecordsSkipped   int                       `json:"records_skipped"`
	DurationMS       int                       `json:"duration_ms"`
	Results          []TranslationRecordResult `json:"results"`
}

type TranslationRecordResult struct {
	RawRecordID        string            `json:"raw_record_id"`
	TaskAttemptID      string            `json:"task_attempt_id"`
	OrganizationNumber string            `json:"organization_number"`
	Status             string            `json:"status"`
	TranslatedPayload  map[string]any    `json:"translated_payload,omitempty"`
	MissingTerms       []string          `json:"missing_terms,omitempty"`
	Error              *TranslationError `json:"error,omitempty"`
	DurationMS         int               `json:"duration_ms,omitempty"`
	Provider           string            `json:"provider,omitempty"`
	Model              string            `json:"model,omitempty"`
	PromptVersion      string            `json:"prompt_version,omitempty"`
}

type TranslationError struct {
	Message       string         `json:"message"`
	Category      string         `json:"category,omitempty"`
	Code          string         `json:"code,omitempty"`
	RetryStrategy string         `json:"retry_strategy,omitempty"`
	Detail        map[string]any `json:"detail,omitempty"`
}

func (a *TranslationActions) TranslateBrregBatch(ctx context.Context, input TranslateBrregBatchInput) (TranslateBrregBatchResult, error) {
	if a == nil || a.translator == nil {
		return TranslateBrregBatchResult{}, errors.New("brreg translation client not available")
	}
	slog.DebugContext(ctx, "translating brreg batch",
		"records_count", len(input.Records),
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
		"source_lang", input.SourceLang,
		"target_lang", input.TargetLang,
		"max_retries", input.MaxRetries,
	)
	request := translateRequestFromInput(input)
	response, err := a.translator.TranslateBrregRecords(ctx, request)
	if err != nil {
		return TranslateBrregBatchResult{}, errors.Wrap(err, "translate brreg batch")
	}
	result := translateResultFromResponse(input.Records, response)
	slog.DebugContext(ctx, "translated brreg batch",
		"status", response.Status,
		"records_seen", response.RecordsSeen,
		"records_completed", response.RecordsCompleted,
		"records_failed", response.RecordsFailed,
		"records_skipped", response.RecordsSkipped,
		"duration_ms", response.DurationMS,
		"results_count", len(result.Results),
		"provider", result.Provider,
		"model", result.Model,
		"prompt_version", result.PromptVersion,
	)
	return result, nil
}

func translateRequestFromInput(input TranslateBrregBatchInput) translationclient.BrregTranslateRequest {
	request := translationclient.BrregTranslateRequest{
		Records:       make([]translationclient.BrregRecord, 0, len(input.Records)),
		LLM:           translationclient.LLMSelection{Provider: defaultString(input.Provider, "default"), Model: input.Model},
		PromptVersion: defaultString(input.PromptVersion, defaultTranslationPromptVersion),
		SourceLang:    defaultString(input.SourceLang, defaultTranslationSourceLang),
		TargetLang:    defaultString(input.TargetLang, defaultTranslationTargetLang),
		MaxRetries:    input.MaxRetries,
	}
	for _, record := range input.Records {
		request.Records = append(request.Records, translationclient.BrregRecord{
			RecordID:           record.RawRecordID,
			OrganizationNumber: record.OrganizationNumber,
			RawPayload:         record.RawPayload,
		})
	}
	return request
}

func translateResultFromResponse(records []ClaimedTranslationRecord, response translationclient.BrregTranslateResponse) TranslateBrregBatchResult {
	claimedByRecordID := make(map[string]ClaimedTranslationRecord, len(records))
	for _, record := range records {
		claimedByRecordID[record.RawRecordID] = record
	}
	results := make([]TranslationRecordResult, 0, len(response.Results))
	seenRecordIDs := make(map[string]struct{}, len(response.Results))
	var unmatchedServiceError *TranslationError
	for _, result := range response.Results {
		claimed, ok := claimedByRecordID[result.RecordID]
		if !ok {
			if unmatchedServiceError == nil && result.Error != nil {
				unmatchedServiceError = translationErrorFromClient(result.Error)
			}
			continue
		}
		seenRecordIDs[result.RecordID] = struct{}{}
		results = append(results, TranslationRecordResult{
			RawRecordID:        result.RecordID,
			TaskAttemptID:      claimed.TaskAttemptID,
			OrganizationNumber: result.OrganizationNumber,
			Status:             result.Status,
			TranslatedPayload:  result.TranslatedPayload,
			MissingTerms:       result.MissingTerms,
			Error:              translationErrorFromClient(result.Error),
			DurationMS:         result.DurationMS,
			Provider:           response.Provider,
			Model:              response.Model,
			PromptVersion:      response.PromptVersion,
		})
	}
	missingResults := 0
	for _, record := range records {
		if _, ok := seenRecordIDs[record.RawRecordID]; ok {
			continue
		}
		missingResults++
		results = append(results, missingTranslationRecordResult(record, response, unmatchedServiceError))
	}
	recordsSeen := response.RecordsSeen
	if recordsSeen == 0 && len(records) > 0 {
		recordsSeen = len(records)
	}
	return TranslateBrregBatchResult{
		Status:           response.Status,
		Provider:         response.Provider,
		Model:            response.Model,
		PromptVersion:    response.PromptVersion,
		RecordsSeen:      recordsSeen,
		RecordsCompleted: response.RecordsCompleted,
		RecordsFailed:    response.RecordsFailed + missingResults,
		RecordsSkipped:   response.RecordsSkipped,
		DurationMS:       response.DurationMS,
		Results:          results,
	}
}

func missingTranslationRecordResult(
	record ClaimedTranslationRecord,
	response translationclient.BrregTranslateResponse,
	serviceError *TranslationError,
) TranslationRecordResult {
	err := serviceError
	if err == nil {
		err = &TranslationError{
			Message:       "translation service did not return a result for the claimed record",
			Category:      "invalid_translation_response",
			Code:          "missing_record_result",
			RetryStrategy: "retry_with_backoff",
		}
	}
	return TranslationRecordResult{
		RawRecordID:        record.RawRecordID,
		TaskAttemptID:      record.TaskAttemptID,
		OrganizationNumber: record.OrganizationNumber,
		Status:             brregdb.ResultStatusFailed.String(),
		Error:              err,
		Provider:           response.Provider,
		Model:              response.Model,
		PromptVersion:      response.PromptVersion,
	}
}

type SubmitBrregTranslationBatchInput struct {
	Results     []TranslationRecordResult `json:"results"`
	MaxAttempts int32                     `json:"max_attempts"`
}

type SubmitBrregTranslationBatchResult struct {
	RecordsSubmitted int32 `json:"records_submitted"`
	RecordsCompleted int32 `json:"records_completed"`
	RecordsFailed    int32 `json:"records_failed"`
	RecordsSkipped   int32 `json:"records_skipped"`
}

func (a *TranslationActions) SubmitBrregTranslationBatch(ctx context.Context, input SubmitBrregTranslationBatchInput) (SubmitBrregTranslationBatchResult, error) {
	if a == nil || a.gateway == nil {
		return SubmitBrregTranslationBatchResult{}, errors.New("brreg translation gateway not available")
	}
	slog.DebugContext(ctx, "submitting brreg translation batch results",
		"results_count", len(input.Results),
		"max_attempts", input.MaxAttempts,
	)
	var summary SubmitBrregTranslationBatchResult
	for _, result := range input.Results {
		slog.DebugContext(ctx, "submitting brreg translation result",
			"raw_record_id", result.RawRecordID,
			"task_attempt_id", result.TaskAttemptID,
			"organization_number", result.OrganizationNumber,
			"status", result.Status,
			"duration_ms", result.DurationMS,
			"provider", result.Provider,
			"model", result.Model,
			"prompt_version", result.PromptVersion,
			"error_category", translationErrorCategory(result.Error),
			"error_code", translationErrorCode(result.Error),
			"retry_strategy", translationErrorRetryStrategy(result.Error),
		)
		rawRecordID, err := uuid.Parse(result.RawRecordID)
		if err != nil {
			return SubmitBrregTranslationBatchResult{}, errors.Wrap(err, "parse brreg translation raw record id")
		}
		taskAttemptID, err := uuid.Parse(result.TaskAttemptID)
		if err != nil {
			return SubmitBrregTranslationBatchResult{}, errors.Wrap(err, "parse brreg translation task attempt id")
		}
		command, err := submitTranslationCommandFromResult(result, rawRecordID, taskAttemptID, input.MaxAttempts)
		if err != nil {
			return SubmitBrregTranslationBatchResult{}, err
		}
		if err := a.gateway.SubmitTranslationResult(ctx, command); err != nil {
			return SubmitBrregTranslationBatchResult{}, errors.Wrap(err, "submit brreg translation result")
		}
		slog.DebugContext(ctx, "submitted brreg translation result",
			"raw_record_id", result.RawRecordID,
			"task_attempt_id", result.TaskAttemptID,
			"organization_number", result.OrganizationNumber,
			"status", result.Status,
			"error_category", translationErrorCategory(result.Error),
			"error_code", translationErrorCode(result.Error),
			"retry_strategy", translationErrorRetryStrategy(result.Error),
		)
		summary.RecordsSubmitted++
		switch brregdb.ResultStatus(result.Status) {
		case brregdb.ResultStatusSucceeded:
			summary.RecordsCompleted++
		case brregdb.ResultStatusSkipped:
			summary.RecordsSkipped++
		case brregdb.ResultStatusFailed:
			summary.RecordsFailed++
		}
	}
	slog.DebugContext(ctx, "submitted brreg translation batch results",
		"records_submitted", summary.RecordsSubmitted,
		"records_completed", summary.RecordsCompleted,
		"records_failed", summary.RecordsFailed,
		"records_skipped", summary.RecordsSkipped,
	)
	return summary, nil
}

func submitTranslationCommandFromResult(
	result TranslationRecordResult,
	rawRecordID uuid.UUID,
	taskAttemptID uuid.UUID,
	maxAttempts int32,
) (brregdb.SubmitTranslationResultCommand, error) {
	translatedPayload, err := json.Marshal(result.TranslatedPayload)
	if err != nil {
		return brregdb.SubmitTranslationResultCommand{}, errors.Wrap(err, "marshal brreg translated payload")
	}
	metadata, err := json.Marshal(translationResultMetadata{
		OrganizationNumber: result.OrganizationNumber,
		MissingTerms:       result.MissingTerms,
		DurationMS:         result.DurationMS,
	})
	if err != nil {
		return brregdb.SubmitTranslationResultCommand{}, errors.Wrap(err, "marshal brreg translation metadata")
	}
	return brregdb.SubmitTranslationResultCommand{
		Result: db.InsertBrregWorkflowTranslationResultParams{
			RawRecordID:       rawRecordID,
			TaskAttemptID:     taskAttemptID,
			Status:            result.Status,
			TranslatedPayload: translatedPayload,
			Model:             stringPointer(result.Model),
			PromptVersion:     stringPointer(result.PromptVersion),
			Error:             translationErrorMessage(result.Error),
			Metadata:          metadata,
		},
		Failure:     taskFailureFromTranslationError(result.Status, result.Error),
		MaxAttempts: maxAttempts,
	}, nil
}

type translationResultMetadata struct {
	OrganizationNumber string   `json:"organization_number,omitempty"`
	MissingTerms       []string `json:"missing_terms,omitempty"`
	DurationMS         int      `json:"duration_ms,omitempty"`
}

func translationErrorFromClient(err *translationclient.TranslationError) *TranslationError {
	if err == nil {
		return nil
	}
	return &TranslationError{
		Message:       err.Message,
		Category:      err.Category,
		Code:          err.Code,
		RetryStrategy: err.RetryStrategy,
		Detail:        err.Detail,
	}
}

func taskFailureFromTranslationError(status string, err *TranslationError) *brregdb.TaskFailure {
	if brregdb.ResultStatus(status) != brregdb.ResultStatusFailed || err == nil {
		return nil
	}
	return &brregdb.TaskFailure{
		ErrorCategory: err.Category,
		ErrorCode:     err.Code,
		RetryStrategy: err.RetryStrategy,
	}
}

func translationErrorMessage(err *TranslationError) *string {
	if err == nil || err.Message == "" {
		return nil
	}
	return &err.Message
}

func stringPointer(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func firstClaimedTranslationRawRecordID(rows []db.ClaimBrregWorkflowTaskSelectionBatchRow) string {
	if len(rows) == 0 {
		return ""
	}
	return rows[0].RawRecordID.String()
}

func firstClaimedTranslationAttempt(rows []db.ClaimBrregWorkflowTaskSelectionBatchRow) int32 {
	if len(rows) == 0 {
		return 0
	}
	return rows[0].Attempt
}

func translationErrorCategory(err *TranslationError) string {
	if err == nil {
		return ""
	}
	return err.Category
}

func translationErrorCode(err *TranslationError) string {
	if err == nil {
		return ""
	}
	return err.Code
}

func translationErrorRetryStrategy(err *TranslationError) string {
	if err == nil {
		return ""
	}
	return err.RetryStrategy
}

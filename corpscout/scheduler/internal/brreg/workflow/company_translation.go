package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	TranslateBrregSourceCompaniesTaskQueue    = "brreg-company-translation"
	TranslateBrregSourceCompaniesWorkflowName = "TranslateBrregSourceCompanies"

	claimBrregCompaniesForTranslationActivity    = "ClaimBrregCompaniesForTranslation"
	applyBrregCachedCompanyTranslationsActivity  = "ApplyBrregCachedCompanyTranslations"
	markBrregCompanyTranslationSucceededActivity = "MarkBrregCompanyTranslationSucceeded"
	markBrregCompanyTranslationSkippedActivity   = "MarkBrregCompanyTranslationSkipped"
	markBrregCompanyTranslationFailedActivity    = "MarkBrregCompanyTranslationFailed"

	defaultCompanyTranslationBatchSize        = 10
	defaultCompanyTranslationMaxParallelTasks = 10
	defaultCompanyTranslationLeaseSeconds     = 900
	defaultCompanyTranslationMaxAttempts      = 5
	defaultCompanyTranslationPromptVersion    = "v1"
	defaultCompanyTranslationTrigger          = "manual"
)

type ClaimBrregCompaniesForTranslationInput = actions.ClaimBrregCompaniesForTranslationInput
type ClaimBrregCompaniesForTranslationResult = actions.ClaimBrregCompaniesForTranslationResult
type ClaimedCompanyForTranslation = actions.ClaimedCompanyForTranslation
type ApplyBrregCachedCompanyTranslationsInput = actions.ApplyBrregCachedCompanyTranslationsInput
type ApplyBrregCachedCompanyTranslationsResult = actions.ApplyBrregCachedCompanyTranslationsResult
type MarkBrregCompanyTranslationSucceededInput = actions.MarkBrregCompanyTranslationSucceededInput
type MarkBrregCompanyTranslationSkippedInput = actions.MarkBrregCompanyTranslationSkippedInput
type MarkBrregCompanyTranslationFailedInput = actions.MarkBrregCompanyTranslationFailedInput

type TranslateBrregSourceCompaniesInput struct {
	BatchSize        int    `json:"batch_size,omitempty"`
	MaxParallelTasks int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds     int    `json:"lease_seconds,omitempty"`
	MaxAttempts      int    `json:"max_attempts,omitempty"`
	PromptVersion    string `json:"prompt_version,omitempty"`
	Trigger          string `json:"trigger,omitempty"`
}

type TranslateBrregSourceCompaniesResult struct {
	Status             string `json:"status"`
	StatusRowsInserted int32  `json:"status_rows_inserted"`
	CompaniesClaimed   int32  `json:"companies_claimed"`
	CompaniesSucceeded int32  `json:"companies_succeeded"`
	CompaniesSkipped   int32  `json:"companies_skipped"`
	CompaniesFailed    int32  `json:"companies_failed"`
	FieldsSeen         int32  `json:"fields_seen"`
	FieldsApplied      int32  `json:"fields_applied"`
	RemainingFields    int32  `json:"remaining_fields"`
}

func TranslateBrregSourceCompanies(
	ctx temporalworkflow.Context,
	input TranslateBrregSourceCompaniesInput,
) (TranslateBrregSourceCompaniesResult, error) {
	input = normalizeTranslateBrregSourceCompaniesInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("brreg company translation workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"batch_size", input.BatchSize,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"prompt_version", input.PromptVersion,
		"trigger", input.Trigger,
	)

	result := TranslateBrregSourceCompaniesResult{Status: "running"}
	var claimed ClaimBrregCompaniesForTranslationResult
	if err := temporalworkflow.ExecuteActivity(ctx, claimBrregCompaniesForTranslationActivity, ClaimBrregCompaniesForTranslationInput{
		Limit:            int32(input.BatchSize),
		MaxParallelTasks: int32(input.MaxParallelTasks),
		LeaseSeconds:     int32(input.LeaseSeconds),
		MaxAttempts:      int32(input.MaxAttempts),
		WorkerID:         workflowInfo.WorkflowExecution.ID,
	}).Get(ctx, &claimed); err != nil {
		return result, errors.Wrap(err, "claim brreg companies for translation")
	}
	result.StatusRowsInserted = claimed.StatusRowsInserted
	result.CompaniesClaimed = int32(len(claimed.Companies))
	if len(claimed.Companies) == 0 {
		result.Status = "drained"
		return result, nil
	}

	for _, company := range claimed.Companies {
		var applied ApplyBrregCachedCompanyTranslationsResult
		err := temporalworkflow.ExecuteActivity(ctx, applyBrregCachedCompanyTranslationsActivity, ApplyBrregCachedCompanyTranslationsInput{
			CompanyID:     company.CompanyID,
			PromptVersion: input.PromptVersion,
		}).Get(ctx, &applied)
		if err != nil {
			result.CompaniesFailed++
			if markErr := markCompanyTranslationFailed(ctx, company.CompanyID, input.MaxAttempts, "apply cached company translations failed", "translation_cache", "apply_cached_failed", "retry_with_backoff", false); markErr != nil {
				return result, markErr
			}
			continue
		}
		result.FieldsSeen += applied.FieldsSeen
		result.FieldsApplied += applied.FieldsApplied
		result.RemainingFields += applied.RemainingFields

		switch {
		case applied.FieldsSeen == 0 && applied.RemainingFields == 0:
			if err := temporalworkflow.ExecuteActivity(ctx, markBrregCompanyTranslationSkippedActivity, MarkBrregCompanyTranslationSkippedInput{
				CompanyID: company.CompanyID,
			}).Get(ctx, nil); err != nil {
				return result, errors.Wrap(err, "mark brreg company translation skipped")
			}
			result.CompaniesSkipped++
		case applied.RemainingFields == 0:
			if err := temporalworkflow.ExecuteActivity(ctx, markBrregCompanyTranslationSucceededActivity, MarkBrregCompanyTranslationSucceededInput{
				CompanyID: company.CompanyID,
			}).Get(ctx, nil); err != nil {
				return result, errors.Wrap(err, "mark brreg company translation succeeded")
			}
			result.CompaniesSucceeded++
		default:
			result.CompaniesFailed++
			if err := markCompanyTranslationFailed(ctx, company.CompanyID, input.MaxAttempts, "company has translation fields without cached terms", "translation_cache", "missing_cached_terms", "wait_for_terms", false); err != nil {
				return result, err
			}
		}
	}

	if result.CompaniesFailed > 0 && result.CompaniesSucceeded == 0 && result.CompaniesSkipped == 0 {
		result.Status = "failed"
		return result, temporal.NewNonRetryableApplicationError("all company translation records failed", "BRREG_ALL_COMPANY_TRANSLATION_RECORDS_FAILED", nil)
	}
	if result.CompaniesFailed > 0 {
		result.Status = "partial"
		return result, nil
	}
	result.Status = "succeeded"
	return result, nil
}

func normalizeTranslateBrregSourceCompaniesInput(input TranslateBrregSourceCompaniesInput) TranslateBrregSourceCompaniesInput {
	if input.BatchSize <= 0 {
		input.BatchSize = defaultCompanyTranslationBatchSize
	}
	if input.MaxParallelTasks <= 0 {
		input.MaxParallelTasks = defaultCompanyTranslationMaxParallelTasks
	}
	if input.LeaseSeconds <= 0 {
		input.LeaseSeconds = defaultCompanyTranslationLeaseSeconds
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = defaultCompanyTranslationMaxAttempts
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultCompanyTranslationPromptVersion
	}
	if input.Trigger == "" {
		input.Trigger = defaultCompanyTranslationTrigger
	}
	return input
}

func markCompanyTranslationFailed(
	ctx temporalworkflow.Context,
	companyID string,
	maxAttempts int,
	message string,
	category string,
	code string,
	retryStrategy string,
	terminal bool,
) error {
	return errors.Wrap(
		temporalworkflow.ExecuteActivity(ctx, markBrregCompanyTranslationFailedActivity, MarkBrregCompanyTranslationFailedInput{
			CompanyID:     companyID,
			Error:         message,
			ErrorCategory: category,
			ErrorCode:     code,
			RetryStrategy: retryStrategy,
			MaxAttempts:   int32(maxAttempts),
			Terminal:      terminal,
		}).Get(ctx, nil),
		"mark brreg company translation failed",
	)
}

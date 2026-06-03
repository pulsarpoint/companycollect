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

	claimBrregCompaniesForTranslationActivity = "ClaimBrregCompaniesForTranslation"
	processBrregCompanyTranslationActivity    = "ProcessBrregCompanyTranslation"

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
type ProcessBrregCompanyTranslationInput = actions.ProcessBrregCompanyTranslationInput
type ProcessBrregCompanyTranslationResult = actions.ProcessBrregCompanyTranslationResult

type TranslateBrregSourceCompaniesInput struct {
	AllRecords       bool   `json:"all_records,omitempty"`
	BatchSize        int    `json:"batch_size,omitempty"`
	MaxParallelTasks int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds     int    `json:"lease_seconds,omitempty"`
	MaxAttempts      int    `json:"max_attempts,omitempty"`
	Provider         string `json:"provider,omitempty"`
	Model            string `json:"model,omitempty"`
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
		"all_records", input.AllRecords,
		"batch_size", input.BatchSize,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
		"trigger", input.Trigger,
	)

	result := TranslateBrregSourceCompaniesResult{Status: "running"}
	for {
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
		result.StatusRowsInserted += claimed.StatusRowsInserted
		if len(claimed.Companies) == 0 {
			break
		}
		result.CompaniesClaimed += int32(len(claimed.Companies))

		for _, company := range claimed.Companies {
			var processed ProcessBrregCompanyTranslationResult
			err := temporalworkflow.ExecuteActivity(ctx, processBrregCompanyTranslationActivity, ProcessBrregCompanyTranslationInput{
				CompanyID:     company.CompanyID,
				Provider:      input.Provider,
				Model:         input.Model,
				PromptVersion: input.PromptVersion,
				MaxAttempts:   int32(input.MaxAttempts),
			}).Get(ctx, &processed)
			if err != nil {
				return result, errors.Wrap(err, "process brreg company translation")
			}
			result.FieldsSeen += processed.FieldsSeen
			result.FieldsApplied += processed.FieldsApplied
			result.RemainingFields += processed.RemainingFields

			switch processed.Status {
			case "succeeded":
				result.CompaniesSucceeded++
			case "skipped":
				result.CompaniesSkipped++
			default:
				result.CompaniesFailed++
			}
		}

		if !input.AllRecords {
			break
		}
	}

	if result.CompaniesClaimed == 0 {
		result.Status = "drained"
		return result, nil
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

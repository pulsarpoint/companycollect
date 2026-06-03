package workflow

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	TranslateBrregSourceCompaniesTaskQueue    = "brreg-company-translation"
	TranslateBrregSourceCompaniesWorkflowName = "TranslateBrregSourceCompanies"

	claimBrregCompaniesForTranslationActivity     = "ClaimBrregCompaniesForTranslation"
	processBrregCompanyTranslationActivity        = "ProcessBrregCompanyTranslation"
	buildBrregTranslationWorksetActivity          = "BuildBrregTranslationWorkset"
	claimBrregTranslationWorksetBatchActivity     = "ClaimBrregTranslationWorksetBatch"
	translateBrregTranslationWorksetBatchActivity = "TranslateBrregTranslationWorksetBatch"
	saveBrregTranslationWorksetBatchActivity      = "SaveBrregTranslationWorksetBatch"
	applyBrregTranslationWorksetActivity          = "ApplyBrregTranslationWorkset"

	defaultCompanyTranslationBatchSize        = 10
	defaultCompanyTranslationMaxParallelTasks = 10
	defaultCompanyTranslationLeaseSeconds     = 900
	defaultCompanyTranslationMaxAttempts      = 5
	defaultCompanyTranslationMaxRequestChars  = 12000
	defaultCompanyTranslationMaxCompanies     = 500
	defaultCompanyTranslationMaxTerms         = 200
	defaultCompanyTranslationMaxBatches       = 200
	defaultCompanyTranslationPromptVersion    = "v1"
	defaultCompanyTranslationTrigger          = "manual"
)

type ClaimBrregCompaniesForTranslationInput = actions.ClaimBrregCompaniesForTranslationInput
type ClaimBrregCompaniesForTranslationResult = actions.ClaimBrregCompaniesForTranslationResult
type ClaimedCompanyForTranslation = actions.ClaimedCompanyForTranslation
type ProcessBrregCompanyTranslationInput = actions.ProcessBrregCompanyTranslationInput
type ProcessBrregCompanyTranslationResult = actions.ProcessBrregCompanyTranslationResult
type BuildBrregTranslationWorksetInput = actions.BuildBrregTranslationWorksetInput
type BuildBrregTranslationWorksetResult = actions.BuildBrregTranslationWorksetResult
type ClaimBrregTranslationWorksetBatchInput = actions.ClaimBrregTranslationWorksetBatchInput
type ClaimBrregTranslationWorksetBatchResult = actions.ClaimBrregTranslationWorksetBatchResult
type TranslationWorksetTerm = actions.TranslationWorksetTerm
type TranslateBrregTranslationWorksetBatchInput = actions.TranslateBrregTranslationWorksetBatchInput
type TranslateBrregTranslationWorksetBatchResult = actions.TranslateBrregTranslationWorksetBatchResult
type TranslationWorksetTermResult = actions.TranslationWorksetTermResult
type SaveBrregTranslationWorksetBatchInput = actions.SaveBrregTranslationWorksetBatchInput
type SaveBrregTranslationWorksetBatchResult = actions.SaveBrregTranslationWorksetBatchResult
type ApplyBrregTranslationWorksetInput = actions.ApplyBrregTranslationWorksetInput
type ApplyBrregTranslationWorksetResult = actions.ApplyBrregTranslationWorksetResult

type TranslateBrregSourceCompaniesInput struct {
	AllRecords           bool   `json:"all_records,omitempty"`
	BatchSize            int    `json:"batch_size,omitempty"`
	ClaimMode            string `json:"claim_mode,omitempty"`
	MaxRequestChars      int    `json:"max_request_chars,omitempty"`
	MaxTerms             int    `json:"max_terms,omitempty"`
	MaxCompaniesPerBatch int    `json:"max_companies_per_batch,omitempty"`
	MaxBatches           int    `json:"max_batches,omitempty"`
	MaxParallelTasks     int    `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds         int    `json:"lease_seconds,omitempty"`
	MaxAttempts          int    `json:"max_attempts,omitempty"`
	Provider             string `json:"provider,omitempty"`
	Model                string `json:"model,omitempty"`
	PromptVersion        string `json:"prompt_version,omitempty"`
	Trigger              string `json:"trigger,omitempty"`
	WorksetPath          string `json:"workset_path,omitempty"`
	WorksetPrepared      bool   `json:"workset_prepared,omitempty"`

	CarriedStatusRowsInserted int32 `json:"carried_status_rows_inserted,omitempty"`
	CarriedCompaniesClaimed   int32 `json:"carried_companies_claimed,omitempty"`
	CarriedFieldsSeen         int32 `json:"carried_fields_seen,omitempty"`
	CarriedFieldsApplied      int32 `json:"carried_fields_applied,omitempty"`
	CarriedRequestChars       int32 `json:"carried_request_chars,omitempty"`
	CarriedTermsClaimed       int32 `json:"carried_terms_claimed,omitempty"`
	CarriedTermsSucceeded     int32 `json:"carried_terms_succeeded,omitempty"`
	CarriedTermsFailed        int32 `json:"carried_terms_failed,omitempty"`
	CarriedBatchesProcessed   int32 `json:"carried_batches_processed,omitempty"`
}

type TranslateBrregSourceCompaniesResult struct {
	Status              string `json:"status"`
	StatusRowsInserted  int32  `json:"status_rows_inserted"`
	CompaniesClaimed    int32  `json:"companies_claimed"`
	CompaniesSucceeded  int32  `json:"companies_succeeded"`
	CompaniesSkipped    int32  `json:"companies_skipped"`
	CompaniesFailed     int32  `json:"companies_failed"`
	FieldsSeen          int32  `json:"fields_seen"`
	FieldsApplied       int32  `json:"fields_applied"`
	RemainingFields     int32  `json:"remaining_fields"`
	RequestCharsClaimed int32  `json:"request_chars_claimed"`
	TermsClaimed        int32  `json:"terms_claimed"`
	TermsSucceeded      int32  `json:"terms_succeeded"`
	TermsFailed         int32  `json:"terms_failed"`
	TermsSaved          int32  `json:"terms_saved"`
	BatchesProcessed    int32  `json:"batches_processed"`
	WorksetPath         string `json:"workset_path,omitempty"`
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
		"claim_mode", input.ClaimMode,
		"max_request_chars", input.MaxRequestChars,
		"max_terms", input.MaxTerms,
		"max_companies_per_batch", input.MaxCompaniesPerBatch,
		"max_batches", input.MaxBatches,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
		"trigger", input.Trigger,
		"workset_path", input.WorksetPath,
		"workset_prepared", input.WorksetPrepared,
	)

	result := TranslateBrregSourceCompaniesResult{
		Status:              "running",
		StatusRowsInserted:  input.CarriedStatusRowsInserted,
		CompaniesClaimed:    input.CarriedCompaniesClaimed,
		FieldsSeen:          input.CarriedFieldsSeen,
		FieldsApplied:       input.CarriedFieldsApplied,
		RequestCharsClaimed: input.CarriedRequestChars,
		TermsClaimed:        input.CarriedTermsClaimed,
		TermsSucceeded:      input.CarriedTermsSucceeded,
		TermsFailed:         input.CarriedTermsFailed,
		BatchesProcessed:    input.CarriedBatchesProcessed,
		WorksetPath:         input.WorksetPath,
	}
	if result.WorksetPath == "" {
		result.WorksetPath = defaultBrregTranslationWorksetPath(workflowInfo.WorkflowExecution.ID)
	}
	input.WorksetPath = result.WorksetPath

	if !input.WorksetPrepared {
		companyLimit := int32(input.BatchSize)
		if input.AllRecords {
			companyLimit = 0
		}
		var built BuildBrregTranslationWorksetResult
		if err := temporalworkflow.ExecuteActivity(ctx, buildBrregTranslationWorksetActivity, BuildBrregTranslationWorksetInput{
			Path:          result.WorksetPath,
			PromptVersion: input.PromptVersion,
			CompanyLimit:  companyLimit,
		}).Get(ctx, &built); err != nil {
			return result, errors.Wrap(err, "build brreg translation workset")
		}
		result.FieldsSeen = built.FieldsExported
		result.CompaniesClaimed = built.CompaniesExported
		if built.FieldsExported == 0 {
			result.Status = "drained"
			return result, nil
		}
		input.WorksetPrepared = true
		input.CarriedFieldsSeen = result.FieldsSeen
		input.CarriedCompaniesClaimed = result.CompaniesClaimed
	}

	for {
		var claimed ClaimBrregTranslationWorksetBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, claimBrregTranslationWorksetBatchActivity, ClaimBrregTranslationWorksetBatchInput{
			Path:            result.WorksetPath,
			MaxRequestChars: int32(input.MaxRequestChars),
			MaxTerms:        int32(input.MaxTerms),
			MaxAttempts:     int32(input.MaxAttempts),
		}).Get(ctx, &claimed); err != nil {
			return result, errors.Wrap(err, "claim brreg translation workset batch")
		}
		if claimed.Status == "drained" || len(claimed.Terms) == 0 {
			break
		}
		result.BatchesProcessed++
		result.TermsClaimed += int32(len(claimed.Terms))
		result.RequestCharsClaimed += claimed.EstimatedChars

		var translated TranslateBrregTranslationWorksetBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, translateBrregTranslationWorksetBatchActivity, TranslateBrregTranslationWorksetBatchInput{
			BatchID:       claimed.BatchID,
			Terms:         claimed.Terms,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
		}).Get(ctx, &translated); err != nil {
			return result, errors.Wrap(err, "translate brreg translation workset batch")
		}

		var saved SaveBrregTranslationWorksetBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, saveBrregTranslationWorksetBatchActivity, SaveBrregTranslationWorksetBatchInput{
			Path:          result.WorksetPath,
			BatchID:       claimed.BatchID,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
			Results:       translated.Results,
		}).Get(ctx, &saved); err != nil {
			return result, errors.Wrap(err, "save brreg translation workset batch")
		}
		result.TermsSucceeded += saved.TermsSucceeded
		result.TermsFailed += saved.TermsFailed

		if input.MaxBatches > 0 && result.BatchesProcessed > 0 && int(result.BatchesProcessed)%input.MaxBatches == 0 {
			input.CarriedStatusRowsInserted = result.StatusRowsInserted
			input.CarriedCompaniesClaimed = result.CompaniesClaimed
			input.CarriedFieldsSeen = result.FieldsSeen
			input.CarriedFieldsApplied = result.FieldsApplied
			input.CarriedRequestChars = result.RequestCharsClaimed
			input.CarriedTermsClaimed = result.TermsClaimed
			input.CarriedTermsSucceeded = result.TermsSucceeded
			input.CarriedTermsFailed = result.TermsFailed
			input.CarriedBatchesProcessed = result.BatchesProcessed
			return result, temporalworkflow.NewContinueAsNewError(ctx, TranslateBrregSourceCompanies, input)
		}
	}

	var applied ApplyBrregTranslationWorksetResult
	if err := temporalworkflow.ExecuteActivity(ctx, applyBrregTranslationWorksetActivity, ApplyBrregTranslationWorksetInput{
		Path:          result.WorksetPath,
		PromptVersion: input.PromptVersion,
	}).Get(ctx, &applied); err != nil {
		return result, errors.Wrap(err, "apply brreg translation workset")
	}
	result.TermsSaved = applied.TermsSaved
	result.FieldsApplied = applied.BindingsApplied
	result.RemainingFields = result.FieldsSeen - result.FieldsApplied
	if result.RemainingFields < 0 {
		result.RemainingFields = 0
	}
	if result.TermsClaimed == 0 && result.TermsSucceeded == 0 && result.TermsFailed == 0 {
		result.Status = "succeeded"
		result.CompaniesSucceeded = result.CompaniesClaimed
		return result, nil
	}
	if result.TermsFailed > 0 && result.TermsSucceeded == 0 {
		result.Status = "failed"
		return result, temporal.NewNonRetryableApplicationError("all company translation terms failed", "BRREG_ALL_COMPANY_TRANSLATION_TERMS_FAILED", nil)
	}
	if result.TermsFailed > 0 || result.RemainingFields > 0 {
		result.Status = "partial"
		return result, nil
	}
	result.Status = "succeeded"
	result.CompaniesSucceeded = result.CompaniesClaimed
	return result, nil
}

func normalizeTranslateBrregSourceCompaniesInput(input TranslateBrregSourceCompaniesInput) TranslateBrregSourceCompaniesInput {
	if input.BatchSize <= 0 {
		input.BatchSize = defaultCompanyTranslationBatchSize
	}
	input.ClaimMode = strings.ToLower(strings.TrimSpace(input.ClaimMode))
	if input.ClaimMode == "" {
		if input.AllRecords {
			input.ClaimMode = actions.CompanyTranslationClaimModeAuto
		} else {
			input.ClaimMode = actions.CompanyTranslationClaimModeFixed
		}
	}
	if input.ClaimMode != actions.CompanyTranslationClaimModeAuto &&
		input.ClaimMode != actions.CompanyTranslationClaimModeFixed {
		input.ClaimMode = actions.CompanyTranslationClaimModeFixed
	}
	if input.MaxRequestChars <= 0 {
		input.MaxRequestChars = defaultCompanyTranslationMaxRequestChars
	}
	if input.MaxTerms <= 0 {
		input.MaxTerms = defaultCompanyTranslationMaxTerms
	}
	if input.MaxCompaniesPerBatch <= 0 {
		input.MaxCompaniesPerBatch = defaultCompanyTranslationMaxCompanies
	}
	if input.MaxBatches <= 0 {
		input.MaxBatches = defaultCompanyTranslationMaxBatches
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
	input.WorksetPath = strings.TrimSpace(input.WorksetPath)
	return input
}

func defaultBrregTranslationWorksetPath(workflowID string) string {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		workflowID = "brreg-company-translation"
	}
	workflowID = strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(workflowID)
	return filepath.Join("/tmp/corpscout/brreg-translation", workflowID+".sqlite")
}

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

	companyTranslationClaimModeAuto  = "auto"
	companyTranslationClaimModeFixed = "fixed"
)

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
	AllRecords           bool              `json:"all_records,omitempty"`
	IDs                  []string          `json:"ids,omitempty"`
	Filters              map[string]string `json:"filters,omitempty"`
	Limit                int               `json:"limit,omitempty"`
	BatchSize            int               `json:"batch_size,omitempty"`
	ClaimMode            string            `json:"claim_mode,omitempty"`
	MaxRequestChars      int               `json:"max_request_chars,omitempty"`
	MaxTerms             int               `json:"max_terms,omitempty"`
	MaxCompaniesPerBatch int               `json:"max_companies_per_batch,omitempty"`
	MaxBatches           int               `json:"max_batches,omitempty"`
	MaxParallelTasks     int               `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds         int               `json:"lease_seconds,omitempty"`
	MaxAttempts          int               `json:"max_attempts,omitempty"`
	Provider             string            `json:"provider,omitempty"`
	Model                string            `json:"model,omitempty"`
	PromptVersion        string            `json:"prompt_version,omitempty"`
	Trigger              string            `json:"trigger,omitempty"`
	WorksetPath          string            `json:"workset_path,omitempty"`
	WorksetPrepared      bool              `json:"workset_prepared,omitempty"`
	WorksetCompanies     int32             `json:"workset_companies,omitempty"`

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

type inFlightTranslationWorksetBatch struct {
	Ordinal int32
	Claimed ClaimBrregTranslationWorksetBatchResult
	Future  temporalworkflow.Future
}

func TranslateBrregSourceCompanies(
	ctx temporalworkflow.Context,
	input TranslateBrregSourceCompaniesInput,
) (TranslateBrregSourceCompaniesResult, error) {
	input = normalizeTranslateBrregSourceCompaniesInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(input.LeaseSeconds) * time.Second,
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
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
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
			companyLimit = int32(input.MaxCompaniesPerBatch)
		} else if input.Limit > 0 {
			companyLimit = int32(input.Limit)
		}
		var built BuildBrregTranslationWorksetResult
		if err := temporalworkflow.ExecuteActivity(ctx, buildBrregTranslationWorksetActivity, BuildBrregTranslationWorksetInput{
			Path:          result.WorksetPath,
			PromptVersion: input.PromptVersion,
			IDs:           input.IDs,
			Filters:       input.Filters,
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
		input.WorksetCompanies = built.CompaniesExported
		input.CarriedFieldsSeen = result.FieldsSeen
		input.CarriedCompaniesClaimed = result.CompaniesClaimed
	}

	current, err := claimAndStartTranslationWorksetBatch(ctx, input, result.WorksetPath, &result)
	if err != nil {
		return result, err
	}
	for current != nil {
		var translated TranslateBrregTranslationWorksetBatchResult
		if err := current.Future.Get(ctx, &translated); err != nil {
			return result, errors.Wrap(err, "translate brreg translation workset batch")
		}

		var next *inFlightTranslationWorksetBatch
		if !shouldContinueAsNewAfterBatch(input, current.Ordinal) {
			next, err = claimAndStartTranslationWorksetBatch(ctx, input, result.WorksetPath, &result)
			if err != nil {
				return result, err
			}
		}

		var saved SaveBrregTranslationWorksetBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, saveBrregTranslationWorksetBatchActivity, SaveBrregTranslationWorksetBatchInput{
			Path:          result.WorksetPath,
			BatchID:       current.Claimed.BatchID,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
			Results:       translated.Results,
		}).Get(ctx, &saved); err != nil {
			return result, errors.Wrap(err, "save brreg translation workset batch")
		}
		result.TermsSucceeded += saved.TermsSucceeded
		result.TermsFailed += saved.TermsFailed

		if shouldContinueAsNewAfterBatch(input, current.Ordinal) {
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
		current = next
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
	if input.AllRecords &&
		applied.BindingsApplied > 0 &&
		input.WorksetCompanies >= int32(input.MaxCompaniesPerBatch) {
		input.WorksetPrepared = false
		input.WorksetPath = ""
		input.WorksetCompanies = 0
		input.CarriedStatusRowsInserted = result.StatusRowsInserted
		input.CarriedCompaniesClaimed = result.CompaniesClaimed
		input.CarriedFieldsSeen = result.FieldsSeen
		input.CarriedFieldsApplied = result.FieldsApplied
		input.CarriedRequestChars = result.RequestCharsClaimed
		input.CarriedTermsClaimed = result.TermsClaimed
		input.CarriedTermsSucceeded = result.TermsSucceeded
		input.CarriedTermsFailed = result.TermsFailed
		input.CarriedBatchesProcessed = result.BatchesProcessed
		logger.Debug("brreg company translation workflow continuing for next all-records chunk",
			"companies_claimed", result.CompaniesClaimed,
			"fields_seen", result.FieldsSeen,
			"fields_applied", result.FieldsApplied,
			"terms_succeeded", result.TermsSucceeded,
			"terms_failed", result.TermsFailed,
			"max_companies_per_batch", input.MaxCompaniesPerBatch,
		)
		return result, temporalworkflow.NewContinueAsNewError(ctx, TranslateBrregSourceCompanies, input)
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

func claimAndStartTranslationWorksetBatch(
	ctx temporalworkflow.Context,
	input TranslateBrregSourceCompaniesInput,
	worksetPath string,
	result *TranslateBrregSourceCompaniesResult,
) (*inFlightTranslationWorksetBatch, error) {
	var claimed ClaimBrregTranslationWorksetBatchResult
	if err := temporalworkflow.ExecuteActivity(ctx, claimBrregTranslationWorksetBatchActivity, ClaimBrregTranslationWorksetBatchInput{
		Path:            worksetPath,
		MaxRequestChars: int32(input.MaxRequestChars),
		MaxTerms:        int32(input.MaxTerms),
		MaxAttempts:     int32(input.MaxAttempts),
	}).Get(ctx, &claimed); err != nil {
		return nil, errors.Wrap(err, "claim brreg translation workset batch")
	}
	if claimed.Status == "drained" || len(claimed.Terms) == 0 {
		return nil, nil
	}
	result.BatchesProcessed++
	result.TermsClaimed += int32(len(claimed.Terms))
	result.RequestCharsClaimed += claimed.EstimatedChars
	return &inFlightTranslationWorksetBatch{
		Ordinal: result.BatchesProcessed,
		Claimed: claimed,
		Future: temporalworkflow.ExecuteActivity(ctx, translateBrregTranslationWorksetBatchActivity, TranslateBrregTranslationWorksetBatchInput{
			BatchID:       claimed.BatchID,
			Terms:         claimed.Terms,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
		}),
	}, nil
}

func shouldContinueAsNewAfterBatch(input TranslateBrregSourceCompaniesInput, batchOrdinal int32) bool {
	return input.MaxBatches > 0 && batchOrdinal > 0 && int(batchOrdinal)%input.MaxBatches == 0
}

func normalizeTranslateBrregSourceCompaniesInput(input TranslateBrregSourceCompaniesInput) TranslateBrregSourceCompaniesInput {
	if input.BatchSize <= 0 {
		input.BatchSize = defaultCompanyTranslationBatchSize
	}
	input.ClaimMode = strings.ToLower(strings.TrimSpace(input.ClaimMode))
	if input.ClaimMode == "" {
		if input.AllRecords {
			input.ClaimMode = companyTranslationClaimModeAuto
		} else {
			input.ClaimMode = companyTranslationClaimModeFixed
		}
	}
	if input.ClaimMode != companyTranslationClaimModeAuto &&
		input.ClaimMode != companyTranslationClaimModeFixed {
		input.ClaimMode = companyTranslationClaimModeFixed
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
	input.IDs = compactTextValues(input.IDs)
	input.Filters = compactTextFilters(input.Filters)
	return input
}

func compactTextValues(values []string) []string {
	compact := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			compact = append(compact, value)
		}
	}
	return compact
}

func compactTextFilters(filters map[string]string) map[string]string {
	if len(filters) == 0 {
		return nil
	}
	compact := make(map[string]string, len(filters))
	for key, value := range filters {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key != "" && value != "" {
			compact[key] = value
		}
	}
	if len(compact) == 0 {
		return nil
	}
	return compact
}

func defaultBrregTranslationWorksetPath(workflowID string) string {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		workflowID = "brreg-company-translation"
	}
	workflowID = strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(workflowID)
	return filepath.Join("/var/lib/corpscout/worksets/brreg-translation", workflowID+".sqlite")
}

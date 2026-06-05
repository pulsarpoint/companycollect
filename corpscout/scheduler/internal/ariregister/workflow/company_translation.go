package workflow

import (
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/actions"
)

const (
	TranslateAriregisterSourceCompaniesTaskQueue    = "ariregister-company-translation"
	TranslateAriregisterSourceCompaniesWorkflowName = "TranslateAriregisterSourceCompanies"

	buildAriregisterTranslationWorksetActivity          = "BuildAriregisterTranslationWorkset"
	claimAriregisterTranslationWorksetBatchActivity     = "ClaimAriregisterTranslationWorksetBatch"
	translateAriregisterTranslationWorksetBatchActivity = "TranslateAriregisterTranslationWorksetBatch"
	saveAriregisterTranslationWorksetBatchActivity      = "SaveAriregisterTranslationWorksetBatch"
	applyAriregisterTranslationWorksetActivity          = "ApplyAriregisterTranslationWorkset"

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

type BuildAriregisterTranslationWorksetInput = actions.BuildAriregisterTranslationWorksetInput
type BuildAriregisterTranslationWorksetResult = actions.BuildAriregisterTranslationWorksetResult
type ClaimAriregisterTranslationWorksetBatchInput = actions.ClaimAriregisterTranslationWorksetBatchInput
type ClaimAriregisterTranslationWorksetBatchResult = actions.ClaimAriregisterTranslationWorksetBatchResult
type TranslationWorksetTerm = actions.TranslationWorksetTerm
type TranslateAriregisterTranslationWorksetBatchInput = actions.TranslateAriregisterTranslationWorksetBatchInput
type TranslateAriregisterTranslationWorksetBatchResult = actions.TranslateAriregisterTranslationWorksetBatchResult
type TranslationWorksetTermResult = actions.TranslationWorksetTermResult
type SaveAriregisterTranslationWorksetBatchInput = actions.SaveAriregisterTranslationWorksetBatchInput
type SaveAriregisterTranslationWorksetBatchResult = actions.SaveAriregisterTranslationWorksetBatchResult
type ApplyAriregisterTranslationWorksetInput = actions.ApplyAriregisterTranslationWorksetInput
type ApplyAriregisterTranslationWorksetResult = actions.ApplyAriregisterTranslationWorksetResult

type TranslateAriregisterSourceCompaniesInput struct {
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

type TranslateAriregisterSourceCompaniesResult struct {
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
	Claimed ClaimAriregisterTranslationWorksetBatchResult
	Future  temporalworkflow.Future
}

func TranslateAriregisterSourceCompanies(
	ctx temporalworkflow.Context,
	input TranslateAriregisterSourceCompaniesInput,
) (TranslateAriregisterSourceCompaniesResult, error) {
	input = normalizeTranslateAriregisterSourceCompaniesInput(input)
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
	logger.Debug("ariregister company translation workflow started",
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

	result := TranslateAriregisterSourceCompaniesResult{
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
		result.WorksetPath = defaultAriregisterTranslationWorksetPath(workflowInfo.WorkflowExecution.ID)
	}
	input.WorksetPath = result.WorksetPath

	if !input.WorksetPrepared {
		companyLimit := int32(input.BatchSize)
		if input.AllRecords {
			companyLimit = int32(input.MaxCompaniesPerBatch)
		} else if input.Limit > 0 {
			companyLimit = int32(input.Limit)
		}
		var built BuildAriregisterTranslationWorksetResult
		if err := temporalworkflow.ExecuteActivity(ctx, buildAriregisterTranslationWorksetActivity, BuildAriregisterTranslationWorksetInput{
			Path:          result.WorksetPath,
			PromptVersion: input.PromptVersion,
			IDs:           input.IDs,
			Filters:       input.Filters,
			CompanyLimit:  companyLimit,
		}).Get(ctx, &built); err != nil {
			return result, errors.Wrap(err, "build ariregister translation workset")
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
		var translated TranslateAriregisterTranslationWorksetBatchResult
		if err := current.Future.Get(ctx, &translated); err != nil {
			return result, errors.Wrap(err, "translate ariregister translation workset batch")
		}

		var next *inFlightTranslationWorksetBatch
		if !shouldContinueAsNewAfterBatch(input, current.Ordinal) {
			next, err = claimAndStartTranslationWorksetBatch(ctx, input, result.WorksetPath, &result)
			if err != nil {
				return result, err
			}
		}

		var saved SaveAriregisterTranslationWorksetBatchResult
		if err := temporalworkflow.ExecuteActivity(ctx, saveAriregisterTranslationWorksetBatchActivity, SaveAriregisterTranslationWorksetBatchInput{
			Path:          result.WorksetPath,
			BatchID:       current.Claimed.BatchID,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
			Results:       translated.Results,
		}).Get(ctx, &saved); err != nil {
			return result, errors.Wrap(err, "save ariregister translation workset batch")
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
			return result, temporalworkflow.NewContinueAsNewError(ctx, TranslateAriregisterSourceCompanies, input)
		}
		current = next
	}

	var applied ApplyAriregisterTranslationWorksetResult
	if err := temporalworkflow.ExecuteActivity(ctx, applyAriregisterTranslationWorksetActivity, ApplyAriregisterTranslationWorksetInput{
		Path:          result.WorksetPath,
		PromptVersion: input.PromptVersion,
	}).Get(ctx, &applied); err != nil {
		return result, errors.Wrap(err, "apply ariregister translation workset")
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
		logger.Debug("ariregister company translation workflow continuing for next all-records chunk",
			"companies_claimed", result.CompaniesClaimed,
			"fields_seen", result.FieldsSeen,
			"fields_applied", result.FieldsApplied,
			"terms_succeeded", result.TermsSucceeded,
			"terms_failed", result.TermsFailed,
			"max_companies_per_batch", input.MaxCompaniesPerBatch,
		)
		return result, temporalworkflow.NewContinueAsNewError(ctx, TranslateAriregisterSourceCompanies, input)
	}
	if result.TermsClaimed == 0 && result.TermsSucceeded == 0 && result.TermsFailed == 0 {
		result.Status = "succeeded"
		result.CompaniesSucceeded = result.CompaniesClaimed
		return result, nil
	}
	if result.TermsFailed > 0 && result.TermsSucceeded == 0 {
		result.Status = "failed"
		return result, temporal.NewNonRetryableApplicationError("all company translation terms failed", "ARIREGISTER_ALL_COMPANY_TRANSLATION_TERMS_FAILED", nil)
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
	input TranslateAriregisterSourceCompaniesInput,
	worksetPath string,
	result *TranslateAriregisterSourceCompaniesResult,
) (*inFlightTranslationWorksetBatch, error) {
	var claimed ClaimAriregisterTranslationWorksetBatchResult
	if err := temporalworkflow.ExecuteActivity(ctx, claimAriregisterTranslationWorksetBatchActivity, ClaimAriregisterTranslationWorksetBatchInput{
		Path:            worksetPath,
		MaxRequestChars: int32(input.MaxRequestChars),
		MaxTerms:        int32(input.MaxTerms),
		MaxAttempts:     int32(input.MaxAttempts),
	}).Get(ctx, &claimed); err != nil {
		return nil, errors.Wrap(err, "claim ariregister translation workset batch")
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
		Future: temporalworkflow.ExecuteActivity(ctx, translateAriregisterTranslationWorksetBatchActivity, TranslateAriregisterTranslationWorksetBatchInput{
			BatchID:       claimed.BatchID,
			Terms:         claimed.Terms,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
		}),
	}, nil
}

func shouldContinueAsNewAfterBatch(input TranslateAriregisterSourceCompaniesInput, batchOrdinal int32) bool {
	return input.MaxBatches > 0 && batchOrdinal > 0 && int(batchOrdinal)%input.MaxBatches == 0
}

func normalizeTranslateAriregisterSourceCompaniesInput(input TranslateAriregisterSourceCompaniesInput) TranslateAriregisterSourceCompaniesInput {
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
	return input
}

func defaultAriregisterTranslationWorksetPath(workflowID string) string {
	workflowID = strings.TrimSpace(workflowID)
	if workflowID == "" {
		workflowID = "manual"
	}
	return filepath.Join("/var/lib/corpscout/worksets", "ariregister-translation-"+workflowID+".sqlite")
}

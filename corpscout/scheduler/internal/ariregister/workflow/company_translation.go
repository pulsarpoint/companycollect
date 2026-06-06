package workflow

import (
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

	buildAriregisterTranslationWorksetActivity = "BuildAriregisterTranslationWorkset"

	defaultCompanyTranslationBatchSize        = 10
	defaultCompanyTranslationMaxParallelTasks = 2
	defaultCompanyTranslationLeaseSeconds     = 1800
	defaultCompanyTranslationMaxAttempts      = 8
	defaultCompanyTranslationBatchDelay       = 60
	defaultCompanyTranslationMaxRequestChars  = 6000
	defaultCompanyTranslationMaxTerms         = 25
	defaultCompanyTranslationMaxBatches       = 200
	defaultCompanyTranslationPromptVersion    = "v1"
	defaultCompanyTranslationTrigger          = "manual"
	companyTranslationClaimModeAuto           = "auto"
	companyTranslationClaimModeFixed          = "fixed"
)

type BuildAriregisterTranslationWorksetInput = actions.BuildAriregisterTranslationWorksetInput
type BuildAriregisterTranslationWorksetResult = actions.BuildAriregisterTranslationWorksetResult

type TranslateAriregisterSourceCompaniesInput struct {
	AllRecords        bool              `json:"all_records,omitempty"`
	IDs               []string          `json:"ids,omitempty"`
	Filters           map[string]string `json:"filters,omitempty"`
	Limit             int               `json:"limit,omitempty"`
	BatchSize         int               `json:"batch_size,omitempty"`
	ClaimMode         string            `json:"claim_mode,omitempty"`
	MaxRequestChars   int               `json:"max_request_chars,omitempty"`
	MaxTerms          int               `json:"max_terms,omitempty"`
	MaxBatches        int               `json:"max_batches,omitempty"`
	MaxParallelTasks  int               `json:"max_parallel_tasks,omitempty"`
	LeaseSeconds      int               `json:"lease_seconds,omitempty"`
	MaxAttempts       int               `json:"max_attempts,omitempty"`
	BatchDelaySeconds int               `json:"batch_delay_seconds,omitempty"`
	Provider          string            `json:"provider,omitempty"`
	Model             string            `json:"model,omitempty"`
	PromptVersion     string            `json:"prompt_version,omitempty"`
	Trigger           string            `json:"trigger,omitempty"`
	WorksetPath       string            `json:"workset_path,omitempty"`
	WorksetPrepared   bool              `json:"workset_prepared,omitempty"`
	WorksetCompanies  int32             `json:"workset_companies,omitempty"`
	QueuePrepared     bool              `json:"queue_prepared,omitempty"`
	QueueCompanies    int32             `json:"queue_companies,omitempty"`

	CarriedStatusRowsInserted int32 `json:"carried_status_rows_inserted,omitempty"`
	CarriedCompaniesClaimed   int32 `json:"carried_companies_claimed,omitempty"`
	CarriedCompaniesSucceeded int32 `json:"carried_companies_succeeded,omitempty"`
	CarriedFieldsSeen         int32 `json:"carried_fields_seen,omitempty"`
	CarriedFieldsApplied      int32 `json:"carried_fields_applied,omitempty"`
	CarriedRequestChars       int32 `json:"carried_request_chars,omitempty"`
	CarriedTermsClaimed       int32 `json:"carried_terms_claimed,omitempty"`
	CarriedTermsSucceeded     int32 `json:"carried_terms_succeeded,omitempty"`
	CarriedTermsFailed        int32 `json:"carried_terms_failed,omitempty"`
	CarriedTermsSaved         int32 `json:"carried_terms_saved,omitempty"`
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

func TranslateAriregisterSourceCompanies(
	ctx temporalworkflow.Context,
	input TranslateAriregisterSourceCompaniesInput,
) (TranslateAriregisterSourceCompaniesResult, error) {
	input = normalizeTranslateAriregisterSourceCompaniesInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(input.LeaseSeconds) * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    companyTranslationRetryInitialInterval(input.BatchDelaySeconds),
			BackoffCoefficient: 2,
			MaximumInterval:    10 * time.Minute,
			MaximumAttempts:    int32(input.MaxAttempts),
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
		"max_batches", input.MaxBatches,
		"max_parallel_tasks", input.MaxParallelTasks,
		"lease_seconds", input.LeaseSeconds,
		"max_attempts", input.MaxAttempts,
		"batch_delay_seconds", input.BatchDelaySeconds,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
		"trigger", input.Trigger,
		"queue_prepared", input.QueuePrepared,
	)

	result := TranslateAriregisterSourceCompaniesResult{
		Status:              "running",
		StatusRowsInserted:  input.CarriedStatusRowsInserted,
		CompaniesClaimed:    input.CarriedCompaniesClaimed,
		CompaniesSucceeded:  input.CarriedCompaniesSucceeded,
		FieldsSeen:          input.CarriedFieldsSeen,
		FieldsApplied:       input.CarriedFieldsApplied,
		RequestCharsClaimed: input.CarriedRequestChars,
		TermsClaimed:        input.CarriedTermsClaimed,
		TermsSucceeded:      input.CarriedTermsSucceeded,
		TermsFailed:         input.CarriedTermsFailed,
		TermsSaved:          input.CarriedTermsSaved,
		BatchesProcessed:    input.CarriedBatchesProcessed,
		WorksetPath:         input.WorksetPath,
	}

	if !input.QueuePrepared {
		companyLimit := int32(input.BatchSize)
		if input.AllRecords {
			companyLimit = 0
		} else if input.Limit > 0 {
			companyLimit = int32(input.Limit)
		}
		var prepared BuildAriregisterTranslationWorksetResult
		if err := temporalworkflow.ExecuteActivity(ctx, buildAriregisterTranslationWorksetActivity, BuildAriregisterTranslationWorksetInput{
			Path:          result.WorksetPath,
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
			IDs:           input.IDs,
			Filters:       input.Filters,
			CompanyLimit:  companyLimit,
		}).Get(ctx, &prepared); err != nil {
			return result, errors.Wrap(err, "prepare ariregister translation queue")
		}
		result.FieldsSeen = prepared.FieldsExported
		result.StatusRowsInserted += prepared.CompaniesQueued
		if prepared.FieldsExported == 0 {
			result.Status = "drained"
			return result, nil
		}
		input.QueuePrepared = true
		input.QueueCompanies = prepared.CompaniesExported
		input.WorksetPrepared = true
		input.WorksetCompanies = prepared.CompaniesExported
		input.CarriedStatusRowsInserted = result.StatusRowsInserted
		input.CarriedFieldsSeen = result.FieldsSeen
		result.Status = "queued"
		return result, nil
	}

	result.Status = "queued"
	return result, nil
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
	if input.BatchDelaySeconds <= 0 {
		input.BatchDelaySeconds = defaultCompanyTranslationBatchDelay
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultCompanyTranslationPromptVersion
	}
	if input.Trigger == "" {
		input.Trigger = defaultCompanyTranslationTrigger
	}
	input.WorksetPath = strings.TrimSpace(input.WorksetPath)
	input.QueuePrepared = input.QueuePrepared || input.WorksetPrepared
	if input.QueueCompanies == 0 {
		input.QueueCompanies = input.WorksetCompanies
	}
	input.IDs = compactTextValues(input.IDs)
	input.Filters = compactTextFilters(input.Filters)
	return input
}

func companyTranslationRetryInitialInterval(batchDelaySeconds int) time.Duration {
	interval := time.Duration(batchDelaySeconds) * time.Second
	if interval < 30*time.Second {
		return 30 * time.Second
	}
	if interval > 10*time.Minute {
		return 10 * time.Minute
	}
	return interval
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

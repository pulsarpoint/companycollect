package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	TranslateBrregSourceTermsTaskQueue    = "brreg-term-translation"
	TranslateBrregSourceTermsWorkflowName = "TranslateBrregSourceTerms"

	ensureBrregTranslationTermsActivity      = "EnsureBrregTranslationTerms"
	publishBrregTranslationTermsActivity     = "PublishBrregTranslationTerms"
	applyBrregCachedTranslationTermsActivity = "ApplyBrregCachedTranslationTerms"

	defaultTermTranslationLimit        = 10000
	defaultTermTranslationBatchSize    = 100
	defaultTermTranslationMaxAttempts  = 3
	defaultTermTranslationMaxLoops     = 100
	maxTermTranslationLoops            = 100
	defaultTermTranslationProvider     = "default"
	defaultTermTranslationPrompt       = "v1"
	defaultTermTranslationTrigger      = "manual"
	defaultTermTranslationLoopInterval = 5 * time.Second
)

type EnsureBrregTranslationTermsInput = actions.EnsureBrregTranslationTermsInput
type EnsureBrregTranslationTermsResult = actions.EnsureBrregTranslationTermsResult
type PublishBrregTranslationTermsInput = actions.PublishBrregTranslationTermsInput
type PublishBrregTranslationTermsResult = actions.PublishBrregTranslationTermsResult
type ApplyBrregCachedTranslationTermsInput = actions.ApplyBrregCachedTranslationTermsInput
type ApplyBrregCachedTranslationTermsResult = actions.ApplyBrregCachedTranslationTermsResult

type TranslateBrregSourceTermsInput struct {
	AllRecords    bool   `json:"all_records,omitempty"`
	Limit         int    `json:"limit,omitempty"`
	TermBatchSize int    `json:"term_batch_size,omitempty"`
	MaxAttempts   int    `json:"max_attempts,omitempty"`
	MaxLoops      int    `json:"max_loops,omitempty"`
	Provider      string `json:"provider,omitempty"`
	Model         string `json:"model,omitempty"`
	PromptVersion string `json:"prompt_version,omitempty"`
	Trigger       string `json:"trigger,omitempty"`

	TermsInsertedCarry  int32 `json:"terms_inserted_carry,omitempty"`
	TermsPublishedCarry int32 `json:"terms_published_carry,omitempty"`
	FieldsAppliedCarry  int32 `json:"fields_applied_carry,omitempty"`
	LoopsCarry          int32 `json:"loops_carry,omitempty"`
}

type TranslateBrregSourceTermsResult struct {
	Status         string `json:"status"`
	TermsInserted  int32  `json:"terms_inserted"`
	TermsPublished int32  `json:"terms_published"`
	FieldsApplied  int32  `json:"fields_applied"`
	Loops          int32  `json:"loops"`
}

func TranslateBrregSourceTerms(
	ctx temporalworkflow.Context,
	input TranslateBrregSourceTermsInput,
) (TranslateBrregSourceTermsResult, error) {
	input = normalizeTranslateBrregSourceTermsInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    defaultTermTranslationMaxAttempts,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("brreg term translation workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"all_records", input.AllRecords,
		"limit", input.Limit,
		"term_batch_size", input.TermBatchSize,
		"max_attempts", input.MaxAttempts,
		"max_loops", input.MaxLoops,
		"provider", input.Provider,
		"model", input.Model,
		"prompt_version", input.PromptVersion,
		"trigger", input.Trigger,
	)

	result := TranslateBrregSourceTermsResult{
		Status:         "running",
		TermsInserted:  input.TermsInsertedCarry,
		TermsPublished: input.TermsPublishedCarry,
		FieldsApplied:  input.FieldsAppliedCarry,
		Loops:          input.LoopsCarry,
	}
	for loop := 0; loop < input.MaxLoops; loop++ {
		result.Loops++

		var applied ApplyBrregCachedTranslationTermsResult
		if err := temporalworkflow.ExecuteActivity(ctx, applyBrregCachedTranslationTermsActivity, ApplyBrregCachedTranslationTermsInput{
			PromptVersion: input.PromptVersion,
			Limit:         int32(input.Limit),
		}).Get(ctx, &applied); err != nil {
			return result, errors.Wrap(err, "apply brreg cached translation terms")
		}
		result.FieldsApplied += applied.FieldsApplied

		var ensured EnsureBrregTranslationTermsResult
		if err := temporalworkflow.ExecuteActivity(ctx, ensureBrregTranslationTermsActivity, EnsureBrregTranslationTermsInput{
			TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
			Provider:           input.Provider,
			Model:              input.Model,
			PromptVersion:      input.PromptVersion,
			Limit:              int32(input.Limit),
		}).Get(ctx, &ensured); err != nil {
			return result, errors.Wrap(err, "ensure brreg translation terms")
		}
		result.TermsInserted += ensured.TermsInserted

		var published PublishBrregTranslationTermsResult
		if err := temporalworkflow.ExecuteActivity(ctx, publishBrregTranslationTermsActivity, PublishBrregTranslationTermsInput{
			Provider:      input.Provider,
			Model:         input.Model,
			PromptVersion: input.PromptVersion,
			Limit:         int32(input.TermBatchSize),
			MaxAttempts:   int32(input.MaxAttempts),
		}).Get(ctx, &published); err != nil {
			return result, errors.Wrap(err, "publish brreg translation terms")
		}
		result.TermsPublished += published.TermsPublished

		if ensured.TermsInserted == 0 && published.TermsPublished == 0 {
			result.Status = "succeeded"
			return result, nil
		}
		if err := temporalworkflow.Sleep(ctx, defaultTermTranslationLoopInterval); err != nil {
			return result, errors.Wrap(err, "sleep between brreg term translation loops")
		}
	}

	input.TermsInsertedCarry = result.TermsInserted
	input.TermsPublishedCarry = result.TermsPublished
	input.FieldsAppliedCarry = result.FieldsApplied
	input.LoopsCarry = result.Loops
	return result, temporalworkflow.NewContinueAsNewError(ctx, TranslateBrregSourceTerms, input)
}

func normalizeTranslateBrregSourceTermsInput(input TranslateBrregSourceTermsInput) TranslateBrregSourceTermsInput {
	if input.AllRecords {
		input.Limit = 0
	} else if input.Limit <= 0 {
		input.Limit = defaultTermTranslationLimit
	}
	if input.TermBatchSize <= 0 {
		input.TermBatchSize = defaultTermTranslationBatchSize
	}
	if input.MaxAttempts <= 0 {
		input.MaxAttempts = defaultTermTranslationMaxAttempts
	}
	if input.MaxLoops <= 0 {
		input.MaxLoops = defaultTermTranslationMaxLoops
	}
	if input.MaxLoops > maxTermTranslationLoops {
		input.MaxLoops = maxTermTranslationLoops
	}
	if input.Provider == "" {
		input.Provider = defaultTermTranslationProvider
	}
	if input.PromptVersion == "" {
		input.PromptVersion = defaultTermTranslationPrompt
	}
	if input.Trigger == "" {
		input.Trigger = defaultTermTranslationTrigger
	}
	if input.TermsInsertedCarry < 0 {
		input.TermsInsertedCarry = 0
	}
	if input.TermsPublishedCarry < 0 {
		input.TermsPublishedCarry = 0
	}
	if input.FieldsAppliedCarry < 0 {
		input.FieldsAppliedCarry = 0
	}
	if input.LoopsCarry < 0 {
		input.LoopsCarry = 0
	}
	return input
}

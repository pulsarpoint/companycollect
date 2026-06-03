package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	ConvertBrregSourceCapitalToUSDTaskQueue    = "brreg-source-capital-fx"
	ConvertBrregSourceCapitalToUSDWorkflowName = "ConvertBrregSourceCapitalToUSD"

	convertBrregSourceCapitalToUSDActivity = "ConvertBrregSourceCapitalToUSDActivity"
)

type ConvertBrregSourceCapitalToUSDActivityInput = actions.ConvertBrregSourceCapitalToUSDActivityInput
type ConvertBrregSourceCapitalToUSDActivityResult = actions.ConvertBrregSourceCapitalToUSDActivityResult

type ConvertBrregSourceCapitalToUSDInput struct {
	IDs            []string          `json:"ids,omitempty"`
	Filters        map[string]string `json:"filters,omitempty"`
	Limit          int               `json:"limit,omitempty"`
	RateDate       string            `json:"rate_date,omitempty"`
	ForceReprocess bool              `json:"force_reprocess,omitempty"`
	Trigger        string            `json:"trigger,omitempty"`
}

type ConvertBrregSourceCapitalToUSDResult struct {
	Status                         string `json:"status"`
	CapitalSeen                    int32  `json:"capital_seen"`
	CapitalConverted               int32  `json:"capital_converted"`
	CapitalSkippedMissingRate      int32  `json:"capital_skipped_missing_rate"`
	CapitalSkippedAlreadyConverted int32  `json:"capital_skipped_already_converted"`
	RateDate                       string `json:"rate_date"`
}

func ConvertBrregSourceCapitalToUSD(
	ctx temporalworkflow.Context,
	input ConvertBrregSourceCapitalToUSDInput,
) (ConvertBrregSourceCapitalToUSDResult, error) {
	input = normalizeConvertBrregSourceCapitalToUSDInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    1 * time.Minute,
			MaximumAttempts:    3,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("brreg source capital fx workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"rate_date", input.RateDate,
		"force_reprocess", input.ForceReprocess,
		"trigger", input.Trigger,
	)

	var activityResult ConvertBrregSourceCapitalToUSDActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, convertBrregSourceCapitalToUSDActivity, ConvertBrregSourceCapitalToUSDActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		IDs:                input.IDs,
		Filters:            input.Filters,
		Limit:              int32(input.Limit),
		RateDate:           input.RateDate,
		ForceReprocess:     input.ForceReprocess,
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return ConvertBrregSourceCapitalToUSDResult{}, errors.Wrap(err, "convert brreg source capital to usd")
	}
	logger.Debug("brreg source capital fx workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"capital_seen", activityResult.CapitalSeen,
		"capital_converted", activityResult.CapitalConverted,
		"capital_skipped_missing_rate", activityResult.CapitalSkippedMissingRate,
		"capital_skipped_already_converted", activityResult.CapitalSkippedAlreadyConverted,
		"rate_date", activityResult.RateDate,
	)

	return ConvertBrregSourceCapitalToUSDResult{
		Status:                         "succeeded",
		CapitalSeen:                    activityResult.CapitalSeen,
		CapitalConverted:               activityResult.CapitalConverted,
		CapitalSkippedMissingRate:      activityResult.CapitalSkippedMissingRate,
		CapitalSkippedAlreadyConverted: activityResult.CapitalSkippedAlreadyConverted,
		RateDate:                       activityResult.RateDate,
	}, nil
}

func normalizeConvertBrregSourceCapitalToUSDInput(input ConvertBrregSourceCapitalToUSDInput) ConvertBrregSourceCapitalToUSDInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

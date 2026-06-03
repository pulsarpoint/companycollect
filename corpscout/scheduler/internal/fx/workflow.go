package fx

import (
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const (
	SyncWorkflowName = "SyncExchangeRates"
	SyncTaskQueue    = "fx-rate-sync"

	syncExchangeRatesActivity = "SyncExchangeRatesActivity"
)

type SyncExchangeRatesInput struct {
	Provider       string `json:"provider,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type SyncExchangeRatesResult = SyncExchangeRatesActivityResult

func SyncExchangeRates(ctx temporalworkflow.Context, input SyncExchangeRatesInput) (SyncExchangeRatesResult, error) {
	input = normalizeWorkflowInput(input)
	info := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("exchange rate sync workflow started",
		"temporal_workflow_id", info.WorkflowExecution.ID,
		"provider", input.Provider,
		"source_url", input.SourceURL,
		"trigger", input.Trigger,
		"force_reprocess", input.ForceReprocess,
	)

	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	var result SyncExchangeRatesActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, syncExchangeRatesActivity, SyncExchangeRatesActivityInput{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		Provider:           input.Provider,
		SourceURL:          input.SourceURL,
		Trigger:            input.Trigger,
		ForceReprocess:     input.ForceReprocess,
	}).Get(ctx, &result); err != nil {
		return SyncExchangeRatesResult{}, errors.Wrap(err, "sync exchange rates activity")
	}
	logger.Debug("exchange rate sync workflow finished",
		"status", result.Status,
		"sync_run_id", result.SyncRunID,
		"source_file_id", result.SourceFileID,
		"sheet_id", result.SheetID,
		"content_sha256", result.ContentSHA256,
		"rate_date", result.RateDate,
		"currencies_seen", result.CurrenciesSeen,
		"currencies_imported", result.CurrenciesImported,
	)
	return result, nil
}

func normalizeWorkflowInput(input SyncExchangeRatesInput) SyncExchangeRatesInput {
	input.Provider = strings.ToLower(strings.TrimSpace(input.Provider))
	input.SourceURL = strings.TrimSpace(input.SourceURL)
	input.Trigger = strings.TrimSpace(input.Trigger)
	if input.Provider == "" {
		input.Provider = DefaultProvider
	}
	if input.SourceURL == "" {
		input.SourceURL = DefaultDailySourceURL
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

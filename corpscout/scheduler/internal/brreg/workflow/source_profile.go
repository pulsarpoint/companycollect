package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
)

const (
	NormalizeBrregSourceProfilesTaskQueue    = "brreg-source-profile"
	NormalizeBrregSourceProfilesWorkflowName = "NormalizeBrregSourceProfiles"

	normalizeBrregSourceProfilesActivity = "NormalizeBrregSourceProfilesActivity"

	RefreshBrregSourceExplorerTaskQueue    = "brreg-source-explorer-refresh"
	RefreshBrregSourceExplorerWorkflowName = "RefreshBrregSourceExplorer"

	refreshBrregSourceExplorerActivity = "RefreshBrregSourceExplorerActivity"
)

type NormalizeBrregSourceProfilesActivityInput = actions.NormalizeBrregSourceProfilesActivityInput
type NormalizeBrregSourceProfilesActivityResult = actions.NormalizeBrregSourceProfilesActivityResult
type RefreshBrregSourceExplorerActivityInput = actions.RefreshBrregSourceExplorerActivityInput
type RefreshBrregSourceExplorerActivityResult = actions.RefreshBrregSourceExplorerActivityResult

type NormalizeBrregSourceProfilesInput struct {
	IDs     []string          `json:"ids,omitempty"`
	Filters map[string]string `json:"filters,omitempty"`
	Limit   int               `json:"limit,omitempty"`
	Trigger string            `json:"trigger,omitempty"`
}

type NormalizeBrregSourceProfilesResult struct {
	Status             string `json:"status"`
	RecordsSeen        int32  `json:"records_seen"`
	CompaniesUpserted  int32  `json:"companies_upserted"`
	AddressesUpserted  int32  `json:"addresses_upserted"`
	IndustriesUpserted int32  `json:"industries_upserted"`
	WebsitesUpserted   int32  `json:"websites_upserted"`
	DomainsUpserted    int32  `json:"domains_upserted"`
	ContactsUpserted   int32  `json:"contacts_upserted"`
	CapitalUpserted    int32  `json:"capital_upserted"`
}

type RefreshBrregSourceExplorerInput struct {
	Trigger string `json:"trigger,omitempty"`
}

type RefreshBrregSourceExplorerResult struct {
	Status                string  `json:"status"`
	Refreshed             bool    `json:"refreshed"`
	UsedConcurrentRefresh bool    `json:"used_concurrent_refresh"`
	SourceEntries         int64   `json:"source_entries"`
	LatestSourceUpdatedAt *string `json:"latest_source_updated_at,omitempty"`
}

func NormalizeBrregSourceProfiles(
	ctx temporalworkflow.Context,
	input NormalizeBrregSourceProfilesInput,
) (NormalizeBrregSourceProfilesResult, error) {
	input = normalizeBrregSourceProfilesInput(input)
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
	logger.Debug("brreg source profile normalization workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"trigger", input.Trigger,
	)

	var activityResult NormalizeBrregSourceProfilesActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, normalizeBrregSourceProfilesActivity, NormalizeBrregSourceProfilesActivityInput{
		IDs:                input.IDs,
		Filters:            input.Filters,
		Limit:              int32(input.Limit),
		Trigger:            input.Trigger,
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
	}).Get(ctx, &activityResult); err != nil {
		return NormalizeBrregSourceProfilesResult{}, errors.Wrap(err, "normalize brreg source profiles")
	}
	logger.Debug("brreg source profile normalization workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"records_seen", activityResult.RecordsSeen,
		"companies_upserted", activityResult.CompaniesUpserted,
	)

	return NormalizeBrregSourceProfilesResult{
		Status:             "succeeded",
		RecordsSeen:        activityResult.RecordsSeen,
		CompaniesUpserted:  activityResult.CompaniesUpserted,
		AddressesUpserted:  activityResult.AddressesUpserted,
		IndustriesUpserted: activityResult.IndustriesUpserted,
		WebsitesUpserted:   activityResult.WebsitesUpserted,
		DomainsUpserted:    activityResult.DomainsUpserted,
		ContactsUpserted:   activityResult.ContactsUpserted,
		CapitalUpserted:    activityResult.CapitalUpserted,
	}, nil
}

func normalizeBrregSourceProfilesInput(input NormalizeBrregSourceProfilesInput) NormalizeBrregSourceProfilesInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

func RefreshBrregSourceExplorer(
	ctx temporalworkflow.Context,
	input RefreshBrregSourceExplorerInput,
) (RefreshBrregSourceExplorerResult, error) {
	input = normalizeRefreshBrregSourceExplorerInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("brreg source explorer refresh workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"trigger", input.Trigger,
	)

	var activityResult RefreshBrregSourceExplorerActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, refreshBrregSourceExplorerActivity, RefreshBrregSourceExplorerActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return RefreshBrregSourceExplorerResult{}, errors.Wrap(err, "refresh brreg source explorer")
	}
	logger.Debug("brreg source explorer refresh workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"source_entries", activityResult.SourceEntries,
		"used_concurrent_refresh", activityResult.UsedConcurrentRefresh,
	)

	return RefreshBrregSourceExplorerResult{
		Status:                "succeeded",
		Refreshed:             activityResult.Refreshed,
		UsedConcurrentRefresh: activityResult.UsedConcurrentRefresh,
		SourceEntries:         activityResult.SourceEntries,
		LatestSourceUpdatedAt: activityResult.LatestSourceUpdatedAt,
	}, nil
}

func normalizeRefreshBrregSourceExplorerInput(input RefreshBrregSourceExplorerInput) RefreshBrregSourceExplorerInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

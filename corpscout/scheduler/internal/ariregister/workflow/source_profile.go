package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/actions"
)

const (
	defaultSourceProfileChunkSize = 5000

	NormalizeAriregisterSourceProfilesTaskQueue            = "ariregister-source-profile"
	NormalizeAriregisterSourceProfilesWithCopyWorkflowName = "NormalizeAriregisterSourceProfilesWithCopy"

	NormalizeAriregisterSourceProfilesWithCopyActivity = "NormalizeAriregisterSourceProfilesWithCopyActivity"

	RefreshAriregisterSourceExplorerTaskQueue    = "ariregister-source-explorer-refresh"
	RefreshAriregisterSourceExplorerWorkflowName = "RefreshAriregisterSourceExplorer"

	RefreshAriregisterSourceExplorerActivity = "RefreshAriregisterSourceExplorerActivity"
)

type NormalizeAriregisterSourceProfilesActivityInput = actions.NormalizeAriregisterSourceProfilesActivityInput
type NormalizeAriregisterSourceProfilesActivityResult = actions.NormalizeAriregisterSourceProfilesActivityResult
type RefreshAriregisterSourceExplorerActivityInput = actions.RefreshAriregisterSourceExplorerActivityInput
type RefreshAriregisterSourceExplorerActivityResult = actions.RefreshAriregisterSourceExplorerActivityResult

type NormalizeAriregisterSourceProfilesInput struct {
	IDs       []string          `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	BatchSize int               `json:"batch_size,omitempty"`
	Trigger   string            `json:"trigger,omitempty"`
}

type NormalizeAriregisterSourceProfilesResult struct {
	Status                       string `json:"status"`
	ChunksProcessed              int32  `json:"chunks_processed"`
	RecordsSeen                  int32  `json:"records_seen"`
	CompaniesUpserted            int32  `json:"companies_upserted"`
	CompanyNamesUpserted         int32  `json:"company_names_upserted"`
	CompanyStatusesUpserted      int32  `json:"company_statuses_upserted"`
	LegalFormsUpserted           int32  `json:"legal_forms_upserted"`
	AddressesUpserted            int32  `json:"addresses_upserted"`
	ContactsUpserted             int32  `json:"contacts_upserted"`
	WebsitesUpserted             int32  `json:"websites_upserted"`
	DomainsUpserted              int32  `json:"domains_upserted"`
	IndustriesUpserted           int32  `json:"industries_upserted"`
	CapitalUpserted              int32  `json:"capital_upserted"`
	FinancialYearPeriodsUpserted int32  `json:"financial_year_periods_upserted"`
	AnnualReportsUpserted        int32  `json:"annual_reports_upserted"`
	ArticlesUpserted             int32  `json:"articles_upserted"`
	RegistryNotesUpserted        int32  `json:"registry_notes_upserted"`
}

type RefreshAriregisterSourceExplorerInput struct {
	Trigger string `json:"trigger,omitempty"`
}

type RefreshAriregisterSourceExplorerResult struct {
	Status                string  `json:"status"`
	Refreshed             bool    `json:"refreshed"`
	UsedConcurrentRefresh bool    `json:"used_concurrent_refresh"`
	SourceEntries         int64   `json:"source_entries"`
	LatestSourceUpdatedAt *string `json:"latest_source_updated_at,omitempty"`
}

func NormalizeAriregisterSourceProfilesWithCopy(
	ctx temporalworkflow.Context,
	input NormalizeAriregisterSourceProfilesInput,
) (NormalizeAriregisterSourceProfilesResult, error) {
	if input.Limit < 0 {
		return NormalizeAriregisterSourceProfilesResult{}, errors.New("limit cannot be negative")
	}
	input = normalizeAriregisterSourceProfilesInput(input)
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
	logger.Debug("ariregister source profile normalization workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"mode", "copy",
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"trigger", input.Trigger,
	)

	result := NormalizeAriregisterSourceProfilesResult{Status: "succeeded"}
	remaining := input.Limit
	for {
		chunkLimit := input.BatchSize
		if len(input.IDs) > 0 {
			if input.Limit > 0 {
				chunkLimit = input.Limit
			} else {
				chunkLimit = len(input.IDs)
			}
		}
		if input.Limit > 0 && (chunkLimit <= 0 || remaining < chunkLimit) {
			chunkLimit = remaining
		}
		if chunkLimit <= 0 {
			chunkLimit = input.BatchSize
		}

		var activityResult NormalizeAriregisterSourceProfilesActivityResult
		if err := temporalworkflow.ExecuteActivity(ctx, NormalizeAriregisterSourceProfilesWithCopyActivity, NormalizeAriregisterSourceProfilesActivityInput{
			IDs:                input.IDs,
			Filters:            input.Filters,
			Limit:              int32(chunkLimit),
			Trigger:            input.Trigger,
			TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		}).Get(ctx, &activityResult); err != nil {
			return NormalizeAriregisterSourceProfilesResult{}, errors.Wrap(err, "normalize ariregister source profiles")
		}
		if activityResult.RecordsSeen == 0 {
			break
		}
		result.ChunksProcessed++
		result.add(activityResult)
		if len(input.IDs) > 0 {
			break
		}
		if input.Limit > 0 {
			remaining -= int(activityResult.RecordsSeen)
			if remaining <= 0 {
				break
			}
		}
		if int(activityResult.RecordsSeen) < chunkLimit {
			break
		}
	}
	refreshCtx := temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		TaskQueue:           RefreshAriregisterSourceExplorerTaskQueue,
		StartToCloseTimeout: 2 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	})
	var refreshResult RefreshAriregisterSourceExplorerActivityResult
	if err := temporalworkflow.ExecuteActivity(refreshCtx, RefreshAriregisterSourceExplorerActivity, RefreshAriregisterSourceExplorerActivityInput{
		Trigger:            input.Trigger,
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
	}).Get(refreshCtx, &refreshResult); err != nil {
		return NormalizeAriregisterSourceProfilesResult{}, errors.Wrap(err, "refresh ariregister source explorer after normalization")
	}
	logger.Debug("ariregister source profile explorer refreshed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"source_entries", refreshResult.SourceEntries,
		"used_concurrent_refresh", refreshResult.UsedConcurrentRefresh,
	)
	logger.Debug("ariregister source profile normalization workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"mode", "copy",
		"chunks_processed", result.ChunksProcessed,
		"records_seen", result.RecordsSeen,
		"companies_upserted", result.CompaniesUpserted,
	)

	return result, nil
}

func normalizeAriregisterSourceProfilesInput(input NormalizeAriregisterSourceProfilesInput) NormalizeAriregisterSourceProfilesInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	if input.BatchSize <= 0 {
		input.BatchSize = defaultSourceProfileChunkSize
	}
	return input
}

func (r *NormalizeAriregisterSourceProfilesResult) add(chunk NormalizeAriregisterSourceProfilesActivityResult) {
	r.RecordsSeen += chunk.RecordsSeen
	r.CompaniesUpserted += chunk.CompaniesUpserted
	r.CompanyNamesUpserted += chunk.CompanyNamesUpserted
	r.CompanyStatusesUpserted += chunk.CompanyStatusesUpserted
	r.LegalFormsUpserted += chunk.LegalFormsUpserted
	r.AddressesUpserted += chunk.AddressesUpserted
	r.ContactsUpserted += chunk.ContactsUpserted
	r.WebsitesUpserted += chunk.WebsitesUpserted
	r.DomainsUpserted += chunk.DomainsUpserted
	r.IndustriesUpserted += chunk.IndustriesUpserted
	r.CapitalUpserted += chunk.CapitalUpserted
	r.FinancialYearPeriodsUpserted += chunk.FinancialYearPeriodsUpserted
	r.AnnualReportsUpserted += chunk.AnnualReportsUpserted
	r.ArticlesUpserted += chunk.ArticlesUpserted
	r.RegistryNotesUpserted += chunk.RegistryNotesUpserted
}

func RefreshAriregisterSourceExplorer(
	ctx temporalworkflow.Context,
	input RefreshAriregisterSourceExplorerInput,
) (RefreshAriregisterSourceExplorerResult, error) {
	input = normalizeRefreshAriregisterSourceExplorerInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 1,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("ariregister source explorer refresh workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"trigger", input.Trigger,
	)

	var activityResult RefreshAriregisterSourceExplorerActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, RefreshAriregisterSourceExplorerActivity, RefreshAriregisterSourceExplorerActivityInput{
		TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		Trigger:            input.Trigger,
	}).Get(ctx, &activityResult); err != nil {
		return RefreshAriregisterSourceExplorerResult{}, errors.Wrap(err, "refresh ariregister source explorer")
	}
	logger.Debug("ariregister source explorer refresh workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"source_entries", activityResult.SourceEntries,
		"used_concurrent_refresh", activityResult.UsedConcurrentRefresh,
	)

	return RefreshAriregisterSourceExplorerResult{
		Status:                "succeeded",
		Refreshed:             activityResult.Refreshed,
		UsedConcurrentRefresh: activityResult.UsedConcurrentRefresh,
		SourceEntries:         activityResult.SourceEntries,
		LatestSourceUpdatedAt: activityResult.LatestSourceUpdatedAt,
	}, nil
}

func normalizeRefreshAriregisterSourceExplorerInput(input RefreshAriregisterSourceExplorerInput) RefreshAriregisterSourceExplorerInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

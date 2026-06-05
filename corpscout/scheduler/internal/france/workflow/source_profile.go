package workflow

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"

	"github.com/pulsarpoint/corpscout/scheduler/internal/france/actions"
)

const (
	defaultFranceSourceProfileChunkSize = 5000

	NormalizeFranceSourceProfilesTaskQueue    = "france-source-profile"
	NormalizeFranceSourceProfilesWorkflowName = "NormalizeFranceSourceProfiles"

	NormalizeFranceSourceProfilesActivity = "NormalizeFranceSourceProfilesActivity"
)

type NormalizeFranceSourceProfilesActivityInput = actions.NormalizeFranceSourceProfilesActivityInput
type NormalizeFranceSourceProfilesActivityResult = actions.NormalizeFranceSourceProfilesActivityResult

type NormalizeFranceSourceProfilesInput struct {
	IDs       []string          `json:"ids,omitempty"`
	Filters   map[string]string `json:"filters,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	BatchSize int               `json:"batch_size,omitempty"`
	Trigger   string            `json:"trigger,omitempty"`
}

type NormalizeFranceSourceProfilesResult struct {
	Status                 string `json:"status"`
	ChunksProcessed        int32  `json:"chunks_processed"`
	RecordsSeen            int32  `json:"records_seen"`
	CompaniesUpserted      int32  `json:"companies_upserted"`
	EstablishmentsUpserted int32  `json:"establishments_upserted"`
	AddressesUpserted      int32  `json:"addresses_upserted"`
	IndustriesInserted     int32  `json:"industries_inserted"`
	WebsitesUpserted       int32  `json:"websites_upserted"`
	DomainsUpserted        int32  `json:"domains_upserted"`
	ContactsUpserted       int32  `json:"contacts_upserted"`
}

func NormalizeFranceSourceProfiles(
	ctx temporalworkflow.Context,
	input NormalizeFranceSourceProfilesInput,
) (NormalizeFranceSourceProfilesResult, error) {
	if input.Limit < 0 {
		return NormalizeFranceSourceProfilesResult{}, errors.New("limit cannot be negative")
	}
	input = normalizeFranceSourceProfilesInput(input)
	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	})
	workflowInfo := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("france source profile normalization workflow started",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"ids_count", len(input.IDs),
		"filters_count", len(input.Filters),
		"limit", input.Limit,
		"batch_size", input.BatchSize,
		"trigger", input.Trigger,
	)

	result := NormalizeFranceSourceProfilesResult{Status: "succeeded"}
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

		var activityResult NormalizeFranceSourceProfilesActivityResult
		if err := temporalworkflow.ExecuteActivity(ctx, NormalizeFranceSourceProfilesActivity, NormalizeFranceSourceProfilesActivityInput{
			IDs:                input.IDs,
			Filters:            input.Filters,
			Limit:              int32(chunkLimit),
			Trigger:            input.Trigger,
			TemporalWorkflowID: workflowInfo.WorkflowExecution.ID,
		}).Get(ctx, &activityResult); err != nil {
			return NormalizeFranceSourceProfilesResult{}, errors.Wrap(err, "normalize france source profiles")
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
	logger.Debug("france source profile normalization workflow completed",
		"temporal_workflow_id", workflowInfo.WorkflowExecution.ID,
		"chunks_processed", result.ChunksProcessed,
		"records_seen", result.RecordsSeen,
		"companies_upserted", result.CompaniesUpserted,
		"establishments_upserted", result.EstablishmentsUpserted,
	)

	return result, nil
}

func normalizeFranceSourceProfilesInput(input NormalizeFranceSourceProfilesInput) NormalizeFranceSourceProfilesInput {
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	if input.BatchSize <= 0 {
		input.BatchSize = defaultFranceSourceProfileChunkSize
	}
	return input
}

func (r *NormalizeFranceSourceProfilesResult) add(chunk NormalizeFranceSourceProfilesActivityResult) {
	r.RecordsSeen += chunk.RecordsSeen
	r.CompaniesUpserted += chunk.CompaniesUpserted
	r.EstablishmentsUpserted += chunk.EstablishmentsUpserted
	r.AddressesUpserted += chunk.AddressesUpserted
	r.IndustriesInserted += chunk.IndustriesInserted
	r.WebsitesUpserted += chunk.WebsitesUpserted
	r.DomainsUpserted += chunk.DomainsUpserted
	r.ContactsUpserted += chunk.ContactsUpserted
}

package nacetaxonomy

import (
	"time"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/temporal"
	temporalworkflow "go.temporal.io/sdk/workflow"
)

const (
	SyncWorkflowName = "SyncNACETaxonomy"
	SyncTaskQueue    = "nace-taxonomy-sync"

	syncNACETaxonomyActivity = "SyncNACETaxonomyActivity"
)

type SyncNACETaxonomyInput struct {
	Revision       string `json:"revision,omitempty"`
	SourceURL      string `json:"source_url,omitempty"`
	Trigger        string `json:"trigger,omitempty"`
	ForceReprocess bool   `json:"force_reprocess,omitempty"`
}

type SyncNACETaxonomyResult = SyncNACETaxonomyActivityResult

func SyncNACETaxonomy(ctx temporalworkflow.Context, input SyncNACETaxonomyInput) (SyncNACETaxonomyResult, error) {
	input = normalizeSyncNACEWorkflowInput(input)
	info := temporalworkflow.GetInfo(ctx)
	logger := temporalworkflow.GetLogger(ctx)
	logger.Debug("nace taxonomy sync workflow started",
		"temporal_workflow_id", info.WorkflowExecution.ID,
		"revision", input.Revision,
		"source_url", input.SourceURL,
		"trigger", input.Trigger,
		"force_reprocess", input.ForceReprocess,
	)

	ctx = temporalworkflow.WithActivityOptions(ctx, temporalworkflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    10 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	var result SyncNACETaxonomyActivityResult
	if err := temporalworkflow.ExecuteActivity(ctx, syncNACETaxonomyActivity, SyncNACETaxonomyActivityInput{
		TemporalWorkflowID: info.WorkflowExecution.ID,
		Revision:           input.Revision,
		SourceURL:          input.SourceURL,
		Trigger:            input.Trigger,
		ForceReprocess:     input.ForceReprocess,
	}).Get(ctx, &result); err != nil {
		return SyncNACETaxonomyResult{}, errors.Wrap(err, "sync nace taxonomy activity")
	}
	logger.Debug("nace taxonomy sync workflow finished",
		"status", result.Status,
		"import_run_id", result.ImportRunID,
		"source_file_id", result.SourceFileID,
		"content_sha256", result.ContentSHA256,
		"records_seen", result.RecordsSeen,
		"records_imported", result.RecordsImported,
		"records_deactivated", result.RecordsDeactivated,
	)
	return result, nil
}

func normalizeSyncNACEWorkflowInput(input SyncNACETaxonomyInput) SyncNACETaxonomyInput {
	if input.Revision == "" {
		input.Revision = DefaultRevision
	}
	if input.Trigger == "" {
		input.Trigger = "manual"
	}
	return input
}

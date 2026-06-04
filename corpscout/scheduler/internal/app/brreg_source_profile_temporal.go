package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregSourceProfileTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg source profile temporal worker", "task_queue", brregworkflow.NormalizeBrregSourceProfilesTaskQueue)
	sourceProfileWorker := temporalworker.New(
		temporalClient,
		brregworkflow.NormalizeBrregSourceProfilesTaskQueue,
		temporalworker.Options{},
	)
	registerBrregSourceProfileWorker(sourceProfileWorker, resources)
	return sourceProfileWorker
}

func newBrregSourceExplorerRefreshTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg source explorer refresh temporal worker", "task_queue", brregworkflow.RefreshBrregSourceExplorerTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		brregworkflow.RefreshBrregSourceExplorerTaskQueue,
		temporalworker.Options{},
	)
	registerBrregSourceExplorerRefreshWorker(worker, resources)
	return worker
}

func registerBrregSourceProfileWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg source profile temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.NormalizeBrregSourceProfiles)
	worker.RegisterWorkflow(brregworkflow.NormalizeBrregSourceProfilesWithCopy)
	worker.RegisterActivityWithOptions(
		resources.sourceProfileActions.NormalizeBrregSourceProfiles,
		activity.RegisterOptions{Name: "NormalizeBrregSourceProfilesActivity"},
	)
	worker.RegisterActivityWithOptions(
		resources.sourceProfileActions.NormalizeBrregSourceProfilesWithCopy,
		activity.RegisterOptions{Name: "NormalizeBrregSourceProfilesWithCopyActivity"},
	)
}

func registerBrregSourceExplorerRefreshWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg source explorer refresh temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.RefreshBrregSourceExplorer)
	worker.RegisterActivityWithOptions(
		resources.sourceProfileActions.RefreshBrregSourceExplorer,
		activity.RegisterOptions{Name: "RefreshBrregSourceExplorerActivity"},
	)
}

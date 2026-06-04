package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	ariregisterworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/workflow"
)

func newAriregisterSourceProfileTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating ariregister source profile temporal worker", "task_queue", ariregisterworkflow.NormalizeAriregisterSourceProfilesTaskQueue)
	sourceProfileWorker := temporalworker.New(
		temporalClient,
		ariregisterworkflow.NormalizeAriregisterSourceProfilesTaskQueue,
		temporalworker.Options{},
	)
	registerAriregisterSourceProfileWorker(sourceProfileWorker, resources)
	return sourceProfileWorker
}

func newAriregisterSourceExplorerRefreshTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating ariregister source explorer refresh temporal worker", "task_queue", ariregisterworkflow.RefreshAriregisterSourceExplorerTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		ariregisterworkflow.RefreshAriregisterSourceExplorerTaskQueue,
		temporalworker.Options{},
	)
	registerAriregisterSourceExplorerRefreshWorker(worker, resources)
	return worker
}

func registerAriregisterSourceProfileWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering ariregister source profile temporal workflow and activities")
	worker.RegisterWorkflow(ariregisterworkflow.NormalizeAriregisterSourceProfilesWithCopy)
	worker.RegisterActivityWithOptions(
		resources.ariregisterSourceProfile.NormalizeAriregisterSourceProfilesWithCopy,
		activity.RegisterOptions{Name: ariregisterworkflow.NormalizeAriregisterSourceProfilesWithCopyActivity},
	)
}

func registerAriregisterSourceExplorerRefreshWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering ariregister source explorer refresh temporal workflow and activities")
	worker.RegisterWorkflow(ariregisterworkflow.RefreshAriregisterSourceExplorer)
	worker.RegisterActivityWithOptions(
		resources.ariregisterSourceProfile.RefreshAriregisterSourceExplorer,
		activity.RegisterOptions{Name: ariregisterworkflow.RefreshAriregisterSourceExplorerActivity},
	)
}

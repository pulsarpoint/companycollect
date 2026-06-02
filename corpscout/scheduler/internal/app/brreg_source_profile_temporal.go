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

func registerBrregSourceProfileWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg source profile temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.NormalizeBrregSourceProfiles)
	worker.RegisterActivityWithOptions(
		resources.sourceProfileActions.NormalizeBrregSourceProfiles,
		activity.RegisterOptions{Name: "NormalizeBrregSourceProfilesActivity"},
	)
}

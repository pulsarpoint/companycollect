package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	franceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/france/workflow"
)

func newFranceSourceProfileTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating france source profile temporal worker", "task_queue", franceworkflow.NormalizeFranceSourceProfilesTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		franceworkflow.NormalizeFranceSourceProfilesTaskQueue,
		temporalworker.Options{},
	)
	registerFranceSourceProfileWorker(worker, resources)
	return worker
}

func registerFranceSourceProfileWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering france source profile temporal workflow and activities")
	worker.RegisterWorkflow(franceworkflow.NormalizeFranceSourceProfiles)
	worker.RegisterActivityWithOptions(
		resources.franceSourceProfile.NormalizeFranceSourceProfiles,
		activity.RegisterOptions{Name: franceworkflow.NormalizeFranceSourceProfilesActivity},
	)
}

package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregSourceCapitalFXTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg source capital fx temporal worker", "task_queue", brregworkflow.ConvertBrregSourceCapitalToUSDTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		brregworkflow.ConvertBrregSourceCapitalToUSDTaskQueue,
		temporalworker.Options{},
	)
	registerBrregSourceCapitalFXWorker(worker, resources)
	return worker
}

func registerBrregSourceCapitalFXWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg source capital fx temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.ConvertBrregSourceCapitalToUSD)
	worker.RegisterActivityWithOptions(
		resources.sourceCapitalFX.ConvertBrregSourceCapitalToUSD,
		activity.RegisterOptions{Name: "ConvertBrregSourceCapitalToUSDActivity"},
	)
}

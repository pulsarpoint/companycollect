package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregSourceFinancialTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg source financial temporal worker", "task_queue", brregworkflow.FetchBrregSourceFinancialStatementsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		brregworkflow.FetchBrregSourceFinancialStatementsTaskQueue,
		temporalworker.Options{},
	)
	registerBrregSourceFinancialWorker(worker, resources)
	return worker
}

func registerBrregSourceFinancialWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg source financial temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.FetchBrregSourceFinancialStatements)
	worker.RegisterActivityWithOptions(
		resources.sourceFinancial.FetchBrregSourceFinancialStatements,
		activity.RegisterOptions{Name: "FetchBrregSourceFinancialStatementsActivity"},
	)
}

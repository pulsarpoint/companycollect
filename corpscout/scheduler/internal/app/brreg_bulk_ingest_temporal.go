package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregBulkIngestTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg bulk ingest temporal worker", "task_queue", brregworkflow.LoadBrregBulkRawRecordsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		brregworkflow.LoadBrregBulkRawRecordsTaskQueue,
		temporalworker.Options{},
	)
	registerBrregBulkIngestWorker(worker, resources)
	return worker
}

func registerBrregBulkIngestWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg bulk ingest temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.LoadBrregBulkRawRecords)
	worker.RegisterActivityWithOptions(
		resources.bulkIngestActions.LoadBrregBulkRawRecords,
		activity.RegisterOptions{Name: "LoadBrregBulkRawRecordsActivity"},
	)
}

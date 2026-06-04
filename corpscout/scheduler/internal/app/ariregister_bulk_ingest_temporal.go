package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	ariregisterworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/workflow"
)

func newAriregisterBulkIngestTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating ariregister bulk ingest temporal worker", "task_queue", ariregisterworkflow.LoadAriregisterBulkRawRecordsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		ariregisterworkflow.LoadAriregisterBulkRawRecordsTaskQueue,
		temporalworker.Options{},
	)
	registerAriregisterBulkIngestWorker(worker, resources)
	return worker
}

func registerAriregisterBulkIngestWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering ariregister bulk ingest temporal workflow and activities")
	worker.RegisterWorkflow(ariregisterworkflow.LoadAriregisterBulkRawRecords)
	worker.RegisterActivityWithOptions(
		resources.ariregisterBulkIngest.LoadAriregisterBulkRawRecords,
		activity.RegisterOptions{Name: "LoadAriregisterBulkRawRecordsActivity"},
	)
}

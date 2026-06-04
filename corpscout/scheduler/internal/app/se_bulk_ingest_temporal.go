package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	seworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/se/workflow"
)

func newSEBulkIngestTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating se bulk ingest temporal worker", "task_queue", seworkflow.LoadSEBulkRawRecordsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		seworkflow.LoadSEBulkRawRecordsTaskQueue,
		temporalworker.Options{},
	)
	registerSEBulkIngestWorker(worker, resources)
	return worker
}

func registerSEBulkIngestWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering se bulk ingest temporal workflow and activities")
	worker.RegisterWorkflow(seworkflow.LoadSEBulkRawRecords)
	worker.RegisterActivityWithOptions(
		resources.seBulkIngest.LoadSEBulkRawRecords,
		activity.RegisterOptions{Name: "LoadSEBulkRawRecordsActivity"},
	)
}

package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	franceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/france/workflow"
)

func newFranceBulkIngestTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating france bulk ingest temporal worker", "task_queue", franceworkflow.LoadFranceBulkRawRecordsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		franceworkflow.LoadFranceBulkRawRecordsTaskQueue,
		temporalworker.Options{},
	)
	registerFranceBulkIngestWorker(worker, resources)
	return worker
}

func registerFranceBulkIngestWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering france bulk ingest temporal workflow and activities")
	worker.RegisterWorkflow(franceworkflow.LoadFranceBulkRawRecords)
	worker.RegisterActivityWithOptions(
		resources.franceBulkIngest.LoadFranceBulkRawRecords,
		activity.RegisterOptions{Name: "LoadFranceBulkRawRecordsActivity"},
	)
}

package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	cvrworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/cvr/workflow"
)

func newCVRRawIngestTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating cvr raw ingest temporal worker", "task_queue", cvrworkflow.LoadCVRRawRecordsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		cvrworkflow.LoadCVRRawRecordsTaskQueue,
		temporalworker.Options{},
	)
	registerCVRRawIngestWorker(worker, resources)
	return worker
}

func registerCVRRawIngestWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering cvr raw ingest temporal workflow and activities")
	worker.RegisterWorkflow(cvrworkflow.LoadCVRRawRecords)
	worker.RegisterActivityWithOptions(
		resources.cvrRawIngest.LoadCVRRawRecords,
		activity.RegisterOptions{Name: "LoadCVRRawRecordsActivity"},
	)
}

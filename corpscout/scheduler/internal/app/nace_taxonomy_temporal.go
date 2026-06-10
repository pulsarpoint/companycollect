package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	naceworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/nace"
)

func newNACETaxonomyTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating nace taxonomy temporal worker", "task_queue", naceworkflow.SyncTaskQueue)
	worker := temporalworker.New(temporalClient, naceworkflow.SyncTaskQueue, temporalworker.Options{})
	worker.RegisterWorkflow(naceworkflow.SyncNACETaxonomy)
	worker.RegisterWorkflow(naceworkflow.SyncNACEToClickHouse)
	worker.RegisterActivityWithOptions(
		resources.naceTaxonomyActions.SyncNACETaxonomyActivity,
		activity.RegisterOptions{Name: naceworkflow.SyncNACETaxonomyActivityName},
	)
	worker.RegisterActivityWithOptions(
		resources.naceTaxonomyActions.SyncNACEToClickHouseActivity,
		activity.RegisterOptions{Name: naceworkflow.SyncNACEToClickHouseActivityName},
	)
	return worker
}

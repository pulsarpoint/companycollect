package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
)

func newNACETaxonomyTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating nace taxonomy temporal worker", "task_queue", nacetaxonomy.SyncTaskQueue)
	worker := temporalworker.New(temporalClient, nacetaxonomy.SyncTaskQueue, temporalworker.Options{})
	worker.RegisterWorkflow(nacetaxonomy.SyncNACETaxonomy)
	worker.RegisterActivityWithOptions(
		resources.naceTaxonomyActions.SyncNACETaxonomyActivity,
		activity.RegisterOptions{Name: "SyncNACETaxonomyActivity"},
	)
	return worker
}

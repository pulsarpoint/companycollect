package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
)

func newFXTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating exchange rate temporal worker", "task_queue", fx.SyncTaskQueue)
	worker := temporalworker.New(temporalClient, fx.SyncTaskQueue, temporalworker.Options{})
	worker.RegisterWorkflow(fx.SyncExchangeRates)
	worker.RegisterActivityWithOptions(
		resources.fxActions.SyncExchangeRatesActivity,
		activity.RegisterOptions{Name: "SyncExchangeRatesActivity"},
	)
	return worker
}

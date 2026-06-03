package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregTermTranslationTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg term translation temporal worker", "task_queue", brregworkflow.TranslateBrregSourceTermsTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		brregworkflow.TranslateBrregSourceTermsTaskQueue,
		temporalworker.Options{},
	)
	registerBrregTermTranslationWorker(worker, resources)
	return worker
}

func registerBrregTermTranslationWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg term translation temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.TranslateBrregSourceTerms)
	worker.RegisterActivityWithOptions(
		resources.termTranslationActions.EnsureBrregTranslationTerms,
		activity.RegisterOptions{Name: "EnsureBrregTranslationTerms"},
	)
	worker.RegisterActivityWithOptions(
		resources.termTranslationActions.PublishBrregTranslationTerms,
		activity.RegisterOptions{Name: "PublishBrregTranslationTerms"},
	)
	worker.RegisterActivityWithOptions(
		resources.termTranslationActions.ApplyBrregCachedTranslationTerms,
		activity.RegisterOptions{Name: "ApplyBrregCachedTranslationTerms"},
	)
}

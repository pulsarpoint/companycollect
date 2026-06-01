package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregTranslationTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg translation temporal worker", "task_queue", brregworkflow.TranslateBrregRawInputsTaskQueue)
	translationWorker := temporalworker.New(
		temporalClient,
		brregworkflow.TranslateBrregRawInputsTaskQueue,
		temporalworker.Options{},
	)
	registerBrregTranslationWorker(translationWorker, resources)
	return translationWorker
}

func registerBrregTranslationWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg translation temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.TranslateBrregRawInputs)
	worker.RegisterActivityWithOptions(
		resources.translationActions.PrepareBrregTranslationWorkflow,
		activity.RegisterOptions{Name: "PrepareBrregTranslationWorkflow"},
	)
	worker.RegisterActivityWithOptions(
		resources.translationActions.FailRunningBrregTranslationTasksForWorkflow,
		activity.RegisterOptions{Name: "FailRunningBrregTranslationTasksForWorkflow"},
	)
	worker.RegisterActivityWithOptions(
		resources.translationActions.FinishBrregTranslationWorkflow,
		activity.RegisterOptions{Name: "FinishBrregTranslationWorkflow"},
	)
	worker.RegisterActivityWithOptions(
		resources.translationActions.ClaimBrregTranslationBatch,
		activity.RegisterOptions{Name: "ClaimBrregTranslationBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.translationActions.TranslateBrregBatch,
		activity.RegisterOptions{Name: "TranslateBrregBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.translationActions.SubmitBrregTranslationBatch,
		activity.RegisterOptions{Name: "SubmitBrregTranslationBatch"},
	)
}

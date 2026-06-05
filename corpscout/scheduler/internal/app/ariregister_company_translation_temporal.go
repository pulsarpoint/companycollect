package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	ariregisterworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/workflow"
)

func newAriregisterCompanyTranslationTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating ariregister company translation temporal worker", "task_queue", ariregisterworkflow.TranslateAriregisterSourceCompaniesTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		ariregisterworkflow.TranslateAriregisterSourceCompaniesTaskQueue,
		temporalworker.Options{},
	)
	registerAriregisterCompanyTranslationWorker(worker, resources)
	return worker
}

func registerAriregisterCompanyTranslationWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering ariregister company translation temporal workflow and activities")
	worker.RegisterWorkflow(ariregisterworkflow.TranslateAriregisterSourceCompanies)
	worker.RegisterActivityWithOptions(
		resources.ariregisterCompanyTranslation.BuildAriregisterTranslationWorkset,
		activity.RegisterOptions{Name: "BuildAriregisterTranslationWorkset"},
	)
	worker.RegisterActivityWithOptions(
		resources.ariregisterCompanyTranslation.ClaimAriregisterTranslationWorksetBatch,
		activity.RegisterOptions{Name: "ClaimAriregisterTranslationWorksetBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.ariregisterCompanyTranslation.TranslateAriregisterTranslationWorksetBatch,
		activity.RegisterOptions{Name: "TranslateAriregisterTranslationWorksetBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.ariregisterCompanyTranslation.SaveAriregisterTranslationWorksetBatch,
		activity.RegisterOptions{Name: "SaveAriregisterTranslationWorksetBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.ariregisterCompanyTranslation.ApplyAriregisterTranslationWorkset,
		activity.RegisterOptions{Name: "ApplyAriregisterTranslationWorkset"},
	)
}

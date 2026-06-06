package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregCompanyTranslationTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg company translation temporal worker", "task_queue", brregworkflow.TranslateBrregSourceCompaniesTaskQueue)
	worker := temporalworker.New(
		temporalClient,
		brregworkflow.TranslateBrregSourceCompaniesTaskQueue,
		temporalworker.Options{},
	)
	registerBrregCompanyTranslationWorker(worker, resources)
	return worker
}

func registerBrregCompanyTranslationWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg company translation temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.TranslateBrregSourceCompanies)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.BuildBrregTranslationWorkset,
		activity.RegisterOptions{Name: "BuildBrregTranslationWorkset"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.ClaimBrregTranslationWorksetBatch,
		activity.RegisterOptions{Name: "ClaimBrregTranslationWorksetBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.TranslateBrregTranslationWorksetBatch,
		activity.RegisterOptions{Name: "TranslateBrregTranslationWorksetBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.SaveBrregTranslationWorksetBatch,
		activity.RegisterOptions{Name: "SaveBrregTranslationWorksetBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.ApplyBrregTranslationWorkset,
		activity.RegisterOptions{Name: "ApplyBrregTranslationWorkset"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.RefreshBrregTranslationStatus,
		activity.RegisterOptions{Name: "RefreshBrregTranslationStatus"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.CompleteBrregTranslationQueueBatch,
		activity.RegisterOptions{Name: "CompleteBrregTranslationQueueBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.ReleaseBrregTranslationQueueBatch,
		activity.RegisterOptions{Name: "ReleaseBrregTranslationQueueBatch"},
	)
}

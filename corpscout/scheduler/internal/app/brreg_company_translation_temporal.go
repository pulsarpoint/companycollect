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
		resources.companyTranslation.ClaimBrregCompaniesForTranslation,
		activity.RegisterOptions{Name: "ClaimBrregCompaniesForTranslation"},
	)
	worker.RegisterActivityWithOptions(
		resources.companyTranslation.ProcessBrregCompanyTranslation,
		activity.RegisterOptions{Name: "ProcessBrregCompanyTranslation"},
	)
}

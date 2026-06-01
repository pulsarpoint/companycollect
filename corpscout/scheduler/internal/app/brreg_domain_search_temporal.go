package app

import (
	"log/slog"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregworkflow "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/workflow"
)

func newBrregDomainSearchTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating brreg domain search temporal worker", "task_queue", brregworkflow.SearchBrregDomainsTaskQueue)
	domainSearchWorker := temporalworker.New(
		temporalClient,
		brregworkflow.SearchBrregDomainsTaskQueue,
		temporalworker.Options{},
	)
	registerBrregDomainSearchWorker(domainSearchWorker, resources)
	return domainSearchWorker
}

func registerBrregDomainSearchWorker(worker temporalworker.Worker, resources *temporalWorkerResources) {
	slog.Debug("registering brreg domain search temporal workflow and activities")
	worker.RegisterWorkflow(brregworkflow.SearchBrregDomains)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.PrepareBrregDomainSearchWorkflow,
		activity.RegisterOptions{Name: "PrepareBrregDomainSearchWorkflow"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.FailRunningBrregDomainSearchTasksForWorkflow,
		activity.RegisterOptions{Name: "FailRunningBrregDomainSearchTasksForWorkflow"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.FinishBrregDomainSearchWorkflow,
		activity.RegisterOptions{Name: "FinishBrregDomainSearchWorkflow"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.ClaimBrregDomainSearchBatch,
		activity.RegisterOptions{Name: "ClaimBrregDomainSearchBatch"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.FetchBrregDomainSearchPages,
		activity.RegisterOptions{Name: "FetchBrregDomainSearchPages"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.AnalyzeBrregDomainSearchPages,
		activity.RegisterOptions{Name: "AnalyzeBrregDomainSearchPages"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.CrawlBrregDomainCandidateSites,
		activity.RegisterOptions{Name: "CrawlBrregDomainCandidateSites"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.AnalyzeBrregDomainCandidateSites,
		activity.RegisterOptions{Name: "AnalyzeBrregDomainCandidateSites"},
	)
	worker.RegisterActivityWithOptions(
		resources.domainSearchActions.SubmitBrregDomainSearchResults,
		activity.RegisterOptions{Name: "SubmitBrregDomainSearchResults"},
	)
}

package app

import (
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	companysources "github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/finland/prhytj"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/unitedstates/coloradoentities"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/unitedstates/irseobmf"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/unitedstates/secedgar"
	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
	companysourceactions "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/actions/companysources"
	naceactions "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/actions/nace"
	companysourceworkflows "github.com/pulsarpoint/corpscout/scheduler/internal/temporal/workflow/companysources"
)

type temporalWorkerResources struct {
	naceTaxonomyActions  *naceactions.Actions
	fxActions            *fx.Actions
	companySourceActions *companysourceactions.Actions
}

func newTemporalWorkerResources(cfg config.Config, pool *pgxpool.Pool, llmStore *llmproviders.Store, s3 *s3client.Client) (*temporalWorkerResources, error) {
	slog.Debug("creating temporal worker resources")
	_ = llmStore
	_ = s3
	sourceRegistry := companysources.NewRegistry(
		prhytj.Source{},
		coloradoentities.Source{},
		irseobmf.Source{},
		secedgar.Source{},
	)
	return &temporalWorkerResources{
		naceTaxonomyActions:  naceactions.NewActions(pool, http.DefaultClient, cfg.ClickHouseNativeURL),
		fxActions:            fx.NewActions(pool, http.DefaultClient, cfg.FXECBSourceURL),
		companySourceActions: companysourceactions.NewActions(pool, sourceRegistry, cfg.SourceRunsRoot, cfg.ClickHouseNativeURL),
	}, nil
}

func (r *temporalWorkerResources) Close() {
}

func newTemporalWorkers(temporalClient client.Client, resources *temporalWorkerResources) []temporalworker.Worker {
	slog.Debug("creating temporal workers")
	return []temporalworker.Worker{
		newNACETaxonomyTemporalWorker(temporalClient, resources),
		newFXTemporalWorker(temporalClient, resources),
		newCompanySourcesTemporalWorker(temporalClient, resources),
	}
}

func newCompanySourcesTemporalWorker(temporalClient client.Client, resources *temporalWorkerResources) temporalworker.Worker {
	slog.Debug("creating company sources temporal worker", "task_queue", companysourceworkflows.SourceTaskQueue)
	worker := temporalworker.New(temporalClient, companysourceworkflows.SourceTaskQueue, temporalworker.Options{})
	worker.RegisterWorkflowWithOptions(
		companysourceworkflows.DownloadSource,
		workflow.RegisterOptions{Name: companysourceworkflows.DownloadSourceWorkflowName},
	)
	worker.RegisterWorkflowWithOptions(
		companysourceworkflows.DownloadSourceFile,
		workflow.RegisterOptions{Name: companysourceworkflows.DownloadSourceFileWorkflowName},
	)
	worker.RegisterWorkflowWithOptions(
		companysourceworkflows.ImportSourceToClickHouse,
		workflow.RegisterOptions{Name: companysourceworkflows.ImportSourceToClickHouseWorkflowName},
	)
	worker.RegisterWorkflowWithOptions(
		companysourceworkflows.SyncSourceToClickHouse,
		workflow.RegisterOptions{Name: companysourceworkflows.SyncSourceToClickHouseWorkflowName},
	)
	worker.RegisterWorkflowWithOptions(
		companysourceworkflows.RefreshSourceExplorerCache,
		workflow.RegisterOptions{Name: companysourceworkflows.RefreshSourceExplorerCacheWorkflowName},
	)
	worker.RegisterWorkflowWithOptions(
		companysourceworkflows.MapSourceIndustriesToNACE,
		workflow.RegisterOptions{Name: companysourceworkflows.MapSourceIndustriesToNACEWorkflowName},
	)
	worker.RegisterActivityWithOptions(
		resources.companySourceActions.PrepareSourceDownloadActivity,
		activity.RegisterOptions{Name: companysourceworkflows.PrepareSourceDownloadActivityName},
	)
	worker.RegisterActivityWithOptions(
		resources.companySourceActions.FinishSourceDownloadActivity,
		activity.RegisterOptions{Name: companysourceworkflows.FinishSourceDownloadActivityName},
	)
	worker.RegisterActivityWithOptions(
		resources.companySourceActions.DownloadSourceFileActivity,
		activity.RegisterOptions{Name: companysourceworkflows.DownloadSourceFileActivityName},
	)
	worker.RegisterActivityWithOptions(
		resources.companySourceActions.ImportSourceToClickHouseActivity,
		activity.RegisterOptions{Name: companysourceworkflows.ImportSourceToClickHouseActivityName},
	)
	worker.RegisterActivityWithOptions(
		resources.companySourceActions.RefreshSourceExplorerCacheActivity,
		activity.RegisterOptions{Name: companysourceworkflows.RefreshSourceExplorerCacheActivityName},
	)
	worker.RegisterActivityWithOptions(
		resources.companySourceActions.MapSourceIndustriesToNACEActivity,
		activity.RegisterOptions{Name: companysourceworkflows.MapSourceIndustriesToNACEActivityName},
	)
	return worker
}

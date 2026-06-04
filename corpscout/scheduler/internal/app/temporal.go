package app

import (
	"log/slog"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	ariregisteractions "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/actions"
	ariregisterdb "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/db"
	brregactions "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/financial"
	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlclient"
	cvractions "github.com/pulsarpoint/corpscout/scheduler/internal/cvr/actions"
	cvrdb "github.com/pulsarpoint/corpscout/scheduler/internal/cvr/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/fx"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
	"github.com/pulsarpoint/corpscout/scheduler/internal/nacetaxonomy"
	"github.com/pulsarpoint/corpscout/scheduler/internal/s3client"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type temporalWorkerResources struct {
	translationClient     *translationclient.Client
	companyTranslation    *brregactions.CompanyTranslationActions
	crawlClient           *crawlclient.Client
	domainSearchActions   *brregactions.DomainSearchActions
	bulkIngestActions     *brregactions.BulkIngestActions
	sourceProfileActions  *brregactions.SourceProfileActions
	sourceCapitalFX       *brregactions.SourceCapitalFXActions
	sourceFinancial       *brregactions.SourceFinancialActions
	ariregisterBulkIngest *ariregisteractions.BulkIngestActions
	cvrRawIngest          *cvractions.RawIngestActions
	naceTaxonomyActions   *nacetaxonomy.Actions
	fxActions             *fx.Actions
}

func newTemporalWorkerResources(cfg config.Config, pool *pgxpool.Pool, llmStore *llmproviders.Store, s3 *s3client.Client) (*temporalWorkerResources, error) {
	slog.Debug("creating temporal worker resources")
	if cfg.NATSURL == "" {
		return nil, errors.New("CORPSCOUT_NATS_URL is required for temporal translation actions")
	}
	translator, err := translationclient.NewNATSWithRequestTimeout(cfg.NATSURL, cfg.NATSRequestTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "create brreg translation nats client")
	}
	slog.Debug("created brreg translation nats client",
		"subject", translationclient.DefaultBrregTranslationSubject,
		"request_timeout", cfg.NATSRequestTimeout.String(),
	)
	crawler, err := crawlclient.NewNATSWithRequestTimeout(cfg.NATSURL, cfg.NATSRequestTimeout)
	if err != nil {
		translator.Close()
		return nil, errors.Wrap(err, "create brreg crawl nats client")
	}
	slog.Debug("created brreg crawl nats client",
		"search_fetch_subject", crawlclient.DefaultSearchFetchSubject,
		"search_analyze_subject", crawlclient.DefaultSearchAnalyzeSubject,
		"page_crawl_subject", crawlclient.DefaultPageCrawlSubject,
		"page_analyze_subject", crawlclient.DefaultPageAnalyzeSubject,
		"request_timeout", cfg.NATSRequestTimeout.String(),
	)
	gateway := brregdb.New(pool)
	brregCompanyData := companydata.New(pool)
	financialClient := financial.NewClient(cfg.BRREGFinancialURL, http.DefaultClient)
	ariregisterGateway := ariregisterdb.New(pool)
	cvrGateway := cvrdb.New(pool)
	return &temporalWorkerResources{
		translationClient:     translator,
		companyTranslation:    brregactions.NewCompanyTranslationActions(brregCompanyData, translator),
		crawlClient:           crawler,
		domainSearchActions:   brregactions.NewDomainSearchActions(gateway, crawler, llmStore, s3),
		bulkIngestActions:     brregactions.NewBulkIngestActions(gateway, http.DefaultClient, cfg.BRREGBulkSourceURL),
		sourceProfileActions:  brregactions.NewSourceProfileActions(gateway),
		sourceCapitalFX:       brregactions.NewSourceCapitalFXActions(gateway),
		sourceFinancial:       brregactions.NewSourceFinancialActions(gateway, financialClient),
		ariregisterBulkIngest: ariregisteractions.NewBulkIngestActions(ariregisterGateway, http.DefaultClient, cfg.AriregisterSourceURL),
		cvrRawIngest: cvractions.NewRawIngestActions(cvrGateway, http.DefaultClient, cvractions.RawIngestConfig{
			SourceURL:   cfg.CVRSourceURL,
			ScrollURL:   cfg.CVRScrollURL,
			Scroll:      cfg.CVRScroll,
			Username:    cfg.CVRUsername,
			Password:    cfg.CVRPassword,
			BearerToken: cfg.CVRBearerToken,
			APIKey:      cfg.CVRAPIKey,
		}),
		naceTaxonomyActions: nacetaxonomy.NewActions(pool, http.DefaultClient),
		fxActions:           fx.NewActions(pool, http.DefaultClient, cfg.FXECBSourceURL),
	}, nil
}

func (r *temporalWorkerResources) Close() {
	if r != nil && r.translationClient != nil {
		slog.Debug("closing brreg translation nats client")
		r.translationClient.Close()
	}
	if r != nil && r.crawlClient != nil {
		slog.Debug("closing brreg crawl nats client")
		r.crawlClient.Close()
	}
}

func newTemporalWorkers(temporalClient client.Client, resources *temporalWorkerResources) []temporalworker.Worker {
	slog.Debug("creating temporal workers")
	return []temporalworker.Worker{
		newBrregCompanyTranslationTemporalWorker(temporalClient, resources),
		newBrregDomainSearchTemporalWorker(temporalClient, resources),
		newBrregBulkIngestTemporalWorker(temporalClient, resources),
		newBrregSourceProfileTemporalWorker(temporalClient, resources),
		newBrregSourceExplorerRefreshTemporalWorker(temporalClient, resources),
		newBrregSourceCapitalFXTemporalWorker(temporalClient, resources),
		newBrregSourceFinancialTemporalWorker(temporalClient, resources),
		newAriregisterBulkIngestTemporalWorker(temporalClient, resources),
		newCVRRawIngestTemporalWorker(temporalClient, resources),
		newNACETaxonomyTemporalWorker(temporalClient, resources),
		newFXTemporalWorker(temporalClient, resources),
	}
}

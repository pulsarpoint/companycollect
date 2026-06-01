package app

import (
	"log/slog"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregactions "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/crawlclient"
	"github.com/pulsarpoint/corpscout/scheduler/internal/llmproviders"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type temporalWorkerResources struct {
	translationClient   *translationclient.Client
	translationActions  *brregactions.TranslationActions
	crawlClient         *crawlclient.Client
	domainSearchActions *brregactions.DomainSearchActions
}

func newTemporalWorkerResources(cfg config.Config, pool brregdb.TxPool, llmStore *llmproviders.Store) (*temporalWorkerResources, error) {
	slog.Debug("creating temporal worker resources")
	if cfg.NATSURL == "" {
		return nil, errors.New("CORPSCOUT_NATS_URL is required for temporal translation actions")
	}
	translator, err := translationclient.NewNATS(cfg.NATSURL)
	if err != nil {
		return nil, errors.Wrap(err, "create brreg translation nats client")
	}
	slog.Debug("created brreg translation nats client", "subject", translationclient.DefaultBrregTranslationSubject)
	crawler, err := crawlclient.NewNATS(cfg.NATSURL)
	if err != nil {
		translator.Close()
		return nil, errors.Wrap(err, "create brreg crawl nats client")
	}
	slog.Debug("created brreg crawl nats client",
		"search_fetch_subject", crawlclient.DefaultSearchFetchSubject,
		"search_analyze_subject", crawlclient.DefaultSearchAnalyzeSubject,
	)
	gateway := brregdb.New(pool)
	return &temporalWorkerResources{
		translationClient:   translator,
		translationActions:  brregactions.NewTranslationActions(gateway, translator, llmStore),
		crawlClient:         crawler,
		domainSearchActions: brregactions.NewDomainSearchActions(gateway, crawler, llmStore),
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
		newBrregTranslationTemporalWorker(temporalClient, resources),
		newBrregDomainSearchTemporalWorker(temporalClient, resources),
	}
}

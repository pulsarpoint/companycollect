package app

import (
	"log/slog"

	"github.com/cockroachdb/errors"
	"go.temporal.io/sdk/client"
	temporalworker "go.temporal.io/sdk/worker"

	brregactions "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/actions"
	brregdb "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/config"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

type temporalWorkerResources struct {
	translationClient  *translationclient.Client
	translationActions *brregactions.TranslationActions
}

func newTemporalWorkerResources(cfg config.Config, pool brregdb.TxPool) (*temporalWorkerResources, error) {
	slog.Debug("creating temporal worker resources")
	if cfg.NATSURL == "" {
		return nil, errors.New("CORPSCOUT_NATS_URL is required for temporal translation actions")
	}
	translator, err := translationclient.NewNATS(cfg.NATSURL)
	if err != nil {
		return nil, errors.Wrap(err, "create brreg translation nats client")
	}
	slog.Debug("created brreg translation nats client", "subject", translationclient.DefaultBrregTranslationSubject)
	gateway := brregdb.New(pool)
	return &temporalWorkerResources{
		translationClient:  translator,
		translationActions: brregactions.NewTranslationActions(gateway, translator),
	}, nil
}

func (r *temporalWorkerResources) Close() {
	if r != nil && r.translationClient != nil {
		slog.Debug("closing brreg translation nats client")
		r.translationClient.Close()
	}
}

func newTemporalWorkers(temporalClient client.Client, resources *temporalWorkerResources) []temporalworker.Worker {
	slog.Debug("creating temporal workers")
	return []temporalworker.Worker{
		newBrregTranslationTemporalWorker(temporalClient, resources),
	}
}

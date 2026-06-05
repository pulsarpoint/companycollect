package companydata

import (
	"context"

	"github.com/cockroachdb/errors"

	ariregisterdb "github.com/pulsarpoint/corpscout/scheduler/internal/ariregister/db"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

const (
	defaultPromptVersion = "v1"
	sourceName           = "ariregister"
	sourceLang           = "et"
	targetLang           = "en"
)

var ariregisterTranslationSourceConfig = sourcetranslation.SourceConfig{
	Source:               sourceName,
	SourceLang:           sourceLang,
	TargetLang:           targetLang,
	DefaultPromptVersion: defaultPromptVersion,
}

type Store struct {
	pool    ariregisterdb.TxPool
	gateway *ariregisterdb.Gateway
}

func New(pool ariregisterdb.TxPool) *Store {
	return &Store{pool: pool, gateway: ariregisterdb.New(pool)}
}

func (s *Store) RefreshTranslationStatus(ctx context.Context) error {
	if s == nil || s.gateway == nil {
		return errors.New("ariregister companydata store not available")
	}
	return s.gateway.RefreshCompanyTranslationStatus(ctx)
}

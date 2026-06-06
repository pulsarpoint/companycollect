package translationqueue

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/stretchr/testify/require"
)

func TestSourceRegistryFindsConfiguredSourcesAndRejectsUnknown(t *testing.T) {
	brreg := namedSourceQueue{name: "brreg"}
	ariregister := namedSourceQueue{name: "ariregister"}
	registry := NewSourceRegistry(nil, namedSourceQueue{name: ""}, brreg, brreg, ariregister)

	found, ok := registry.Source("brreg")
	require.True(t, ok)
	require.Equal(t, "brreg", found.Name())

	found, ok = registry.Get("ariregister")
	require.True(t, ok)
	require.Equal(t, "ariregister", found.Name())

	_, ok = registry.Get("unknown")
	require.False(t, ok)
}

func TestSourceRegistryListsSourcesInConfiguredOrder(t *testing.T) {
	brreg := namedSourceQueue{name: "brreg"}
	ariregister := namedSourceQueue{name: "ariregister"}
	ignoredDuplicate := namedSourceQueue{name: "brreg"}

	registry := NewSourceRegistry(ariregister, nil, namedSourceQueue{name: " "}, brreg, ignoredDuplicate)

	sources := registry.Sources()
	require.Len(t, sources, 2)
	require.Equal(t, "ariregister", sources[0].Name())
	require.Equal(t, "brreg", sources[1].Name())
	require.Equal(t, []string{"ariregister", "brreg"}, registry.Names())
}

type namedSourceQueue struct {
	name string
}

func (s namedSourceQueue) Name() string {
	return s.name
}

func (s namedSourceQueue) PrepareQueue(context.Context, PrepareQueueCommand) error {
	return nil
}

func (s namedSourceQueue) ClaimBatch(context.Context, ClaimBatchCommand) (ClaimBatchResult, error) {
	return ClaimBatchResult{}, nil
}

func (s namedSourceQueue) ReleaseBatch(context.Context, string) (QueueBatchResult, error) {
	return QueueBatchResult{}, nil
}

func (s namedSourceQueue) CompleteBatch(context.Context, string) (QueueBatchResult, error) {
	return QueueBatchResult{}, nil
}

func (s namedSourceQueue) ResetStale(context.Context, int32) (QueueBatchResult, error) {
	return QueueBatchResult{}, nil
}

func (s namedSourceQueue) LoadMissingFields(
	context.Context,
	sourcetranslation.LoadMissingFieldsCommand,
) ([]sourcetranslation.MissingField, error) {
	return nil, nil
}

func (s namedSourceQueue) LoadCachedTerms(
	context.Context,
	sourcetranslation.LoadCachedTermsCommand,
) (map[string]sourcetranslation.CachedTerm, error) {
	return nil, nil
}

func (s namedSourceQueue) SaveTerms(
	context.Context,
	sourcetranslation.SaveTermsCommand,
) (sourcetranslation.SaveTermsResult, error) {
	return sourcetranslation.SaveTermsResult{}, nil
}

func (s namedSourceQueue) ApplyTranslations(
	context.Context,
	sourcetranslation.ApplyCompanyTranslationsCommand,
) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	return sourcetranslation.ApplyCompanyTranslationsResult{}, nil
}

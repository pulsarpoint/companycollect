package translationqueue

import (
	"context"
	"strings"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type PrepareQueueCommand struct {
	IDs           []string
	Filters       map[string]string
	CompanyLimit  int32
	Provider      string
	Model         string
	PromptVersion string
	SourceLang    string
	TargetLang    string
}

type ClaimBatchCommand struct {
	BatchID          string
	MaxCandidateRows int32
	MaxRequestChars  int32
	MaxSourceRunning int32
}

type ClaimBatchResult struct {
	Status         string
	BatchID        string
	CompanyIDs     []string
	EstimatedChars int32
	Provider       string
	Model          string
	PromptVersion  string
	SourceLang     string
	TargetLang     string
}

type QueueBatchResult struct {
	RowsAffected int32
}

type SourceQueue interface {
	Name() string
	PrepareQueue(context.Context, PrepareQueueCommand) error
	ClaimBatch(context.Context, ClaimBatchCommand) (ClaimBatchResult, error)
	ReleaseBatch(context.Context, string) (QueueBatchResult, error)
	CompleteBatch(context.Context, string) (QueueBatchResult, error)
	ResetStale(context.Context, int32) (QueueBatchResult, error)
	LoadMissingFields(context.Context, sourcetranslation.LoadMissingFieldsCommand) ([]sourcetranslation.MissingField, error)
	LoadCachedTerms(context.Context, sourcetranslation.LoadCachedTermsCommand) (map[string]sourcetranslation.CachedTerm, error)
	SaveTerms(context.Context, sourcetranslation.SaveTermsCommand) (sourcetranslation.SaveTermsResult, error)
	ApplyTranslations(context.Context, sourcetranslation.ApplyCompanyTranslationsCommand) (sourcetranslation.ApplyCompanyTranslationsResult, error)
}

type SourceRegistry struct {
	sources []SourceQueue
	byName  map[string]SourceQueue
}

func NewSourceRegistry(sources ...SourceQueue) SourceRegistry {
	registry := SourceRegistry{
		sources: make([]SourceQueue, 0, len(sources)),
		byName:  make(map[string]SourceQueue),
	}
	for _, source := range sources {
		if source == nil {
			continue
		}
		name := strings.TrimSpace(source.Name())
		if name == "" {
			continue
		}
		if _, exists := registry.byName[name]; exists {
			continue
		}
		registry.sources = append(registry.sources, source)
		registry.byName[name] = source
	}
	return registry
}

func (r SourceRegistry) Source(name string) (SourceQueue, bool) {
	source, ok := r.byName[strings.TrimSpace(name)]
	return source, ok
}

func (r SourceRegistry) Get(name string) (SourceQueue, bool) {
	return r.Source(name)
}

func (r SourceRegistry) Sources() []SourceQueue {
	sources := make([]SourceQueue, len(r.sources))
	copy(sources, r.sources)
	return sources
}

func (r SourceRegistry) Names() []string {
	names := make([]string, 0, len(r.sources))
	for _, source := range r.sources {
		names = append(names, source.Name())
	}
	return names
}

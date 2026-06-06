package translationqueue

import (
	"context"
	"log/slog"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
)

type JobPublisher interface {
	PublishJob(context.Context, TranslationJob) error
}

type DispatcherConfig struct {
	SourceBufferTarget int32
	MaxCandidateRows   int32
	MaxRequestChars    int32
}

type Dispatcher struct {
	registry  SourceRegistry
	publisher JobPublisher
	config    DispatcherConfig
}

func NewDispatcher(registry SourceRegistry, publisher JobPublisher, config DispatcherConfig) *Dispatcher {
	if config.SourceBufferTarget <= 0 {
		config.SourceBufferTarget = 2
	}
	if config.MaxCandidateRows <= 0 {
		config.MaxCandidateRows = 25
	}
	if config.MaxRequestChars <= 0 {
		config.MaxRequestChars = 6000
	}
	return &Dispatcher{registry: registry, publisher: publisher, config: config}
}

func (d *Dispatcher) RefillOnce(ctx context.Context) error {
	if d == nil {
		return errors.New("translation dispatcher is required")
	}
	for _, source := range d.registry.Sources() {
		if err := d.RefillSource(ctx, source); err != nil {
			return errors.Wrapf(err, "refill %s translation source buffer", source.Name())
		}
	}
	return nil
}

func (d *Dispatcher) RefillSource(ctx context.Context, source SourceQueue) error {
	if d == nil {
		return errors.New("translation dispatcher is required")
	}
	if source == nil {
		return errors.New("translation source queue is required")
	}
	for attempts := int32(0); attempts < d.config.SourceBufferTarget; attempts++ {
		shouldContinue, err := d.refillSourceBatch(ctx, source)
		if err != nil {
			return err
		}
		if !shouldContinue {
			return nil
		}
	}
	return nil
}

func (d *Dispatcher) refillSourceBatch(ctx context.Context, source SourceQueue) (bool, error) {
	claimed, err := source.ClaimBatch(ctx, ClaimBatchCommand{
		BatchID:          uuid.NewString(),
		MaxCandidateRows: d.config.MaxCandidateRows,
		MaxRequestChars:  d.config.MaxRequestChars,
		MaxSourceRunning: d.config.SourceBufferTarget,
	})
	if err != nil {
		return false, errors.Wrap(err, "claim translation batch")
	}
	if strings.TrimSpace(claimed.Status) != "claimed" || len(claimed.CompanyIDs) == 0 {
		return false, nil
	}
	if strings.TrimSpace(claimed.BatchID) == "" {
		return false, errors.New("claimed translation batch id is required")
	}

	fields, err := source.LoadMissingFields(ctx, sourcetranslation.LoadMissingFieldsCommand{
		PromptVersion: claimed.PromptVersion,
		CompanyIDs:    claimed.CompanyIDs,
	})
	if err != nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return false, errors.Wrap(err, "load translation batch missing fields")
	}
	if len(fields) == 0 {
		if _, err := source.CompleteBatch(ctx, claimed.BatchID); err != nil {
			return false, errors.Wrap(err, "complete empty translation batch")
		}
		return true, nil
	}

	cached, err := source.LoadCachedTerms(ctx, sourcetranslation.LoadCachedTermsCommand{
		PromptVersion: claimed.PromptVersion,
		SourceLang:    claimed.SourceLang,
		TargetLang:    claimed.TargetLang,
		TermKeys:      sourcetranslation.TranslationTermKeys(fields),
	})
	if err != nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return false, errors.Wrap(err, "load cached translation terms")
	}
	built := sourcetranslation.BuildTranslationQueueTerms(fields, cached)
	if len(built.CachedBindings) > 0 {
		applied, err := applyTranslationBindingsByCompany(ctx, source, built.CachedBindings)
		if err != nil {
			_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
			return false, errors.Wrap(err, "apply cached translation bindings")
		}
		slog.DebugContext(ctx, "applied cached translation bindings before dispatch",
			"source", source.Name(),
			"batch_id", claimed.BatchID,
			"bindings_applied", applied,
		)
	}
	if len(built.UncachedTerms) == 0 {
		if _, err := source.CompleteBatch(ctx, claimed.BatchID); err != nil {
			return false, errors.Wrap(err, "complete cached-only translation batch")
		}
		return true, nil
	}

	job := buildTranslationJob(source.Name(), claimed, built.UncachedTerms)
	if d.publisher == nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return false, errors.New("translation job publisher is required")
	}
	if err := d.publisher.PublishJob(ctx, job); err != nil {
		_, _ = source.ReleaseBatch(ctx, claimed.BatchID)
		return false, errors.Wrap(err, "publish translation job")
	}
	return true, nil
}

func buildTranslationJob(
	source string,
	claimed ClaimBatchResult,
	terms []sourcetranslation.TranslationTerm,
) TranslationJob {
	job := TranslationJob{
		JobID:         uuid.NewString(),
		BatchID:       claimed.BatchID,
		Source:        source,
		SourceLang:    claimed.SourceLang,
		TargetLang:    claimed.TargetLang,
		Provider:      claimed.Provider,
		Model:         claimed.Model,
		PromptVersion: claimed.PromptVersion,
		CompanyIDs:    append([]string(nil), claimed.CompanyIDs...),
		Terms:         make([]TranslationJobTerm, 0, len(terms)),
	}
	for _, term := range terms {
		job.Terms = append(job.Terms, TranslationJobTerm{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
		})
	}
	return job
}

func applyTranslationBindingsByCompany(
	ctx context.Context,
	source SourceQueue,
	bindings []sourcetranslation.TranslationBinding,
) (int32, error) {
	groups := make(map[string][]sourcetranslation.TranslationBinding)
	companyIDs := make([]string, 0)
	for _, binding := range bindings {
		companyID := strings.TrimSpace(binding.CompanyID)
		if companyID == "" {
			continue
		}
		if _, exists := groups[companyID]; !exists {
			companyIDs = append(companyIDs, companyID)
		}
		groups[companyID] = append(groups[companyID], binding)
	}
	var totalApplied int32
	for _, companyID := range companyIDs {
		applied, err := source.ApplyTranslations(ctx, sourcetranslation.ApplyCompanyTranslationsCommand{
			CompanyID: companyID,
			Bindings:  groups[companyID],
		})
		if err != nil {
			return 0, err
		}
		totalApplied += applied.BindingsApplied
	}
	return totalApplied, nil
}

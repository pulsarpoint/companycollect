package translationqueue

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/stretchr/testify/require"
)

func TestDispatcherPublishesClaimedBatchWithUncachedTerms(t *testing.T) {
	source := &dispatcherSourceStub{
		name: "brreg",
		claim: ClaimBatchResult{
			Status:        "claimed",
			BatchID:       "batch-1",
			CompanyIDs:    []string{"company-a"},
			Provider:      "default",
			PromptVersion: "v1",
			SourceLang:    "no",
			TargetLang:    "en",
		},
		fields: []sourcetranslation.MissingField{dispatcherMissingField("company-a", "a", "Aksjeselskap")},
	}
	publisher := &dispatcherJobPublisherStub{}
	dispatcher := NewDispatcher(SourceRegistry{}, publisher, DispatcherConfig{
		SourceBufferTarget: 1,
		MaxCandidateRows:   25,
		MaxRequestChars:    6000,
	})

	err := dispatcher.RefillSource(context.Background(), source)
	require.NoError(t, err)
	require.Len(t, publisher.jobs, 1)
	require.Equal(t, "batch-1", publisher.jobs[0].BatchID)
	require.Equal(t, "brreg", publisher.jobs[0].Source)
	require.Equal(t, []string{"company-a"}, publisher.jobs[0].CompanyIDs)
	require.Equal(t, "Aksjeselskap", publisher.jobs[0].Terms[0].SourceText)
	require.NotEmpty(t, publisher.jobs[0].Terms[0].SourceTextNormalized)
}

func TestDispatcherCompletesCachedOnlyBatchWithoutPublishing(t *testing.T) {
	source := &dispatcherSourceStub{
		name: "brreg",
		claim: ClaimBatchResult{
			Status:        "claimed",
			BatchID:       "batch-1",
			CompanyIDs:    []string{"company-a", "company-b"},
			Provider:      "default",
			PromptVersion: "v1",
			SourceLang:    "no",
			TargetLang:    "en",
		},
		fields: []sourcetranslation.MissingField{
			dispatcherMissingField("company-a", "a", "Aksjeselskap"),
			dispatcherMissingField("company-b", "b", "Stiftelse"),
		},
		cached: map[string]sourcetranslation.CachedTerm{
			"a": {TermKey: "a", TranslatedText: "Limited liability company"},
			"b": {TermKey: "b", TranslatedText: "Foundation"},
		},
	}
	publisher := &dispatcherJobPublisherStub{}
	dispatcher := NewDispatcher(SourceRegistry{}, publisher, DispatcherConfig{
		SourceBufferTarget: 1,
		MaxCandidateRows:   25,
		MaxRequestChars:    6000,
	})

	err := dispatcher.RefillSource(context.Background(), source)
	require.NoError(t, err)
	require.Empty(t, publisher.jobs)
	require.Equal(t, int32(2), source.applied)
	require.Len(t, source.appliedCommands, 2)
	require.Equal(t, "company-a", source.appliedCommands[0].CompanyID)
	require.Equal(t, "company-b", source.appliedCommands[1].CompanyID)
	require.Equal(t, int32(1), source.completed)
	require.Equal(t, int32(0), source.released)
}

func TestDispatcherRefillsUntilSourceBufferTarget(t *testing.T) {
	source := &dispatcherSourceStub{
		name: "brreg",
		claims: []ClaimBatchResult{
			{
				Status:        "claimed",
				CompanyIDs:    []string{"company-a"},
				Provider:      "default",
				PromptVersion: "v1",
				SourceLang:    "no",
				TargetLang:    "en",
			},
			{
				Status:        "claimed",
				CompanyIDs:    []string{"company-b"},
				Provider:      "default",
				PromptVersion: "v1",
				SourceLang:    "no",
				TargetLang:    "en",
			},
		},
		fieldsByCompany: map[string][]sourcetranslation.MissingField{
			"company-a": []sourcetranslation.MissingField{dispatcherMissingField("company-a", "a", "Aksjeselskap")},
			"company-b": []sourcetranslation.MissingField{dispatcherMissingField("company-b", "b", "Stiftelse")},
		},
	}
	publisher := &dispatcherJobPublisherStub{}
	dispatcher := NewDispatcher(SourceRegistry{}, publisher, DispatcherConfig{SourceBufferTarget: 2})

	err := dispatcher.RefillSource(context.Background(), source)
	require.NoError(t, err)
	require.Len(t, publisher.jobs, 2)
	require.Len(t, source.claimCommands, 2)
	require.NotEmpty(t, publisher.jobs[0].BatchID)
	require.NotEmpty(t, publisher.jobs[1].BatchID)
	require.NotEqual(t, publisher.jobs[0].BatchID, publisher.jobs[1].BatchID)
	require.Equal(t, int32(2), source.claimCommands[0].MaxSourceRunning)
}

func TestDispatcherReleasesClaimedBatchWhenPublishFails(t *testing.T) {
	source := &dispatcherSourceStub{
		name: "brreg",
		claim: ClaimBatchResult{
			Status:        "claimed",
			BatchID:       "batch-1",
			CompanyIDs:    []string{"company-a"},
			Provider:      "default",
			PromptVersion: "v1",
			SourceLang:    "no",
			TargetLang:    "en",
		},
		fields: []sourcetranslation.MissingField{dispatcherMissingField("company-a", "a", "Aksjeselskap")},
	}
	publisher := &dispatcherJobPublisherStub{fail: stderrors.New("publish failed")}
	dispatcher := NewDispatcher(SourceRegistry{}, publisher, DispatcherConfig{SourceBufferTarget: 1})

	err := dispatcher.RefillSource(context.Background(), source)
	require.ErrorContains(t, err, "publish translation job")
	require.Equal(t, int32(1), source.released)
	require.Equal(t, []string{"batch-1"}, source.releasedBatchIDs)
}

func dispatcherMissingField(companyID, termKey, sourceText string) sourcetranslation.MissingField {
	return sourcetranslation.MissingField{
		CompanyID:            companyID,
		SourceTable:          "brreg_source.companies",
		SourceRowID:          companyID,
		SourceColumn:         "organization_form_label",
		TargetColumn:         "organization_form_label_en",
		SourceText:           sourceText,
		SourceTextNormalized: sourcetranslation.NormalizeText(sourceText),
		TermKey:              termKey,
	}
}

type dispatcherSourceStub struct {
	SourceQueue
	name             string
	claim            ClaimBatchResult
	claims           []ClaimBatchResult
	claimCommands    []ClaimBatchCommand
	fields           []sourcetranslation.MissingField
	fieldsByCompany  map[string][]sourcetranslation.MissingField
	cached           map[string]sourcetranslation.CachedTerm
	applied          int32
	appliedCommands  []sourcetranslation.ApplyCompanyTranslationsCommand
	completed        int32
	released         int32
	releasedBatchIDs []string
}

func (s *dispatcherSourceStub) Name() string {
	return s.name
}

func (s *dispatcherSourceStub) ClaimBatch(_ context.Context, command ClaimBatchCommand) (ClaimBatchResult, error) {
	s.claimCommands = append(s.claimCommands, command)
	var result ClaimBatchResult
	if len(s.claims) > 0 {
		result = s.claims[0]
		s.claims = s.claims[1:]
	} else if s.claim.Status != "" {
		result = s.claim
		s.claim = ClaimBatchResult{}
	} else {
		return ClaimBatchResult{Status: "drained", BatchID: command.BatchID}, nil
	}
	if result.BatchID == "" {
		result.BatchID = command.BatchID
	}
	return result, nil
}

func (s *dispatcherSourceStub) LoadMissingFields(
	_ context.Context,
	command sourcetranslation.LoadMissingFieldsCommand,
) ([]sourcetranslation.MissingField, error) {
	if len(s.fieldsByCompany) == 0 {
		return append([]sourcetranslation.MissingField(nil), s.fields...), nil
	}
	fields := make([]sourcetranslation.MissingField, 0)
	for _, companyID := range command.CompanyIDs {
		fields = append(fields, s.fieldsByCompany[companyID]...)
	}
	return fields, nil
}

func (s *dispatcherSourceStub) LoadCachedTerms(
	context.Context,
	sourcetranslation.LoadCachedTermsCommand,
) (map[string]sourcetranslation.CachedTerm, error) {
	if s.cached == nil {
		return map[string]sourcetranslation.CachedTerm{}, nil
	}
	return s.cached, nil
}

func (s *dispatcherSourceStub) ApplyTranslations(
	_ context.Context,
	command sourcetranslation.ApplyCompanyTranslationsCommand,
) (sourcetranslation.ApplyCompanyTranslationsResult, error) {
	s.appliedCommands = append(s.appliedCommands, sourcetranslation.ApplyCompanyTranslationsCommand{
		CompanyID: command.CompanyID,
		Bindings:  append([]sourcetranslation.TranslationBinding(nil), command.Bindings...),
	})
	s.applied += int32(len(command.Bindings))
	return sourcetranslation.ApplyCompanyTranslationsResult{
		BindingsApplied: int32(len(command.Bindings)),
	}, nil
}

func (s *dispatcherSourceStub) CompleteBatch(context.Context, string) (QueueBatchResult, error) {
	s.completed++
	return QueueBatchResult{RowsAffected: 1}, nil
}

func (s *dispatcherSourceStub) ReleaseBatch(_ context.Context, batchID string) (QueueBatchResult, error) {
	s.released++
	s.releasedBatchIDs = append(s.releasedBatchIDs, batchID)
	return QueueBatchResult{RowsAffected: 1}, nil
}

type dispatcherJobPublisherStub struct {
	jobs []TranslationJob
	fail error
}

func (p *dispatcherJobPublisherStub) PublishJob(_ context.Context, job TranslationJob) error {
	if p.fail != nil {
		return p.fail
	}
	p.jobs = append(p.jobs, job)
	return nil
}

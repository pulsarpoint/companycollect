package translationqueue

import (
	"context"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/stretchr/testify/require"
)

func TestResultCollectorSavesAppliesAndCompletesSuccessfulResult(t *testing.T) {
	source := &collectorSourceStub{
		name: "brreg",
		fields: []sourcetranslation.MissingField{
			collectorMissingField("company-a", "a", "Aksjeselskap"),
		},
	}
	collector := NewResultCollector(NewSourceRegistry(source))

	err := collector.HandleResult(context.Background(), TranslationResult{
		BatchID:       "batch-1",
		Source:        "brreg",
		Status:        "succeeded",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Results: []TranslationResultTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), source.saved)
	require.Equal(t, int32(1), source.applied)
	require.Equal(t, int32(1), source.completed)
	require.Zero(t, source.released)
}

func TestResultCollectorAppliesByCompanyForSuccessfulResult(t *testing.T) {
	source := &collectorSourceStub{
		name: "brreg",
		fields: []sourcetranslation.MissingField{
			collectorMissingField("company-a", "a", "Aksjeselskap"),
			collectorMissingField("company-b", "a", "Aksjeselskap"),
		},
	}
	collector := NewResultCollector(NewSourceRegistry(source))

	err := collector.HandleResult(context.Background(), TranslationResult{
		BatchID:       "batch-1",
		Source:        "brreg",
		Status:        "succeeded",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a", "company-b"},
		Results: []TranslationResultTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, int32(2), source.applied)
	require.Len(t, source.appliedCommands, 2)
	require.Equal(t, "company-a", source.appliedCommands[0].CompanyID)
	require.Equal(t, "company-b", source.appliedCommands[1].CompanyID)
	require.Equal(t, int32(1), source.completed)
}

func TestResultCollectorReleasesPartialResultAfterApplyingSuccesses(t *testing.T) {
	source := &collectorSourceStub{
		name: "brreg",
		fields: []sourcetranslation.MissingField{
			collectorMissingField("company-a", "a", "Aksjeselskap"),
			collectorMissingField("company-a", "b", "Ukjent"),
		},
	}
	collector := NewResultCollector(NewSourceRegistry(source))

	err := collector.HandleResult(context.Background(), TranslationResult{
		BatchID:       "batch-1",
		Source:        "brreg",
		Status:        "partial",
		Provider:      "default",
		PromptVersion: "v1",
		CompanyIDs:    []string{"company-a"},
		Results: []TranslationResultTerm{{
			TermKey:              "a",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}},
		Failures: []TranslationFailureResult{{
			TermKey:              "b",
			SourceText:           "Ukjent",
			SourceTextNormalized: "ukjent",
			Status:               "failed_retryable",
			ErrorCode:            "llm_timeout",
			Error:                "provider timed out",
		}},
	})

	require.NoError(t, err)
	require.Equal(t, int32(2), source.saved)
	require.Equal(t, int32(1), source.applied)
	require.Equal(t, int32(1), source.released)
	require.Zero(t, source.completed)
}

func TestResultCollectorReleasesWholeBatchFailure(t *testing.T) {
	source := &collectorSourceStub{name: "brreg"}
	collector := NewResultCollector(NewSourceRegistry(source))

	err := collector.HandleResult(context.Background(), TranslationResult{
		BatchID: "batch-1",
		Source:  "brreg",
		Status:  "failed",
	})

	require.NoError(t, err)
	require.Equal(t, int32(1), source.released)
	require.Zero(t, source.completed)
}

func collectorMissingField(companyID, termKey, sourceText string) sourcetranslation.MissingField {
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

type collectorSourceStub struct {
	SourceQueue
	name             string
	fields           []sourcetranslation.MissingField
	saved            int32
	savedCommands    []sourcetranslation.SaveTermsCommand
	applied          int32
	appliedCommands  []sourcetranslation.ApplyCompanyTranslationsCommand
	completed        int32
	released         int32
	releasedBatchIDs []string
}

func (s *collectorSourceStub) Name() string {
	return s.name
}

func (s *collectorSourceStub) SaveTerms(
	_ context.Context,
	command sourcetranslation.SaveTermsCommand,
) (sourcetranslation.SaveTermsResult, error) {
	s.savedCommands = append(s.savedCommands, sourcetranslation.SaveTermsCommand{
		PromptVersion: command.PromptVersion,
		SourceLang:    command.SourceLang,
		TargetLang:    command.TargetLang,
		Terms:         append([]sourcetranslation.TranslationTermResult(nil), command.Terms...),
	})
	s.saved += int32(len(command.Terms))
	return sourcetranslation.SaveTermsResult{TermsSaved: int32(len(command.Terms))}, nil
}

func (s *collectorSourceStub) LoadMissingFields(
	context.Context,
	sourcetranslation.LoadMissingFieldsCommand,
) ([]sourcetranslation.MissingField, error) {
	return append([]sourcetranslation.MissingField(nil), s.fields...), nil
}

func (s *collectorSourceStub) ApplyTranslations(
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

func (s *collectorSourceStub) CompleteBatch(context.Context, string) (QueueBatchResult, error) {
	s.completed++
	return QueueBatchResult{RowsAffected: 1}, nil
}

func (s *collectorSourceStub) ReleaseBatch(_ context.Context, batchID string) (QueueBatchResult, error) {
	s.released++
	s.releasedBatchIDs = append(s.releasedBatchIDs, batchID)
	return QueueBatchResult{RowsAffected: 1}, nil
}

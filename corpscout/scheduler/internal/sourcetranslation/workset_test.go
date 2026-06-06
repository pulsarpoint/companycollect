package sourcetranslation

import (
	"context"
	"database/sql"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	_ "modernc.org/sqlite"
)

func TestBuildTranslationWorksetWritesCachedAndPendingTerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "organization_form", "organization_form_en", "Aksjeselskap"),
			missingField("company-1", "source.industries", "industry-1", "source_label", "source_label_en", "Aksjeselskap"),
			missingField("company-2", "source.capital", "capital-1", "capital_type", "capital_type_en", "Aksjekapital"),
		},
		cachedTerms: map[string]CachedTerm{
			TermKey("Aksjekapital"): {
				TermKey:        TermKey("Aksjekapital"),
				TranslatedText: "Share capital",
			},
		},
	}

	result, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v2",
		CompanyIDs:    []string{" company-1 ", "company-2"},
		Filters:       map[string]string{" status ": " missing "},
		CompanyLimit:  10,
		FieldLimit:    20,
	})

	require.NoError(t, err)
	require.Equal(t, path, result.Path)
	require.EqualValues(t, 3, result.FieldsExported)
	require.EqualValues(t, 2, result.TermsExported)
	require.EqualValues(t, 2, result.CompaniesExported)
	require.EqualValues(t, 1, result.CachedFields)
	require.Equal(t, 1, store.refreshCount)
	require.Equal(t, LoadMissingFieldsCommand{
		PromptVersion: "prompt-v2",
		CompanyIDs:    []string{"company-1", "company-2"},
		Filters:       map[string]string{"status": "missing"},
		CompanyLimit:  10,
		FieldLimit:    20,
	}, store.loadMissingCommand)
	require.Equal(t, sortedValues([]string{TermKey("Aksjeselskap"), TermKey("Aksjekapital")}), sortedValues(store.loadCachedCommand.TermKeys))

	db := openSourceTranslationWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 2, countWorksetRows(t, db, "translation_terms"))
	require.EqualValues(t, 3, countWorksetRows(t, db, "translation_bindings"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'succeeded'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'pending'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'cached'"))
	require.EqualValues(t, 2, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'pending'"))
	require.Equal(t, "test-source", worksetMetadataValue(t, db, "source"))
	require.Equal(t, "no", worksetMetadataValue(t, db, "source_lang"))
	require.Equal(t, "en", worksetMetadataValue(t, db, "target_lang"))
	require.Equal(t, "prompt-v2", worksetMetadataValue(t, db, "prompt_version"))

	var cachedText string
	err = db.QueryRowContext(t.Context(), `
SELECT translated_text
FROM translation_terms
WHERE term_key = ?
`, TermKey("Aksjekapital")).Scan(&cachedText)
	require.NoError(t, err)
	require.Equal(t, "Share capital", cachedText)
}

func TestClaimTranslationWorksetBatchPacksTermsByBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "response_class", "response_class_en", "Kort"),
			missingField("company-2", "source.companies", "company-2", "activity_description", "activity_description_en", "Dette er en lengre tekst"),
			missingField("company-3", "source.capital", "capital-3", "capital_type", "capital_type_en", "Aksjekapital"),
		},
	}
	_, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)

	claimed, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            path,
		MaxRequestChars: 20,
		MaxTerms:        10,
	})

	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	require.NotZero(t, claimed.BatchID)
	require.Len(t, claimed.Terms, 1)
	require.Equal(t, "Aksjekapital", claimed.Terms[0].SourceText)
	require.EqualValues(t, len([]rune("Aksjekapital")), claimed.EstimatedChars)

	db := openSourceTranslationWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'running'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_batches", "status = 'running'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "term_key = '"+TermKey("Aksjekapital")+"' AND attempt_count = 1"))

	oversizedPath := filepath.Join(t.TempDir(), "oversized-translation-workset.sqlite")
	oversizedStore := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "activity_description", "activity_description_en", "Dette er mye lengre enn grensen"),
		},
	}
	_, err = BuildWorkset(t.Context(), oversizedStore, testSourceConfig(), BuildWorksetCommand{
		Path:          oversizedPath,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)

	oversized, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            oversizedPath,
		MaxRequestChars: 5,
		MaxTerms:        10,
	})

	require.NoError(t, err)
	require.Equal(t, "claimed", oversized.Status)
	require.Len(t, oversized.Terms, 1)
	require.Greater(t, oversized.EstimatedChars, int32(5))
}

func TestSaveTranslationWorksetBatchUpdatesBindings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "response_class", "response_class_en", "Enhet"),
			missingField("company-2", "source.capital", "capital-2", "capital_type", "capital_type_en", "Aksjekapital"),
		},
	}
	_, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)
	claimed, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
	})
	require.NoError(t, err)
	require.Len(t, claimed.Terms, 2)

	result, err := SaveBatch(t.Context(), SaveBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "prompt-v1",
		Results: []TranslationTermResult{{
			TermKey:              TermKey("Enhet"),
			SourceText:           "Enhet",
			SourceTextNormalized: NormalizeText("Enhet"),
			TranslatedText:       "Entity",
			Status:               "succeeded",
		}, {
			TermKey:    TermKey("Aksjekapital"),
			SourceText: "Aksjekapital",
			Status:     "failed",
			ErrorCode:  "temporary",
		}},
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.TermsSucceeded)
	require.EqualValues(t, 1, result.TermsFailed)

	db := openSourceTranslationWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'succeeded'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'failed_retryable'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'translated'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'failed'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_batches", "status = 'succeeded'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "term_key = '"+TermKey("Enhet")+"' AND provider = 'mock' AND model = 'mock-fast' AND translated_text = 'Entity'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_bindings", "term_key = '"+TermKey("Aksjekapital")+"' AND error = 'temporary'"))
}

func TestSaveTranslationWorksetBatchRejectsWrongBatchID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "response_class", "response_class_en", "Enhet"),
		},
	}
	_, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)
	claimed, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
	})
	require.NoError(t, err)

	_, err = SaveBatch(t.Context(), SaveBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID + 1,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "prompt-v1",
		Results: []TranslationTermResult{{
			TermKey:        TermKey("Enhet"),
			SourceText:     "Enhet",
			TranslatedText: "Entity",
			Status:         "succeeded",
		}},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "does not contain running term")

	db := openSourceTranslationWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'running'"))
	require.EqualValues(t, 0, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'translated'"))
}

func TestSaveTranslationWorksetBatchRejectsMissingClaimedTermResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "response_class", "response_class_en", "Enhet"),
			missingField("company-2", "source.capital", "capital-2", "capital_type", "capital_type_en", "Aksjekapital"),
		},
	}
	_, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)
	claimed, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
	})
	require.NoError(t, err)
	require.Len(t, claimed.Terms, 2)

	_, err = SaveBatch(t.Context(), SaveBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "prompt-v1",
		Results: []TranslationTermResult{{
			TermKey:        TermKey("Enhet"),
			SourceText:     "Enhet",
			TranslatedText: "Entity",
			Status:         "succeeded",
		}},
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsaved running terms")

	db := openSourceTranslationWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 2, countWorksetRowsWhere(t, db, "translation_terms", "status = 'running'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_batches", "status = 'running'"))
	require.EqualValues(t, 0, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'translated'"))
}

func TestApplyTranslationWorksetGroupsBindingsByCompany(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.capital", "capital-1", "capital_type", "capital_type_en", "Aksjekapital"),
			missingField("company-1", "source.companies", "company-1", "organization_form", "organization_form_en", "Aksjeselskap"),
			missingField("company-2", "source.companies", "company-2", "response_class", "response_class_en", "Enhet"),
			missingField("company-2", "source.companies", "company-2", "temporary_label", "temporary_label_en", "Midlertidig"),
		},
		cachedTerms: map[string]CachedTerm{
			TermKey("Aksjekapital"): {
				TermKey:        TermKey("Aksjekapital"),
				TranslatedText: "Share capital",
			},
		},
	}
	_, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)
	claimed, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            path,
		MaxRequestChars: 1000,
		MaxTerms:        10,
	})
	require.NoError(t, err)
	require.Len(t, claimed.Terms, 3)
	_, err = SaveBatch(t.Context(), SaveBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "prompt-v1",
		Results: []TranslationTermResult{{
			TermKey:              TermKey("Aksjeselskap"),
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: NormalizeText("Aksjeselskap"),
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
		}, {
			TermKey:              TermKey("Enhet"),
			SourceText:           "Enhet",
			SourceTextNormalized: NormalizeText("Enhet"),
			TranslatedText:       "Entity",
			Status:               "succeeded",
		}, {
			TermKey:    TermKey("Midlertidig"),
			SourceText: "Midlertidig",
			Status:     "failed_terminal",
			Error:      "unsupported",
		}},
	})
	require.NoError(t, err)

	result, err := ApplyWorkset(t.Context(), store, ApplyWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})

	require.NoError(t, err)
	require.EqualValues(t, 4, result.TermsSaved)
	require.EqualValues(t, 3, result.BindingsApplied)
	require.Len(t, store.savedTerms, 1)
	require.Len(t, store.savedTerms[0].Terms, 4)
	require.Len(t, store.appliedCompanyCommands, 2)
	require.Equal(t, "company-1", store.appliedCompanyCommands[0].CompanyID)
	require.Len(t, store.appliedCompanyCommands[0].Bindings, 2)
	require.Equal(t, "company-2", store.appliedCompanyCommands[1].CompanyID)
	require.Len(t, store.appliedCompanyCommands[1].Bindings, 1)
	require.Equal(t, 2, store.refreshCount)

	db := openSourceTranslationWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 3, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'applied'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_bindings", "status = 'failed'"))
	require.EqualValues(t, 1, countWorksetRowsWhere(t, db, "translation_terms", "status = 'failed_terminal'"))
}

func TestApplyTranslationWorksetRejectsOutOfGroupAppliedBindingIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	store := &fakeSourceStore{
		missingFields: []MissingField{
			missingField("company-1", "source.companies", "company-1", "response_class", "response_class_en", "Enhet"),
		},
		appliedBindingIDs: []int64{999},
	}
	_, err := BuildWorkset(t.Context(), store, testSourceConfig(), BuildWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})
	require.NoError(t, err)
	claimed, err := ClaimBatch(t.Context(), ClaimBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
	})
	require.NoError(t, err)
	_, err = SaveBatch(t.Context(), SaveBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "prompt-v1",
		Results: []TranslationTermResult{{
			TermKey:        TermKey("Enhet"),
			SourceText:     "Enhet",
			TranslatedText: "Entity",
			Status:         "succeeded",
		}},
	})
	require.NoError(t, err)

	_, err = ApplyWorkset(t.Context(), store, ApplyWorksetCommand{
		Path:          path,
		PromptVersion: "prompt-v1",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "out-of-group binding id")
}

type fakeSourceStore struct {
	missingFields []MissingField
	cachedTerms   map[string]CachedTerm

	refreshCount           int
	loadMissingCommand     LoadMissingFieldsCommand
	loadCachedCommand      LoadCachedTermsCommand
	savedTerms             []SaveTermsCommand
	appliedCompanyCommands []ApplyCompanyTranslationsCommand
	appliedBindingIDs      []int64
}

func (s *fakeSourceStore) LoadMissingTranslationFields(
	_ context.Context,
	command LoadMissingFieldsCommand,
) ([]MissingField, error) {
	s.loadMissingCommand = command
	return append([]MissingField(nil), s.missingFields...), nil
}

func (s *fakeSourceStore) LoadCachedTranslationTerms(
	_ context.Context,
	command LoadCachedTermsCommand,
) (map[string]CachedTerm, error) {
	s.loadCachedCommand = LoadCachedTermsCommand{
		PromptVersion: command.PromptVersion,
		SourceLang:    command.SourceLang,
		TargetLang:    command.TargetLang,
		TermKeys:      append([]string(nil), command.TermKeys...),
	}
	terms := make(map[string]CachedTerm)
	for _, key := range command.TermKeys {
		if term, ok := s.cachedTerms[key]; ok {
			terms[key] = term
		}
	}
	return terms, nil
}

func (s *fakeSourceStore) SaveTranslationTerms(
	_ context.Context,
	command SaveTermsCommand,
) (SaveTermsResult, error) {
	s.savedTerms = append(s.savedTerms, SaveTermsCommand{
		PromptVersion: command.PromptVersion,
		SourceLang:    command.SourceLang,
		TargetLang:    command.TargetLang,
		Terms:         append([]TranslationTermResult(nil), command.Terms...),
	})
	return SaveTermsResult{TermsSaved: int32(len(command.Terms))}, nil
}

func (s *fakeSourceStore) ApplyCompanyTranslations(
	_ context.Context,
	command ApplyCompanyTranslationsCommand,
) (ApplyCompanyTranslationsResult, error) {
	s.appliedCompanyCommands = append(s.appliedCompanyCommands, ApplyCompanyTranslationsCommand{
		CompanyID: command.CompanyID,
		Bindings:  append([]TranslationBinding(nil), command.Bindings...),
	})
	appliedIDs := make([]int64, 0, len(command.Bindings))
	for _, binding := range command.Bindings {
		appliedIDs = append(appliedIDs, binding.ID)
	}
	if len(s.appliedBindingIDs) > 0 {
		appliedIDs = append([]int64(nil), s.appliedBindingIDs...)
	}
	return ApplyCompanyTranslationsResult{
		BindingsApplied:   int32(len(appliedIDs)),
		AppliedBindingIDs: appliedIDs,
	}, nil
}

func (s *fakeSourceStore) RefreshTranslationStatus(_ context.Context) error {
	s.refreshCount++
	return nil
}

func testSourceConfig() SourceConfig {
	return SourceConfig{
		Source:               "test-source",
		SourceLang:           "no",
		TargetLang:           "en",
		DefaultPromptVersion: "prompt-v1",
	}
}

func missingField(
	companyID string,
	sourceTable string,
	sourceRowID string,
	sourceColumn string,
	targetColumn string,
	sourceText string,
) MissingField {
	return MissingField{
		CompanyID:            companyID,
		SourceTable:          sourceTable,
		SourceRowID:          sourceRowID,
		SourceColumn:         sourceColumn,
		TargetColumn:         targetColumn,
		SourceText:           sourceText,
		SourceTextNormalized: NormalizeText(sourceText),
		TermKey:              TermKey(sourceText),
	}
}

func sortedValues(values []string) []string {
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return sorted
}

func openSourceTranslationWorksetDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	return db
}

func countWorksetRows(t *testing.T, db *sql.DB, table string) int64 {
	t.Helper()
	return countWorksetRowsWhere(t, db, table, "true")
}

func countWorksetRowsWhere(t *testing.T, db *sql.DB, table string, where string) int64 {
	t.Helper()
	var count int64
	err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table+" WHERE "+where).Scan(&count)
	require.NoError(t, err)
	return count
}

func worksetMetadataValue(t *testing.T, db *sql.DB, key string) string {
	t.Helper()
	var value string
	err := db.QueryRowContext(t.Context(), `
SELECT value
FROM workset_metadata
WHERE key = ?
`, key).Scan(&value)
	require.NoError(t, err)
	return value
}

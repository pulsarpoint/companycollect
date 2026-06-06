package companydata

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestBuildTranslationWorksetScopesCompaniesFromMaterializedExplorer(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555101",
		OrganizationName:      "MATERIALIZED TARGET AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	second := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555102",
		OrganizationName:      "MATERIALIZED OTHER AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	_, err := tx.Exec(t.Context(), "REFRESH MATERIALIZED VIEW brreg_source.mv_company_explorer")
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	result, err := New(tx).BuildTranslationWorkset(t.Context(), BuildTranslationWorksetCommand{
		Path:          path,
		PromptVersion: "v1",
		IDs:           []string{first.CompanyID.String()},
		Filters:       map[string]string{"translation_status": "missing"},
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.CompaniesExported)
	require.EqualValues(t, 2, result.FieldsExported)

	db := openWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 2, countRowsWhere(t, db, "translation_bindings", "company_id = '"+first.CompanyID.String()+"'"))
	require.EqualValues(t, 0, countRowsWhere(t, db, "translation_bindings", "company_id = '"+second.CompanyID.String()+"'"))
}

func TestSaveTranslationWorksetBatchReportsMissingWorksetFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "translation-workset.sqlite")

	_, err := SaveTranslationWorksetBatch(t.Context(), SaveTranslationWorksetBatchCommand{
		Path:    path,
		BatchID: 1,
		Results: []TranslationTermResult{{
			TermKey:        "term-1",
			TranslatedText: "Entity",
			Status:         "succeeded",
		}},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "translation workset file does not exist")
}

func TestBuildTranslationWorksetAllRecordsUsesLiveMissingTranslationsWhenExplorerIsStale(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555201",
		OrganizationName:      "STALE EXPLORER FIRST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	second := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555202",
		OrganizationName:      "STALE EXPLORER SECOND AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	_, err := tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET updated_at = now() + interval '1 minute'
WHERE id = $1
`, first.CompanyID)
	require.NoError(t, err)
	_, err = tx.Exec(t.Context(), "REFRESH MATERIALIZED VIEW brreg_source.mv_company_explorer")
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	result, err := New(tx).BuildTranslationWorkset(t.Context(), BuildTranslationWorksetCommand{
		Path:          path,
		PromptVersion: "v1",
		IDs:           []string{first.CompanyID.String(), second.CompanyID.String()},
		CompanyLimit:  1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.CompaniesExported)

	db := openWorksetDB(t, path)
	require.EqualValues(t, 2, countRowsWhere(t, db, "translation_bindings", "company_id = '"+first.CompanyID.String()+"'"))
	db.Close()

	_, err = tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET organization_form_label_en = 'Limited liability company',
    response_class_en = 'Entity',
    updated_at = now()
WHERE id = $1
`, first.CompanyID)
	require.NoError(t, err)

	nextPath := filepath.Join(t.TempDir(), "translation-workset-next.sqlite")
	nextResult, err := New(tx).BuildTranslationWorkset(t.Context(), BuildTranslationWorksetCommand{
		Path:          nextPath,
		PromptVersion: "v1",
		IDs:           []string{first.CompanyID.String(), second.CompanyID.String()},
		CompanyLimit:  1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, nextResult.CompaniesExported)
	require.EqualValues(t, 2, nextResult.FieldsExported)

	nextDB := openWorksetDB(t, nextPath)
	defer nextDB.Close()
	require.EqualValues(t, 0, countRowsWhere(t, nextDB, "translation_bindings", "company_id = '"+first.CompanyID.String()+"'"))
	require.EqualValues(t, 2, countRowsWhere(t, nextDB, "translation_bindings", "company_id = '"+second.CompanyID.String()+"'"))
}

func TestStoreLoadMissingTranslationFieldsUsesBRREGFiltersAndNormalizedKeys(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555301",
		OrganizationName:      "FILTER TARGET AS",
		OrganizationFormLabel: " Aksjeselskap ",
		ResponseClass:         "Enhet",
	})
	second := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555302",
		OrganizationName:      "FILTER OTHER AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	_, err := tx.Exec(t.Context(), "REFRESH MATERIALIZED VIEW brreg_source.mv_company_explorer")
	require.NoError(t, err)
	store := New(tx)
	require.NoError(t, store.RefreshTranslationStatus(t.Context()))

	fields, err := store.LoadMissingTranslationFields(t.Context(), sourcetranslation.LoadMissingFieldsCommand{
		CompanyIDs:   []string{" " + first.CompanyID.String() + " ", second.CompanyID.String()},
		Filters:      map[string]string{"query": "target", "state": "active", "registration_status": "active", "translation_status": "missing", "website_status": "without"},
		CompanyLimit: 1,
		FieldLimit:   1,
	})

	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, first.CompanyID.String(), fields[0].CompanyID)
	require.Equal(t, "brreg_source.companies", fields[0].SourceTable)
	require.Equal(t, first.CompanyID.String(), fields[0].SourceRowID)
	require.Equal(t, "organization_form_label", fields[0].SourceColumn)
	require.Equal(t, "organization_form_label_en", fields[0].TargetColumn)
	require.Equal(t, "Aksjeselskap", fields[0].SourceText)
	require.Equal(t, "aksjeselskap", fields[0].SourceTextNormalized)
	require.Equal(t, translationTermKey("Aksjeselskap"), fields[0].TermKey)
	require.EqualValues(t, 20, fields[0].Priority)
}

func TestStoreLoadCachedTranslationTermsReturnsSucceededBRREGTerms(t *testing.T) {
	tx := testdb.BeginTx(t)
	_, err := tx.Exec(t.Context(), `
INSERT INTO brreg_source.translation_terms (
  source, source_lang, target_lang, source_text_normalized, source_text,
  term_key, translated_text, status, prompt_version
) VALUES
  ('brreg', 'no', 'en', 'aksjeselskap', 'Aksjeselskap', $1, 'Limited liability company', 'succeeded', 'v1'),
  ('brreg', 'no', 'en', 'enhet', 'Enhet', $2, 'Entity', 'failed_retryable', 'v1'),
  ('brreg', 'no', 'en', 'aksjekapital', 'Aksjekapital', $3, 'Share capital', 'succeeded', 'v2'),
  ('brreg', 'no', 'en', 'norge', 'Norge', $4, '   ', 'succeeded', 'v1')
`, translationTermKey("Aksjeselskap"), translationTermKey("Enhet"), translationTermKey("Aksjekapital"), translationTermKey("Norge"))
	require.NoError(t, err)

	terms, err := New(tx).LoadCachedTranslationTerms(t.Context(), sourcetranslation.LoadCachedTermsCommand{
		PromptVersion: "v1",
		TermKeys: []string{
			translationTermKey("Aksjeselskap"),
			translationTermKey("Enhet"),
			translationTermKey("Aksjekapital"),
			translationTermKey("Norge"),
			translationTermKey("Ukjent"),
		},
	})

	require.NoError(t, err)
	require.Equal(t, map[string]sourcetranslation.CachedTerm{
		translationTermKey("Aksjeselskap"): {
			TermKey:        translationTermKey("Aksjeselskap"),
			TranslatedText: "Limited liability company",
		},
	}, terms)
}

func TestStoreTranslationQueuePreparesClaimsAndCompletesBRREGCompanies(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555350",
		OrganizationName:      "QUEUE FIRST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	second := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555351",
		OrganizationName:      "QUEUE SECOND AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	store := New(tx)

	prepared, err := store.PrepareTranslationQueue(t.Context(), PrepareTranslationQueueCommand{
		IDs:           []string{first.CompanyID.String(), second.CompanyID.String()},
		CompanyLimit:  10,
		Provider:      "deepseek",
		Model:         "deepseek-chat",
		PromptVersion: "v2",
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, prepared.CompaniesSeen)
	require.EqualValues(t, 2, prepared.CompaniesQueued)

	preparedAgain, err := store.PrepareTranslationQueue(t.Context(), PrepareTranslationQueueCommand{
		IDs:          []string{first.CompanyID.String(), second.CompanyID.String()},
		CompanyLimit: 10,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, preparedAgain.CompaniesSeen)
	require.EqualValues(t, 0, preparedAgain.CompaniesQueued)

	claimed, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:          "brreg-test-batch-1",
		MaxCandidateRows: 10,
		MaxRequestChars:  10000,
		MaxSourceRunning: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	require.ElementsMatch(t, []string{first.CompanyID.String(), second.CompanyID.String()}, claimed.CompanyIDs)
	require.Greater(t, claimed.EstimatedChars, int32(0))
	require.Equal(t, "deepseek", claimed.Provider)
	require.Equal(t, "deepseek-chat", claimed.Model)
	require.Equal(t, "v2", claimed.PromptVersion)
	require.Equal(t, "no", claimed.SourceLang)
	require.Equal(t, "en", claimed.TargetLang)

	blocked, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:          "brreg-test-batch-2",
		MaxCandidateRows: 10,
		MaxRequestChars:  10000,
		MaxSourceRunning: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "blocked", blocked.Status)

	completed, err := store.CompleteTranslationQueueBatch(t.Context(), "brreg-test-batch-1")
	require.NoError(t, err)
	require.EqualValues(t, 2, completed.RowsAffected)

	drained, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:          "brreg-test-batch-3",
		MaxCandidateRows: 10,
		MaxRequestChars:  10000,
	})
	require.NoError(t, err)
	require.Equal(t, "drained", drained.Status)
}

func TestTranslationQueueCapacityAllowsUpToConfiguredSourceRunningBatches(t *testing.T) {
	command := normalizeClaimTranslationQueueBatchCommand(ClaimTranslationQueueBatchCommand{
		BatchID:          "brreg-capacity-batch",
		MaxSourceRunning: 2,
	})

	require.True(t, canClaimTranslationQueueBatch(translationQueueRunningCounts{
		SourceRunning: 1,
	}, command))
	require.False(t, canClaimTranslationQueueBatch(translationQueueRunningCounts{
		SourceRunning: 2,
	}, command))
}

func TestStoreResetStaleTranslationQueueEntriesReleasesBRREGRunningBatch(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999555352",
		OrganizationName:      "QUEUE STALE AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	store := New(tx)
	_, err := store.PrepareTranslationQueue(t.Context(), PrepareTranslationQueueCommand{
		IDs: []string{seed.CompanyID.String()},
	})
	require.NoError(t, err)
	claimed, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:          "brreg-stale-batch",
		MaxCandidateRows: 10,
		MaxRequestChars:  10000,
	})
	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	_, err = tx.Exec(t.Context(), `
UPDATE brreg_source.translation_queue_entries
SET status_changed_at = now() - interval '2 hours'
WHERE batch_id = 'brreg-stale-batch'
`)
	require.NoError(t, err)

	reset, err := store.ResetStaleTranslationQueueEntries(t.Context(), 3600)
	require.NoError(t, err)
	require.EqualValues(t, 1, reset.RowsAffected)

	reclaimed, err := store.ClaimTranslationQueueBatch(t.Context(), ClaimTranslationQueueBatchCommand{
		BatchID:          "brreg-reclaimed-batch",
		MaxCandidateRows: 10,
		MaxRequestChars:  10000,
	})
	require.NoError(t, err)
	require.Equal(t, "claimed", reclaimed.Status)
	require.Equal(t, []string{seed.CompanyID.String()}, reclaimed.CompanyIDs)
}

func TestStoreApplyCompanyTranslationsRejectsUnsupportedBRREGTarget(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber: "999555303",
		OrganizationName:   "UNSUPPORTED TARGET AS",
		ResponseClass:      "Enhet",
	})

	_, err := New(tx).ApplyCompanyTranslations(t.Context(), sourcetranslation.ApplyCompanyTranslationsCommand{
		CompanyID: seed.CompanyID.String(),
		Bindings: []sourcetranslation.TranslationBinding{{
			ID:             42,
			CompanyID:      seed.CompanyID.String(),
			SourceTable:    "brreg_source.companies",
			SourceRowID:    seed.CompanyID.String(),
			SourceColumn:   "response_class",
			TargetColumn:   "unsupported_en",
			TranslatedText: "Entity",
		}},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported brreg company translation target column")
}

func TestStoreApplyCompanyTranslationsReturnsAlreadySatisfiedBRREGBindings(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber: "999555304",
		OrganizationName:   "SATISFIED TARGET AS",
		ResponseClass:      "Enhet",
	})
	_, err := tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET response_class_en = 'Existing value'
WHERE id = $1
`, seed.CompanyID)
	require.NoError(t, err)

	result, err := New(tx).ApplyCompanyTranslations(t.Context(), sourcetranslation.ApplyCompanyTranslationsCommand{
		CompanyID: seed.CompanyID.String(),
		Bindings: []sourcetranslation.TranslationBinding{{
			ID:             77,
			CompanyID:      seed.CompanyID.String(),
			SourceTable:    "brreg_source.companies",
			SourceRowID:    seed.CompanyID.String(),
			SourceColumn:   "response_class",
			TargetColumn:   "response_class_en",
			TranslatedText: "Entity",
		}},
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.BindingsApplied)
	require.Equal(t, []int64{77}, result.AppliedBindingIDs)
	var responseClassEN string
	err = tx.QueryRow(t.Context(), `
SELECT response_class_en
FROM brreg_source.companies
WHERE id = $1
`, seed.CompanyID).Scan(&responseClassEN)
	require.NoError(t, err)
	require.Equal(t, "Existing value", responseClassEN)
}

func TestStoreBuildTranslationWorksetExportsMissingFieldsFromRelatedTables(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111242",
		OrganizationName:      "WORKSET BUILD AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		AddressCountry:        "Norge",
		IndustryLabel:         "Andre egeninvesteringsselskaper",
		WebsiteTitle:          "Kontakt oss",
		WebsiteDescription:    "Offisiell hjemmeside",
		ContactLabel:          "Sentralbord",
		CapitalType:           "Aksjekapital",
		RoleLabel:             "Styrets leder",
		RoleGroup:             "Styret",
	})
	_, err := tx.Exec(t.Context(), "REFRESH MATERIALIZED VIEW brreg_source.mv_company_explorer")
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")

	result, err := New(tx).BuildTranslationWorkset(t.Context(), BuildTranslationWorksetCommand{
		Path:          path,
		PromptVersion: "v1",
	})

	require.NoError(t, err)
	require.Equal(t, path, result.Path)
	require.GreaterOrEqual(t, result.FieldsExported, int32(9))
	require.EqualValues(t, 1, result.CompaniesExported)

	db := openWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, result.FieldsExported, countRows(t, db, "translation_bindings"))
	require.EqualValues(t, 2, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.companies' AND source_row_id = '"+seed.CompanyID.String()+"'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.addresses' AND source_row_id = '"+seed.AddressID.String()+"'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.industries' AND source_row_id = '"+seed.IndustryID.String()+"'"))
	require.EqualValues(t, 2, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.websites' AND source_row_id = '"+seed.WebsiteID.String()+"'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.contacts' AND source_row_id = '"+seed.ContactID.String()+"'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.capital' AND source_row_id = '"+seed.CapitalID.String()+"'"))
	require.EqualValues(t, 2, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.roles' AND source_row_id = '"+seed.RoleID.String()+"'"))
}

func TestStoreApplyTranslationWorksetUpdatesPostgresFields(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111243",
		OrganizationName:      "WORKSET APPLY AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		AddressCountry:        "Norge",
		IndustryLabel:         "Andre egeninvesteringsselskaper",
		WebsiteTitle:          "Kontakt oss",
		WebsiteDescription:    "Offisiell hjemmeside",
		ContactLabel:          "Sentralbord",
		CapitalType:           "Aksjekapital",
		RoleLabel:             "Styrets leder",
		RoleGroup:             "Styret",
	})
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	built, err := New(tx).BuildTranslationWorkset(t.Context(), BuildTranslationWorksetCommand{
		Path:          path,
		PromptVersion: "v1",
		IDs:           []string{seed.CompanyID.String()},
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, built.FieldsExported, int32(9))

	claimed, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 10000,
		MaxTerms:        50,
		MaxAttempts:     3,
	})
	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	require.NotEmpty(t, claimed.Terms)
	translated := map[string]string{
		"Aksjeselskap":                    "Limited liability company",
		"Enhet":                           "Entity",
		"Norge":                           "Norway",
		"Andre egeninvesteringsselskaper": "Other own-investment companies",
		"Kontakt oss":                     "Contact us",
		"Offisiell hjemmeside":            "Official website",
		"Sentralbord":                     "Switchboard",
		"Aksjekapital":                    "Share capital",
		"Styrets leder":                   "Chair of the board",
		"Styret":                          "Board",
	}
	results := make([]TranslationTermResult, 0, len(claimed.Terms))
	for _, term := range claimed.Terms {
		translatedText := translated[term.SourceText]
		require.NotEmpty(t, translatedText, "missing test translation for %q", term.SourceText)
		results = append(results, TranslationTermResult{
			TermKey:              term.TermKey,
			SourceText:           term.SourceText,
			SourceTextNormalized: term.SourceTextNormalized,
			TranslatedText:       translatedText,
			Status:               "succeeded",
		})
	}
	saved, err := SaveTranslationWorksetBatch(t.Context(), SaveTranslationWorksetBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Results:       results,
	})
	require.NoError(t, err)
	require.EqualValues(t, len(results), saved.TermsSucceeded)
	drained, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 10000,
		MaxTerms:        50,
		MaxAttempts:     3,
	})
	require.NoError(t, err)
	require.Equal(t, "drained", drained.Status)

	result, err := New(tx).ApplyTranslationWorkset(t.Context(), ApplyTranslationWorksetCommand{
		Path:          path,
		PromptVersion: "v1",
	})

	require.NoError(t, err)
	require.EqualValues(t, built.FieldsExported, result.BindingsApplied)
	require.EqualValues(t, len(results), result.TermsSaved)

	var responseClassEN string
	var countryEN string
	var industryLabelEN string
	var websiteTitleEN string
	var websiteDescriptionEN string
	var contactLabelEN string
	var capitalTypeEN string
	var roleLabelEN string
	var roleGroupEN string
	err = tx.QueryRow(t.Context(), `
SELECT
  company.response_class_en,
  address.country_en,
  industry.source_label_en,
  website.title_en,
  website.description_en,
  contact.label_en,
  capital.capital_type_en,
  role.role_label_en,
  role.role_group_en
FROM brreg_source.companies company
JOIN brreg_source.addresses address ON address.company_id = company.id
JOIN brreg_source.industries industry ON industry.company_id = company.id
JOIN brreg_source.websites website ON website.company_id = company.id
JOIN brreg_source.contacts contact ON contact.company_id = company.id
JOIN brreg_source.capital capital ON capital.company_id = company.id
JOIN brreg_source.roles role ON role.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(
		&responseClassEN,
		&countryEN,
		&industryLabelEN,
		&websiteTitleEN,
		&websiteDescriptionEN,
		&contactLabelEN,
		&capitalTypeEN,
		&roleLabelEN,
		&roleGroupEN,
	)
	require.NoError(t, err)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "Norway", countryEN)
	require.Equal(t, "Other own-investment companies", industryLabelEN)
	require.Equal(t, "Contact us", websiteTitleEN)
	require.Equal(t, "Official website", websiteDescriptionEN)
	require.Equal(t, "Switchboard", contactLabelEN)
	require.Equal(t, "Share capital", capitalTypeEN)
	require.Equal(t, "Chair of the board", roleLabelEN)
	require.Equal(t, "Board", roleGroupEN)

	db := openWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 9, countRowsWhere(t, db, "translation_bindings", "status = 'applied'"))
}

func openWorksetDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	return countRowsWhere(t, db, table, "1 = 1")
}

func countRowsWhere(t *testing.T, db *sql.DB, table string, where string) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table+" WHERE "+where).Scan(&count)
	require.NoError(t, err)
	return count
}

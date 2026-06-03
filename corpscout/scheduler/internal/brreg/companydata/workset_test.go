package companydata

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestWriteTranslationWorksetCreatesSQLiteFileWithBindingsAndTerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	rows := []translationWorksetRow{
		{
			CompanyID:            "company-1",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-1",
			SourceColumn:         "organization_form_label",
			TargetColumn:         "organization_form_label_en",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              translationTermKey("Aksjeselskap"),
		},
		{
			CompanyID:            "company-1",
			SourceTable:          "brreg_source.industries",
			SourceRowID:          "industry-1",
			SourceColumn:         "source_label",
			TargetColumn:         "source_label_en",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              translationTermKey("Aksjeselskap"),
		},
		{
			CompanyID:            "company-2",
			SourceTable:          "brreg_source.capital",
			SourceRowID:          "capital-1",
			SourceColumn:         "capital_type",
			TargetColumn:         "capital_type_en",
			SourceText:           "Aksjekapital",
			SourceTextNormalized: "aksjekapital",
			TermKey:              translationTermKey("Aksjekapital"),
			CachedTranslatedText: "Share capital",
		},
	}

	result, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{
		Source:        "brreg",
		SourceLang:    "no",
		TargetLang:    "en",
		PromptVersion: "v1",
	}, rows)

	require.NoError(t, err)
	require.Equal(t, path, result.Path)
	require.EqualValues(t, 3, result.FieldsExported)
	require.EqualValues(t, 2, result.TermsExported)
	require.EqualValues(t, 2, result.CompaniesExported)
	require.EqualValues(t, 1, result.CachedFields)

	db := openWorksetDB(t, path)
	defer db.Close()

	require.EqualValues(t, 3, countRows(t, db, "translation_bindings"))
	require.EqualValues(t, 2, countRows(t, db, "translation_terms"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "status = 'cached'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_terms", "status = 'succeeded'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_terms", "status = 'pending'"))

	var sourceText string
	var translatedText sql.NullString
	err = db.QueryRowContext(t.Context(), `
SELECT source_text, translated_text
FROM translation_terms
WHERE term_key = ?
`, translationTermKey("Aksjekapital")).Scan(&sourceText, &translatedText)
	require.NoError(t, err)
	require.Equal(t, "Aksjekapital", sourceText)
	require.True(t, translatedText.Valid)
	require.Equal(t, "Share capital", translatedText.String)
}

func TestWriteTranslationWorksetReplacesExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	first, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{}, []translationWorksetRow{{
		CompanyID:            "company-1",
		SourceTable:          "brreg_source.companies",
		SourceRowID:          "company-1",
		SourceColumn:         "response_class",
		TargetColumn:         "response_class_en",
		SourceText:           "Enhet",
		SourceTextNormalized: "enhet",
		TermKey:              translationTermKey("Enhet"),
	}})
	require.NoError(t, err)
	require.EqualValues(t, 1, first.FieldsExported)

	second, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{}, nil)

	require.NoError(t, err)
	require.EqualValues(t, 0, second.FieldsExported)
	db := openWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 0, countRows(t, db, "translation_bindings"))
	require.EqualValues(t, 0, countRows(t, db, "translation_terms"))
}

func TestClaimTranslationWorksetBatchPacksPendingTermsByCharacterBudget(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	_, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{}, []translationWorksetRow{
		{
			CompanyID:            "company-1",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-1",
			SourceColumn:         "response_class",
			TargetColumn:         "response_class_en",
			SourceText:           "Kort",
			SourceTextNormalized: "kort",
			TermKey:              translationTermKey("Kort"),
		},
		{
			CompanyID:            "company-2",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-2",
			SourceColumn:         "activity_description",
			TargetColumn:         "activity_description_en",
			SourceText:           "Dette er en lengre tekst",
			SourceTextNormalized: "dette er en lengre tekst",
			TermKey:              translationTermKey("Dette er en lengre tekst"),
		},
		{
			CompanyID:            "company-3",
			SourceTable:          "brreg_source.capital",
			SourceRowID:          "capital-3",
			SourceColumn:         "capital_type",
			TargetColumn:         "capital_type_en",
			SourceText:           "Aksjekapital",
			SourceTextNormalized: "aksjekapital",
			TermKey:              translationTermKey("Aksjekapital"),
		},
	})
	require.NoError(t, err)

	claimed, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 20,
		MaxTerms:        10,
	})

	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	require.NotZero(t, claimed.BatchID)
	require.Len(t, claimed.Terms, 1)
	require.Equal(t, "Aksjekapital", claimed.Terms[0].SourceText)
	require.EqualValues(t, 12, claimed.EstimatedChars)

	db := openWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_terms", "status = 'running'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_batches", "status = 'running'"))
}

func TestClaimTranslationWorksetBatchKeepsOversizedFirstTerm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	_, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{}, []translationWorksetRow{{
		CompanyID:            "company-1",
		SourceTable:          "brreg_source.companies",
		SourceRowID:          "company-1",
		SourceColumn:         "activity_description",
		TargetColumn:         "activity_description_en",
		SourceText:           "Dette er mye lengre enn grensen",
		SourceTextNormalized: "dette er mye lengre enn grensen",
		TermKey:              translationTermKey("Dette er mye lengre enn grensen"),
	}})
	require.NoError(t, err)

	claimed, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 5,
		MaxTerms:        10,
	})

	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	require.Len(t, claimed.Terms, 1)
	require.Greater(t, claimed.EstimatedChars, int32(5))
}

func TestClaimTranslationWorksetBatchRetriesFailedTermsUntilMaxAttempts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	_, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{}, []translationWorksetRow{{
		CompanyID:            "company-1",
		SourceTable:          "brreg_source.companies",
		SourceRowID:          "company-1",
		SourceColumn:         "response_class",
		TargetColumn:         "response_class_en",
		SourceText:           "Enhet",
		SourceTextNormalized: "enhet",
		TermKey:              translationTermKey("Enhet"),
	}})
	require.NoError(t, err)

	first, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
		MaxAttempts:     2,
	})
	require.NoError(t, err)
	_, err = SaveTranslationWorksetBatch(t.Context(), SaveTranslationWorksetBatchCommand{
		Path:          path,
		BatchID:       first.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Results: []TranslationTermResult{{
			TermKey: translationTermKey("Enhet"),
			Status:  "failed_retryable",
			Error:   "temporary",
		}},
	})
	require.NoError(t, err)

	second, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
		MaxAttempts:     2,
	})
	require.NoError(t, err)
	require.Equal(t, "claimed", second.Status)
	require.Len(t, second.Terms, 1)
	require.Equal(t, "Enhet", second.Terms[0].SourceText)
	_, err = SaveTranslationWorksetBatch(t.Context(), SaveTranslationWorksetBatchCommand{
		Path:          path,
		BatchID:       second.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Results: []TranslationTermResult{{
			TermKey: translationTermKey("Enhet"),
			Status:  "failed_retryable",
			Error:   "still temporary",
		}},
	})
	require.NoError(t, err)

	drained, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
		MaxAttempts:     2,
	})
	require.NoError(t, err)
	require.Equal(t, "drained", drained.Status)
}

func TestSaveTranslationWorksetBatchStoresResultsAndUpdatesBindings(t *testing.T) {
	path := filepath.Join(t.TempDir(), "translation-workset.sqlite")
	_, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{}, []translationWorksetRow{
		{
			CompanyID:            "company-1",
			SourceTable:          "brreg_source.companies",
			SourceRowID:          "company-1",
			SourceColumn:         "response_class",
			TargetColumn:         "response_class_en",
			SourceText:           "Enhet",
			SourceTextNormalized: "enhet",
			TermKey:              translationTermKey("Enhet"),
		},
		{
			CompanyID:            "company-2",
			SourceTable:          "brreg_source.capital",
			SourceRowID:          "capital-2",
			SourceColumn:         "capital_type",
			TargetColumn:         "capital_type_en",
			SourceText:           "Aksjekapital",
			SourceTextNormalized: "aksjekapital",
			TermKey:              translationTermKey("Aksjekapital"),
		},
	})
	require.NoError(t, err)
	claimed, err := ClaimTranslationWorksetBatch(t.Context(), ClaimTranslationWorksetBatchCommand{
		Path:            path,
		MaxRequestChars: 100,
		MaxTerms:        10,
	})
	require.NoError(t, err)
	require.Len(t, claimed.Terms, 2)

	result, err := SaveTranslationWorksetBatch(t.Context(), SaveTranslationWorksetBatchCommand{
		Path:          path,
		BatchID:       claimed.BatchID,
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		Results: []TranslationTermResult{{
			TermKey:              translationTermKey("Enhet"),
			SourceText:           "Enhet",
			SourceTextNormalized: "enhet",
			TranslatedText:       "Entity",
			Status:               "succeeded",
		}, {
			TermKey:              translationTermKey("Aksjekapital"),
			SourceText:           "Aksjekapital",
			SourceTextNormalized: "aksjekapital",
			Status:               "failed_retryable",
			Error:                "temporary failure",
			ErrorCode:            "temporary",
		}},
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.TermsSucceeded)
	require.EqualValues(t, 1, result.TermsFailed)
	db := openWorksetDB(t, path)
	defer db.Close()
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_terms", "status = 'succeeded'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_terms", "status = 'failed_retryable'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "status = 'translated'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "status = 'failed'"))
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_batches", "status = 'succeeded'"))
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
	require.EqualValues(t, 1, countRowsWhere(t, db, "translation_bindings", "source_table = 'brreg_source.companies' AND source_row_id = '"+seed.CompanyID.String()+"'"))
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
	_, err := writeTranslationWorkset(t.Context(), path, translationWorksetMetadata{PromptVersion: "v1"}, []translationWorksetRow{
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.companies", seed.CompanyID.String(), "response_class", "response_class_en", "Enhet", "Entity"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.addresses", seed.AddressID.String(), "country", "country_en", "Norge", "Norway"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.industries", seed.IndustryID.String(), "source_label", "source_label_en", "Andre egeninvesteringsselskaper", "Other own-investment companies"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.websites", seed.WebsiteID.String(), "title", "title_en", "Kontakt oss", "Contact us"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.websites", seed.WebsiteID.String(), "description", "description_en", "Offisiell hjemmeside", "Official website"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.contacts", seed.ContactID.String(), "label", "label_en", "Sentralbord", "Switchboard"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.capital", seed.CapitalID.String(), "capital_type", "capital_type_en", "Aksjekapital", "Share capital"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.roles", seed.RoleID.String(), "role_label", "role_label_en", "Styrets leder", "Chair of the board"),
		cachedWorksetRow(seed.CompanyID.String(), "brreg_source.roles", seed.RoleID.String(), "role_group", "role_group_en", "Styret", "Board"),
	})
	require.NoError(t, err)

	result, err := New(tx).ApplyTranslationWorkset(t.Context(), ApplyTranslationWorksetCommand{
		Path:          path,
		PromptVersion: "v1",
	})

	require.NoError(t, err)
	require.EqualValues(t, 9, result.BindingsApplied)
	require.EqualValues(t, 9, result.TermsSaved)

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

func cachedWorksetRow(
	companyID string,
	sourceTable string,
	sourceRowID string,
	sourceColumn string,
	targetColumn string,
	sourceText string,
	translatedText string,
) translationWorksetRow {
	return translationWorksetRow{
		CompanyID:            companyID,
		SourceTable:          sourceTable,
		SourceRowID:          sourceRowID,
		SourceColumn:         sourceColumn,
		TargetColumn:         targetColumn,
		SourceText:           sourceText,
		SourceTextNormalized: normalizeTranslationText(sourceText),
		TermKey:              translationTermKey(sourceText),
		CachedTranslatedText: translatedText,
	}
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

package companydata

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestCompanyDataTranslationTermsReturnsUniqueMissingNorwegianTerms(t *testing.T) {
	companyID := uuid.New()
	capitalID := uuid.New()
	data := &CompanyData{
		Company: Company{
			ID:                      companyID,
			OrganizationNumber:      "999111222",
			OrganizationName:        "TERM TEST AS",
			OrganizationFormLabel:   "Aksjeselskap",
			OrganizationFormLabelEN: "Limited liability company",
			ResponseClass:           "Enhet",
			ActivityDescription:     "Utvikling av programvare",
			StatutoryPurpose:        "Utvikling av programvare",
		},
		Capital: []Capital{{
			ID:          capitalID,
			CompanyID:   companyID,
			CapitalType: "Aksjekapital",
		}},
	}

	terms := data.TranslationTerms()

	require.Equal(t, []TranslationTerm{
		{Key: translationTermKey("Enhet"), SourceText: "Enhet", NormalizedText: "enhet"},
		{Key: translationTermKey("Utvikling av programvare"), SourceText: "Utvikling av programvare", NormalizedText: "utvikling av programvare"},
		{Key: translationTermKey("Aksjekapital"), SourceText: "Aksjekapital", NormalizedText: "aksjekapital"},
	}, terms)
}

func TestCompanyDataApplyTranslationsUpdatesEnglishFields(t *testing.T) {
	companyID := uuid.New()
	capitalID := uuid.New()
	data := &CompanyData{
		Company: Company{
			ID:                    companyID,
			OrganizationFormLabel: "Aksjeselskap",
			ResponseClass:         "Enhet",
			ActivityDescription:   "Utvikling av programvare",
			StatutoryPurpose:      "Investering i aksjer",
		},
		Capital: []Capital{{
			ID:          capitalID,
			CompanyID:   companyID,
			CapitalType: "Aksjekapital",
		}},
	}

	result := data.ApplyTranslations([]TermTranslation{
		{SourceText: "Aksjeselskap", TranslatedText: "Limited liability company"},
		{SourceText: "Enhet", TranslatedText: "Entity"},
		{SourceText: "Utvikling av programvare", TranslatedText: "Software development"},
		{SourceText: "Investering i aksjer", TranslatedText: "Investment in shares"},
		{SourceText: "Aksjekapital", TranslatedText: "Share capital"},
	})

	require.EqualValues(t, 5, result.FieldsApplied)
	require.EqualValues(t, 0, result.TermsWithoutMatch)
	require.True(t, data.TranslationComplete())
	require.Equal(t, "Limited liability company", data.Company.OrganizationFormLabelEN)
	require.Equal(t, "Entity", data.Company.ResponseClassEN)
	require.Equal(t, "Software development", data.Company.ActivityDescriptionEN)
	require.Equal(t, "Investment in shares", data.Company.StatutoryPurposeEN)
	require.Equal(t, "Share capital", data.Capital[0].CapitalTypeEN)
}

func TestCompanyDataMissingFieldCountCountsFieldsWhileTermsDeduplicate(t *testing.T) {
	data := &CompanyData{
		Company: Company{
			ActivityDescription: "Utvikling av programvare",
			StatutoryPurpose:    "Utvikling av programvare",
			ResponseClass:       "   ",
		},
		Capital: []Capital{{
			CapitalType: "Utvikling av programvare",
		}},
	}

	terms := data.TranslationTerms()

	require.Len(t, terms, 1)
	require.Equal(t, "Utvikling av programvare", terms[0].SourceText)
	require.EqualValues(t, 3, data.MissingTranslationFieldCount())
}

func TestCompanyDataApplyTranslationsIgnoresEmptyAndUnknownTerms(t *testing.T) {
	data := &CompanyData{
		Company: Company{
			OrganizationFormLabel: "Aksjeselskap",
			ResponseClass:         "Enhet",
		},
	}

	result := data.ApplyTranslations([]TermTranslation{
		{SourceText: "Aksjeselskap", TranslatedText: "Limited liability company"},
		{SourceText: "Enhet", TranslatedText: "  "},
		{SourceText: "Ukjent", TranslatedText: "Unknown"},
	})

	require.EqualValues(t, 1, result.FieldsApplied)
	require.EqualValues(t, 2, result.TermsWithoutMatch)
	require.Equal(t, "Limited liability company", data.Company.OrganizationFormLabelEN)
	require.Empty(t, data.Company.ResponseClassEN)
}

func TestStoreLoadReturnsCompanyDataGraph(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111222",
		OrganizationName:      "TERM TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		ActivityDescription:   "Utvikling av programvare",
		StatutoryPurpose:      "Investering i aksjer",
		CapitalType:           "Aksjekapital",
	})

	data, err := New(tx).Load(t.Context(), seed.CompanyID)

	require.NoError(t, err)
	require.Equal(t, seed.CompanyID, data.Company.ID)
	require.Equal(t, "999111222", data.Company.OrganizationNumber)
	require.Equal(t, "TERM TEST AS", data.Company.OrganizationName)
	require.Equal(t, "Aksjeselskap", data.Company.OrganizationFormLabel)
	require.Len(t, data.Capital, 1)
	require.Equal(t, seed.CapitalID, data.Capital[0].ID)
	require.Equal(t, "Aksjekapital", data.Capital[0].CapitalType)
}

func TestStoreLoadHandlesCompanyWithoutCapital(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111226",
		OrganizationName:      "NO CAPITAL AS",
		OrganizationFormLabel: "Aksjeselskap",
	})

	data, err := New(tx).Load(t.Context(), seed.CompanyID)

	require.NoError(t, err)
	require.Equal(t, seed.CompanyID, data.Company.ID)
	require.Empty(t, data.Capital)
}

func TestStoreLoadDoesNotReturnSupersededCompany(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111227",
		OrganizationName:      "SUPERSEDED AS",
		OrganizationFormLabel: "Aksjeselskap",
	})
	_, err := tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET row_status = 'superseded'
WHERE id = $1
`, seed.CompanyID)
	require.NoError(t, err)

	data, err := New(tx).Load(t.Context(), seed.CompanyID)

	require.Error(t, err)
	require.Nil(t, data)
	require.Contains(t, err.Error(), "not found")
}

func TestStoreSavePersistsCompanyDataTranslations(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111223",
		OrganizationName:      "SAVE TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		ActivityDescription:   "Utvikling av programvare",
		StatutoryPurpose:      "Investering i aksjer",
		CapitalType:           "Aksjekapital",
	})
	store := New(tx)
	data, err := store.Load(t.Context(), seed.CompanyID)
	require.NoError(t, err)
	data.ApplyTranslations([]TermTranslation{
		{SourceText: "Aksjeselskap", TranslatedText: "Limited liability company"},
		{SourceText: "Enhet", TranslatedText: "Entity"},
		{SourceText: "Utvikling av programvare", TranslatedText: "Software development"},
		{SourceText: "Investering i aksjer", TranslatedText: "Investment in shares"},
		{SourceText: "Aksjekapital", TranslatedText: "Share capital"},
	})

	err = store.Save(t.Context(), data)

	require.NoError(t, err)
	var organizationFormLabelEN string
	var responseClassEN string
	var activityDescriptionEN string
	var statutoryPurposeEN string
	var capitalTypeEN string
	err = tx.QueryRow(t.Context(), `
SELECT
  company.organization_form_label_en,
  company.response_class_en,
  company.activity_description_en,
  company.statutory_purpose_en,
  capital.capital_type_en
FROM brreg_source.companies company
JOIN brreg_source.capital capital ON capital.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(
		&organizationFormLabelEN,
		&responseClassEN,
		&activityDescriptionEN,
		&statutoryPurposeEN,
		&capitalTypeEN,
	)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "Software development", activityDescriptionEN)
	require.Equal(t, "Investment in shares", statutoryPurposeEN)
	require.Equal(t, "Share capital", capitalTypeEN)
}

func TestStoreSaveDoesNotClearExistingTranslationsWithEmptyValues(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111234",
		OrganizationName:      "NONDESTRUCTIVE SAVE AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		CapitalType:           "Aksjekapital",
	})
	_, err := tx.Exec(t.Context(), `
UPDATE brreg_source.companies
SET organization_form_label_en = 'Limited liability company',
    response_class_en = 'Entity'
WHERE id = $1
`, seed.CompanyID)
	require.NoError(t, err)
	_, err = tx.Exec(t.Context(), `
UPDATE brreg_source.capital
SET capital_type_en = 'Share capital'
WHERE id = $1
`, seed.CapitalID)
	require.NoError(t, err)

	err = New(tx).Save(t.Context(), &CompanyData{
		Company: Company{ID: seed.CompanyID},
		Capital: []Capital{{ID: seed.CapitalID}},
	})

	require.NoError(t, err)
	var organizationFormLabelEN string
	var responseClassEN string
	var capitalTypeEN string
	err = tx.QueryRow(t.Context(), `
SELECT company.organization_form_label_en, company.response_class_en, capital.capital_type_en
FROM brreg_source.companies company
JOIN brreg_source.capital capital ON capital.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(&organizationFormLabelEN, &responseClassEN, &capitalTypeEN)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "Share capital", capitalTypeEN)
}

func TestStoreApplyCachedTranslationsUsesTranslationTerms(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111225",
		OrganizationName:      "CACHE TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
		ActivityDescription:   "Utvikling av programvare",
		StatutoryPurpose:      "Investering i aksjer",
		CapitalType:           "Aksjekapital",
	})
	seedCachedTranslationTerm(t, tx, "Aksjeselskap", "Limited liability company")
	seedCachedTranslationTerm(t, tx, "Enhet", "Entity")
	seedCachedTranslationTerm(t, tx, "Utvikling av programvare", "Software development")
	seedCachedTranslationTerm(t, tx, "Investering i aksjer", "Investment in shares")
	seedCachedTranslationTerm(t, tx, "Aksjekapital", "Share capital")

	result, err := New(tx).ApplyCachedTranslations(t.Context(), ApplyCachedTranslationsCommand{
		CompanyID:     seed.CompanyID,
		PromptVersion: "v1",
	})

	require.NoError(t, err)
	require.EqualValues(t, 5, result.FieldsSeen)
	require.EqualValues(t, 5, result.FieldsApplied)
	require.EqualValues(t, 0, result.RemainingFields)
	loaded, err := New(tx).Load(t.Context(), seed.CompanyID)
	require.NoError(t, err)
	require.True(t, loaded.TranslationComplete())
	require.Equal(t, "Limited liability company", loaded.Company.OrganizationFormLabelEN)
	require.Equal(t, "Share capital", loaded.Capital[0].CapitalTypeEN)
}

func TestStoreApplyCachedTranslationsRespectsPromptVersionAndStatus(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111228",
		OrganizationName:      "CACHE FILTER TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	seedTranslationTerm(t, tx, translationTermSeed{
		SourceText:     "Aksjeselskap",
		TranslatedText: "Limited liability company",
		PromptVersion:  "v2",
		Status:         "succeeded",
	})
	seedTranslationTerm(t, tx, translationTermSeed{
		SourceText:     "Enhet",
		TranslatedText: "Entity",
		PromptVersion:  "v1",
		Status:         "failed_retryable",
	})

	result, err := New(tx).ApplyCachedTranslations(t.Context(), ApplyCachedTranslationsCommand{
		CompanyID:     seed.CompanyID,
		PromptVersion: "v1",
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.FieldsSeen)
	require.EqualValues(t, 0, result.FieldsApplied)
	require.EqualValues(t, 2, result.RemainingFields)
	loaded, err := New(tx).Load(t.Context(), seed.CompanyID)
	require.NoError(t, err)
	require.Empty(t, loaded.Company.OrganizationFormLabelEN)
	require.Empty(t, loaded.Company.ResponseClassEN)
}

func TestStoreClaimForTranslationReturnsCompanyData(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111224",
		OrganizationName:      "CLAIM TEST AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})

	result, err := New(tx).ClaimForTranslation(t.Context(), ClaimForTranslationCommand{
		Limit:            10,
		MaxParallelTasks: 10,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         "test-worker",
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.StatusRowsInserted)
	require.Len(t, result.Companies, 1)
	require.Equal(t, seed.CompanyID, result.Companies[0].Company.ID)
	require.Equal(t, "CLAIM TEST AS", result.Companies[0].Company.OrganizationName)
	require.EqualValues(t, 1, result.Companies[0].AttemptCount)
}

func TestStoreClaimForTranslationRespectsLimitAndActiveCapacity(t *testing.T) {
	tx := testdb.BeginTx(t)
	for idx, organizationNumber := range []string{"999111229", "999111230", "999111231"} {
		seedCompanyData(t, tx, companyDataSeed{
			OrganizationNumber:    organizationNumber,
			OrganizationName:      "CLAIM CAPACITY TEST " + string(rune('A'+idx)),
			OrganizationFormLabel: "Aksjeselskap",
		})
	}
	store := New(tx)

	first, err := store.ClaimForTranslation(t.Context(), ClaimForTranslationCommand{
		Limit:            2,
		MaxParallelTasks: 2,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         "test-worker",
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, first.StatusRowsInserted)
	require.Len(t, first.Companies, 2)

	second, err := store.ClaimForTranslation(t.Context(), ClaimForTranslationCommand{
		Limit:            2,
		MaxParallelTasks: 2,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         "test-worker",
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, second.StatusRowsInserted)
	require.Empty(t, second.Companies)
}

func TestStoreClaimForTranslationReclaimsStaleRunningCompany(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111232",
		OrganizationName:      "STALE CLAIM AS",
		OrganizationFormLabel: "Aksjeselskap",
	})
	_, err := tx.Exec(t.Context(), `
INSERT INTO brreg_source.company_process_status (
  company_id,
  translation_status,
  translation_attempt_count,
  translation_lease_by,
  translation_lease_until
) VALUES ($1, 'running', 1, 'old-worker', now() - interval '1 minute')
`, seed.CompanyID)
	require.NoError(t, err)

	result, err := New(tx).ClaimForTranslation(t.Context(), ClaimForTranslationCommand{
		Limit:            1,
		MaxParallelTasks: 1,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         "new-worker",
	})

	require.NoError(t, err)
	require.Len(t, result.Companies, 1)
	require.Equal(t, seed.CompanyID, result.Companies[0].Company.ID)
	require.EqualValues(t, 2, result.Companies[0].AttemptCount)
}

func TestStoreClaimForTranslationDoesNotClaimTerminalFailures(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedCompanyData(t, tx, companyDataSeed{
		OrganizationNumber:    "999111233",
		OrganizationName:      "TERMINAL CLAIM AS",
		OrganizationFormLabel: "Aksjeselskap",
	})
	_, err := tx.Exec(t.Context(), `
INSERT INTO brreg_source.company_process_status (
  company_id,
  translation_status,
  translation_attempt_count,
  translation_error
) VALUES ($1, 'failed_terminal', 3, 'terminal')
`, seed.CompanyID)
	require.NoError(t, err)

	result, err := New(tx).ClaimForTranslation(t.Context(), ClaimForTranslationCommand{
		Limit:            1,
		MaxParallelTasks: 1,
		LeaseSeconds:     900,
		MaxAttempts:      3,
		WorkerID:         "test-worker",
	})

	require.NoError(t, err)
	require.Empty(t, result.Companies)
}

type seededCompanyData struct {
	RawRecordID uuid.UUID
	CompanyID   uuid.UUID
	CapitalID   uuid.UUID
}

type companyDataSeed struct {
	OrganizationNumber    string
	OrganizationName      string
	OrganizationFormLabel string
	ResponseClass         string
	ActivityDescription   string
	StatutoryPurpose      string
	CapitalType           string
}

func seedCompanyData(t *testing.T, tx pgx.Tx, seed companyDataSeed) seededCompanyData {
	t.Helper()
	ctx := context.Background()
	rawRecordID := uuid.New()
	companyID := uuid.New()
	capitalID := uuid.New()

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id, source_native_id, organization_number, organization_name,
  registration_status, country_iso2, raw_payload, payload_hash
) VALUES ($1, $2, $2, $3, 'active', 'NO', '{}'::jsonb, $4)
`, rawRecordID, seed.OrganizationNumber, seed.OrganizationName, translationTermKey(seed.OrganizationNumber))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id, raw_record_id, organization_number, source_native_id, organization_name,
  organization_name_normalized, country_iso2, organization_form_label, response_class,
  activity_description, statutory_purpose,
  lifecycle_status, registration_status, row_status, payload_hash
) VALUES ($1, $2, $3, $3, $4, lower($4), 'NO', $5, $6, $7, $8, 'active', 'active', 'active', $9)
`, companyID, rawRecordID, seed.OrganizationNumber, seed.OrganizationName,
		nullText(seed.OrganizationFormLabel),
		nullText(seed.ResponseClass),
		nullText(seed.ActivityDescription),
		nullText(seed.StatutoryPurpose),
		translationTermKey(seed.OrganizationNumber),
	)
	require.NoError(t, err)

	if seed.CapitalType != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id, company_id, raw_record_id, capital_type, original_amount, original_currency, raw_capital_payload
) VALUES ($1, $2, $3, $4, 30000, 'NOK', '{}'::jsonb)
`, capitalID, companyID, rawRecordID, seed.CapitalType)
		require.NoError(t, err)
	}

	return seededCompanyData{RawRecordID: rawRecordID, CompanyID: companyID, CapitalID: capitalID}
}

func nullText(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func seedCachedTranslationTerm(t *testing.T, tx pgx.Tx, sourceText string, translatedText string) {
	t.Helper()
	seedTranslationTerm(t, tx, translationTermSeed{
		SourceText:     sourceText,
		TranslatedText: translatedText,
		PromptVersion:  "v1",
		Status:         "succeeded",
	})
}

type translationTermSeed struct {
	SourceText     string
	TranslatedText string
	PromptVersion  string
	Status         string
}

func seedTranslationTerm(t *testing.T, tx pgx.Tx, seed translationTermSeed) {
	t.Helper()
	if seed.PromptVersion == "" {
		seed.PromptVersion = "v1"
	}
	if seed.Status == "" {
		seed.Status = "succeeded"
	}
	_, err := tx.Exec(t.Context(), `
INSERT INTO brreg_source.translation_terms (
  source, source_lang, target_lang, source_text_normalized, source_text,
  term_key, translated_text, status, prompt_version, metadata
) VALUES (
  'brreg', 'no', 'en', $1, $2, $3, $4, $5, $6, '{}'::jsonb
)
ON CONFLICT (source, source_lang, target_lang, prompt_version, term_key) DO UPDATE
SET translated_text = EXCLUDED.translated_text,
    status = EXCLUDED.status,
    updated_at = now()
`, normalizeTranslationText(seed.SourceText), seed.SourceText, translationTermKey(seed.SourceText), seed.TranslatedText, seed.Status, seed.PromptVersion)
	require.NoError(t, err)
}

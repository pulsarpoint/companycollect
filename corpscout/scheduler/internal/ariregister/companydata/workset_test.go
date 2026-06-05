package companydata

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/sourcetranslation"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestStoreLoadMissingTranslationFieldsUsesAriregisterFilters(t *testing.T) {
	tx := testdb.BeginTx(t)
	first := seedAriregisterCompanyData(t, tx, ariregisterCompanySeed{
		RegistryCode:            "999555401",
		LegalName:               "FILTER TARGET OU",
		RegistrationStatusLabel: " Registrisse kantud ",
		LegalFormLabel:          "Osaühing",
		AddressCountryLabel:     "Eesti",
	})
	second := seedAriregisterCompanyData(t, tx, ariregisterCompanySeed{
		RegistryCode:            "999555402",
		LegalName:               "FILTER OTHER OU",
		RegistrationStatusLabel: "Registrisse kantud",
		LegalFormLabel:          "Osaühing",
		AddressCountryLabel:     "Eesti",
	})
	store := New(tx)
	require.NoError(t, store.RefreshTranslationStatus(t.Context()))

	fields, err := store.LoadMissingTranslationFields(t.Context(), sourcetranslation.LoadMissingFieldsCommand{
		CompanyIDs: []string{
			" " + first.CompanyID.String() + " ",
			second.CompanyID.String(),
		},
		Filters:      map[string]string{"query": "target", "lifecycle_status": "active", "registration_status": "active", "translation_status": "missing"},
		CompanyLimit: 1,
		FieldLimit:   3,
	})

	require.NoError(t, err)
	require.Len(t, fields, 3)
	fieldsByTarget := make(map[string]sourcetranslation.MissingField)
	for _, field := range fields {
		require.Equal(t, first.CompanyID.String(), field.CompanyID)
		fieldsByTarget[field.TargetColumn] = field
	}
	registration := fieldsByTarget["registration_status_label_en"]
	require.Equal(t, "ariregister_source.companies", registration.SourceTable)
	require.Equal(t, first.CompanyID.String(), registration.SourceRowID)
	require.Equal(t, "registration_status_label", registration.SourceColumn)
	require.Equal(t, "Registrisse kantud", registration.SourceText)
	require.Equal(t, "registrisse kantud", registration.SourceTextNormalized)
	require.Equal(t, sourcetranslation.TermKey("Registrisse kantud"), registration.TermKey)
	require.EqualValues(t, 20, registration.Priority)
	require.Contains(t, fieldsByTarget, "legal_form_label_en")
	require.Contains(t, fieldsByTarget, "country_label_en")
}

func TestStoreLoadCachedTranslationTermsReturnsSucceededAriregisterTerms(t *testing.T) {
	tx := testdb.BeginTx(t)
	_, err := tx.Exec(t.Context(), `
INSERT INTO ariregister_source.translation_terms (
  source, source_lang, target_lang, source_text_normalized, source_text,
  term_key, translated_text, status, prompt_version
) VALUES
  ('ariregister', 'et', 'en', 'osaühing', 'Osaühing', $1, 'Private limited company', 'succeeded', 'v1'),
  ('ariregister', 'et', 'en', 'eesti', 'Eesti', $2, 'Estonia', 'failed_retryable', 'v1'),
  ('ariregister', 'et', 'en', 'registrisse kantud', 'Registrisse kantud', $3, 'Entered in the register', 'succeeded', 'v2'),
  ('ariregister', 'et', 'en', 'märkus', 'Märkus', $4, '   ', 'succeeded', 'v1')
`, sourcetranslation.TermKey("Osaühing"), sourcetranslation.TermKey("Eesti"), sourcetranslation.TermKey("Registrisse kantud"), sourcetranslation.TermKey("Märkus"))
	require.NoError(t, err)

	terms, err := New(tx).LoadCachedTranslationTerms(t.Context(), sourcetranslation.LoadCachedTermsCommand{
		PromptVersion: "v1",
		TermKeys: []string{
			sourcetranslation.TermKey("Osaühing"),
			sourcetranslation.TermKey("Eesti"),
			sourcetranslation.TermKey("Registrisse kantud"),
			sourcetranslation.TermKey("Märkus"),
			sourcetranslation.TermKey("Puudub"),
		},
	})

	require.NoError(t, err)
	require.Equal(t, map[string]sourcetranslation.CachedTerm{
		sourcetranslation.TermKey("Osaühing"): {
			TermKey:        sourcetranslation.TermKey("Osaühing"),
			TranslatedText: "Private limited company",
		},
	}, terms)
}

func TestStoreSaveTranslationTermsPersistsAriregisterTerms(t *testing.T) {
	tx := testdb.BeginTx(t)
	result, err := New(tx).SaveTranslationTerms(t.Context(), sourcetranslation.SaveTermsCommand{
		PromptVersion: "v1",
		Terms: []sourcetranslation.TranslationTermResult{
			{
				SourceText:           "Osaühing",
				SourceTextNormalized: "osaühing",
				TermKey:              sourcetranslation.TermKey("Osaühing"),
				TranslatedText:       "Private limited company",
				Status:               "succeeded",
				Provider:             "test-provider",
				Model:                "test-model",
				Metadata:             map[string]any{"source": "test"},
			},
			{
				SourceText:    "Eesti",
				TermKey:       sourcetranslation.TermKey("Eesti"),
				Status:        "failed_retryable",
				Error:         "temporary",
				PromptVersion: "v2",
				Metadata:      map[string]any{},
			},
		},
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.TermsSaved)
	var count int64
	err = tx.QueryRow(t.Context(), `
SELECT count(*)
FROM ariregister_source.translation_terms
WHERE source = 'ariregister'
  AND source_lang = 'et'
  AND target_lang = 'en'
  AND term_key IN ($1, $2)
`, sourcetranslation.TermKey("Osaühing"), sourcetranslation.TermKey("Eesti")).Scan(&count)
	require.NoError(t, err)
	require.EqualValues(t, 2, count)
}

func TestStoreApplyCompanyTranslationsUpdatesSupportedColumns(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedAriregisterCompanyData(t, tx, ariregisterCompanySeed{
		RegistryCode:            "999555403",
		LegalName:               "APPLY TARGET OU",
		LegalNameEN:             "Existing legal name",
		RegistrationStatusLabel: "Registrisse kantud",
		LegalFormLabel:          "Osaühing",
		AddressCountryLabel:     "Eesti",
	})

	result, err := New(tx).ApplyCompanyTranslations(t.Context(), sourcetranslation.ApplyCompanyTranslationsCommand{
		CompanyID: seed.CompanyID.String(),
		Bindings: []sourcetranslation.TranslationBinding{
			{
				ID:             1,
				CompanyID:      seed.CompanyID.String(),
				SourceTable:    "ariregister_source.companies",
				SourceRowID:    seed.CompanyID.String(),
				SourceColumn:   "registration_status_label",
				TargetColumn:   "registration_status_label_en",
				TranslatedText: "Entered in the register",
			},
			{
				ID:             2,
				CompanyID:      seed.CompanyID.String(),
				SourceTable:    "ariregister_source.legal_forms",
				SourceRowID:    seed.LegalFormID.String(),
				SourceColumn:   "legal_form_label",
				TargetColumn:   "legal_form_label_en",
				TranslatedText: "Private limited company",
			},
			{
				ID:             3,
				CompanyID:      seed.CompanyID.String(),
				SourceTable:    "ariregister_source.addresses",
				SourceRowID:    seed.AddressID.String(),
				SourceColumn:   "country_label",
				TargetColumn:   "country_label_en",
				TranslatedText: "Estonia",
			},
			{
				ID:             4,
				CompanyID:      seed.CompanyID.String(),
				SourceTable:    "ariregister_source.companies",
				SourceRowID:    seed.CompanyID.String(),
				SourceColumn:   "legal_name",
				TargetColumn:   "legal_name_en",
				TranslatedText: "Translated legal name",
			},
		},
	})

	require.NoError(t, err)
	require.EqualValues(t, 3, result.BindingsApplied)
	require.Equal(t, []int64{1, 2, 3}, result.AppliedBindingIDs)
	var registrationStatusEN, legalNameEN, legalFormEN, countryEN string
	err = tx.QueryRow(t.Context(), `
SELECT
  company.registration_status_label_en,
  company.legal_name_en,
  form.legal_form_label_en,
  address.country_label_en
FROM ariregister_source.companies company
JOIN ariregister_source.legal_forms form ON form.company_id = company.id
JOIN ariregister_source.addresses address ON address.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(&registrationStatusEN, &legalNameEN, &legalFormEN, &countryEN)
	require.NoError(t, err)
	require.Equal(t, "Entered in the register", registrationStatusEN)
	require.Equal(t, "Existing legal name", legalNameEN)
	require.Equal(t, "Private limited company", legalFormEN)
	require.Equal(t, "Estonia", countryEN)
}

func TestStoreApplyCompanyTranslationsRejectsUnsupportedTargetColumn(t *testing.T) {
	tx := testdb.BeginTx(t)
	seed := seedAriregisterCompanyData(t, tx, ariregisterCompanySeed{
		RegistryCode:            "999555404",
		LegalName:               "UNSUPPORTED TARGET OU",
		RegistrationStatusLabel: "Registrisse kantud",
	})

	_, err := New(tx).ApplyCompanyTranslations(t.Context(), sourcetranslation.ApplyCompanyTranslationsCommand{
		CompanyID: seed.CompanyID.String(),
		Bindings: []sourcetranslation.TranslationBinding{{
			ID:             42,
			CompanyID:      seed.CompanyID.String(),
			SourceTable:    "ariregister_source.companies",
			SourceRowID:    seed.CompanyID.String(),
			SourceColumn:   "registration_status_label",
			TargetColumn:   "unsupported_en",
			TranslatedText: "Unsupported",
		}},
	})

	require.Error(t, err)
	require.ErrorContains(t, err, "unsupported ariregister company translation target column")
}

type ariregisterCompanySeed struct {
	RegistryCode            string
	LegalName               string
	LegalNameEN             string
	RegistrationStatusLabel string
	LegalFormLabel          string
	AddressCountryLabel     string
}

type seededAriregisterCompanyData struct {
	RawRecordID uuid.UUID
	CompanyID   uuid.UUID
	LegalFormID uuid.UUID
	AddressID   uuid.UUID
}

func seedAriregisterCompanyData(t *testing.T, tx pgx.Tx, seed ariregisterCompanySeed) seededAriregisterCompanyData {
	t.Helper()
	ctx := context.Background()
	rawRecordID := uuid.New()
	companyID := uuid.New()
	legalFormID := uuid.New()
	addressID := uuid.New()

	_, err := tx.Exec(ctx, `
INSERT INTO ariregister_workflow.raw_records (
  id, source_native_id, registry_code, legal_name, registration_status,
  legal_form, country_iso2, raw_payload, payload_hash
) VALUES ($1, $2, $2, $3, 'Registrisse kantud', 'Osaühing', 'EE', '{}'::jsonb, $4)
`, rawRecordID, seed.RegistryCode, seed.LegalName, sourcetranslation.TermKey(seed.RegistryCode))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO ariregister_source.companies (
  id, raw_record_id, registry_code, source_native_id, country_iso2,
  legal_name, legal_name_normalized, legal_name_en, registration_status,
  registration_status_label, lifecycle_status, legal_form_code,
  legal_form_label, payload_hash, row_status
) VALUES ($1, $2, $3, $3, 'EE', $4, lower($4), $5, 'active', $6, 'active', 'OÜ', $7, $8, 'active')
`, companyID, rawRecordID, seed.RegistryCode, seed.LegalName, nullText(seed.LegalNameEN),
		nullText(seed.RegistrationStatusLabel), nullText(seed.LegalFormLabel), sourcetranslation.TermKey(seed.RegistryCode))
	require.NoError(t, err)

	if seed.LegalFormLabel != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO ariregister_source.legal_forms (
  id, company_id, raw_record_id, source_entry_id, legal_form_code,
  legal_form_label, raw_legal_form_payload
) VALUES ($1, $2, $3, 1, 'OÜ', $4, '{}'::jsonb)
`, legalFormID, companyID, rawRecordID, seed.LegalFormLabel)
		require.NoError(t, err)
	}

	if seed.AddressCountryLabel != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO ariregister_source.addresses (
  id, company_id, raw_record_id, source_entry_id, address_type,
  country_code, country_label, raw_address_payload
) VALUES ($1, $2, $3, 1, 'registered', 'EST', $4, '{}'::jsonb)
`, addressID, companyID, rawRecordID, seed.AddressCountryLabel)
		require.NoError(t, err)
	}

	return seededAriregisterCompanyData{
		RawRecordID: rawRecordID,
		CompanyID:   companyID,
		LegalFormID: legalFormID,
		AddressID:   addressID,
	}
}

func nullText(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

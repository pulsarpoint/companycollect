package translation

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestListFieldsForTranslationReturnsMissingFieldsUpToLimit(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber:    "991000001",
		OrganizationName:      "LIMIT TEST AS",
		OrganizationForm:      "Aksjeselskap",
		ResponseClass:         "Enhet",
		ActivityDescription:   "Utvikling av programvare",
		StatutoryPurpose:      "Investering i aksjer",
		CapitalType:           "Aksjekapital",
		TranslatedFormLabel:   "Limited company",
		TranslatedCapitalType: "Share capital",
	})

	fields, err := ListFieldsForTranslation(ctx, tx, ListFieldsForTranslationCommand{Limit: 3})

	require.NoError(t, err)
	require.Len(t, fields, 3)
	for _, field := range fields {
		require.NotZero(t, field.SourceRowID)
		require.NotZero(t, field.CompanyID)
		require.NotEmpty(t, field.SourceTable)
		require.NotEmpty(t, field.SourceColumn)
		require.NotEmpty(t, field.TargetColumn)
		require.NotEmpty(t, field.SourceText)
		require.Equal(t, strings.ToLower(strings.TrimSpace(field.SourceText)), field.SourceTextNormalized)
		require.Equal(t, brregTranslationTermKey(field.SourceText), field.TermKey)
	}
}

func TestListFieldsForTranslationSkipsInactiveAndTranslatedFields(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber:  "991000002",
		OrganizationName:    "ACTIVE TEST AS",
		OrganizationForm:    "Aksjeselskap",
		TranslatedFormLabel: "Limited company",
		ResponseClass:       "Enhet",
	})
	seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber: "991000003",
		OrganizationName:   "INACTIVE TEST AS",
		OrganizationForm:   "Forening",
		ResponseClass:      "Enhet",
		RowStatus:          "superseded",
	})

	fields, err := ListFieldsForTranslation(ctx, tx, ListFieldsForTranslationCommand{Limit: 50})

	require.NoError(t, err)
	require.NotEmpty(t, fields)
	for _, field := range fields {
		require.NotEqual(t, "organization_form_label", field.SourceColumn)
		require.NotEqual(t, "Forening", field.SourceText)
	}
}

func TestListFieldsForCompanyTranslationOnlyReturnsSelectedCompany(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	selected := seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber:  "991000006",
		OrganizationName:    "SELECTED COMPANY TEST AS",
		OrganizationForm:    "Aksjeselskap",
		ResponseClass:       "Enhet",
		ActivityDescription: "Utvikling av programvare",
		StatutoryPurpose:    "Investering i aksjer",
		CapitalType:         "Aksjekapital",
	})
	other := seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber: "991000007",
		OrganizationName:   "OTHER COMPANY TEST AS",
		OrganizationForm:   "Forening",
		ResponseClass:      "Enhet",
	})

	fields, err := ListFieldsForCompanyTranslation(ctx, tx, ListFieldsForCompanyTranslationCommand{
		CompanyID: selected.CompanyID,
		Limit:     50,
	})

	require.NoError(t, err)
	require.NotEmpty(t, fields)
	for _, field := range fields {
		require.Equal(t, selected.CompanyID, field.CompanyID)
		require.NotEqual(t, other.CompanyID, field.CompanyID)
	}
}

func TestApplyCachedTranslationsForFieldsUpdatesOnlyCachedValues(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	seed := seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber:  "991000004",
		OrganizationName:    "CACHE TEST AS",
		OrganizationForm:    "Aksjeselskap",
		ResponseClass:       "Enhet",
		ActivityDescription: "Utvikling av programvare",
		StatutoryPurpose:    "Investering i aksjer",
		CapitalType:         "Aksjekapital",
	})
	seedSucceededTranslationTerm(t, tx, "v1", "Aksjeselskap", "Limited liability company")
	seedSucceededTranslationTerm(t, tx, "v1", "Enhet", "Entity")
	seedSucceededTranslationTerm(t, tx, "v1", "Aksjekapital", "Share capital")

	fields, err := ListFieldsForTranslation(ctx, tx, ListFieldsForTranslationCommand{Limit: 50})
	require.NoError(t, err)

	result, err := ApplyCachedTranslationsForFields(ctx, tx, ApplyCachedTranslationsForFieldsCommand{
		PromptVersion: "v1",
		Fields:        fields,
	})

	require.NoError(t, err)
	require.EqualValues(t, len(fields), result.FieldsSeen)
	require.EqualValues(t, 3, result.FieldsApplied)
	require.EqualValues(t, len(fields)-3, result.FieldsMissingCachedTranslation)

	var organizationFormLabelEN string
	var responseClassEN string
	var activityDescriptionEN *string
	var statutoryPurposeEN *string
	err = tx.QueryRow(ctx, `
SELECT organization_form_label_en, response_class_en, activity_description_en, statutory_purpose_en
FROM brreg_source.companies
WHERE id = $1
`, seed.CompanyID).Scan(
		&organizationFormLabelEN,
		&responseClassEN,
		&activityDescriptionEN,
		&statutoryPurposeEN,
	)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Nil(t, activityDescriptionEN)
	require.Nil(t, statutoryPurposeEN)

	var capitalTypeEN string
	err = tx.QueryRow(ctx, `
SELECT capital_type_en
FROM brreg_source.capital
WHERE id = $1
`, seed.CapitalID).Scan(&capitalTypeEN)
	require.NoError(t, err)
	require.Equal(t, "Share capital", capitalTypeEN)
}

func TestApplyCachedTranslationsForCompanyUsesOnlySelectedCompanyFields(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	selected := seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber: "991000008",
		OrganizationName:   "SELECTED CACHE TEST AS",
		OrganizationForm:   "Aksjeselskap",
		ResponseClass:      "Enhet",
	})
	other := seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber: "991000009",
		OrganizationName:   "OTHER CACHE TEST AS",
		OrganizationForm:   "Aksjeselskap",
		ResponseClass:      "Enhet",
	})
	seedSucceededTranslationTerm(t, tx, "v1", "Aksjeselskap", "Limited liability company")
	seedSucceededTranslationTerm(t, tx, "v1", "Enhet", "Entity")

	result, err := ApplyCachedTranslationsForCompany(ctx, tx, ApplyCachedTranslationsForCompanyCommand{
		CompanyID:     selected.CompanyID,
		PromptVersion: "v1",
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.FieldsSeen)
	require.EqualValues(t, 2, result.FieldsApplied)

	var selectedFormEN string
	var selectedResponseEN string
	err = tx.QueryRow(ctx, `
SELECT organization_form_label_en, response_class_en
FROM brreg_source.companies
WHERE id = $1
`, selected.CompanyID).Scan(&selectedFormEN, &selectedResponseEN)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", selectedFormEN)
	require.Equal(t, "Entity", selectedResponseEN)

	var otherFormEN *string
	var otherResponseEN *string
	err = tx.QueryRow(ctx, `
SELECT organization_form_label_en, response_class_en
FROM brreg_source.companies
WHERE id = $1
`, other.CompanyID).Scan(&otherFormEN, &otherResponseEN)
	require.NoError(t, err)
	require.Nil(t, otherFormEN)
	require.Nil(t, otherResponseEN)
}

func TestApplyCachedTranslationsForFieldsDoesNotOverwriteExistingTargetValues(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	seed := seedBRREGTranslationCompany(t, tx, brregTranslationCompanySeed{
		OrganizationNumber:  "991000005",
		OrganizationName:    "NO OVERWRITE TEST AS",
		OrganizationForm:    "Aksjeselskap",
		TranslatedFormLabel: "Already translated",
		ResponseClass:       "Enhet",
	})
	seedSucceededTranslationTerm(t, tx, "v1", "Aksjeselskap", "Limited liability company")
	seedSucceededTranslationTerm(t, tx, "v1", "Enhet", "Entity")

	fields := []FieldForTranslation{
		{
			SourceTable:          "brreg_source.companies",
			SourceRowID:          seed.CompanyID,
			CompanyID:            seed.CompanyID,
			SourceColumn:         "organization_form_label",
			TargetColumn:         "organization_form_label_en",
			SourceLang:           "no",
			TargetLang:           "en",
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              brregTranslationTermKey("Aksjeselskap"),
		},
		{
			SourceTable:          "brreg_source.companies",
			SourceRowID:          seed.CompanyID,
			CompanyID:            seed.CompanyID,
			SourceColumn:         "response_class",
			TargetColumn:         "response_class_en",
			SourceLang:           "no",
			TargetLang:           "en",
			SourceText:           "Enhet",
			SourceTextNormalized: "enhet",
			TermKey:              brregTranslationTermKey("Enhet"),
		},
	}

	result, err := ApplyCachedTranslationsForFields(ctx, tx, ApplyCachedTranslationsForFieldsCommand{
		PromptVersion: "v1",
		Fields:        fields,
	})

	require.NoError(t, err)
	require.EqualValues(t, 2, result.FieldsSeen)
	require.EqualValues(t, 1, result.FieldsApplied)
	require.Zero(t, result.FieldsMissingCachedTranslation)

	var organizationFormLabelEN string
	var responseClassEN string
	err = tx.QueryRow(ctx, `
SELECT organization_form_label_en, response_class_en
FROM brreg_source.companies
WHERE id = $1
`, seed.CompanyID).Scan(&organizationFormLabelEN, &responseClassEN)
	require.NoError(t, err)
	require.Equal(t, "Already translated", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
}

type brregTranslationCompanySeed struct {
	OrganizationNumber    string
	OrganizationName      string
	OrganizationForm      string
	TranslatedFormLabel   string
	ResponseClass         string
	ActivityDescription   string
	StatutoryPurpose      string
	CapitalType           string
	TranslatedCapitalType string
	RowStatus             string
}

type seededBRREGTranslationCompany struct {
	RawRecordID uuid.UUID
	CompanyID   uuid.UUID
	CapitalID   uuid.UUID
}

func seedBRREGTranslationCompany(
	t *testing.T,
	tx pgx.Tx,
	seed brregTranslationCompanySeed,
) seededBRREGTranslationCompany {
	t.Helper()
	ctx := t.Context()
	rawRecordID := uuid.New()
	companyID := uuid.New()
	capitalID := uuid.New()
	rowStatus := seed.RowStatus
	if rowStatus == "" {
		rowStatus = "active"
	}
	if seed.OrganizationName == "" {
		seed.OrganizationName = seed.OrganizationNumber + " AS"
	}

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id, source_native_id, organization_number, organization_name,
  registration_status, country_iso2, raw_payload, payload_hash
) VALUES ($1, $2, $2, $3, 'active', 'NO', '{}'::jsonb, $4)
`, rawRecordID, seed.OrganizationNumber, seed.OrganizationName, brregTranslationTermKey(seed.OrganizationNumber))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id, raw_record_id, organization_number, source_native_id, organization_name,
  organization_name_normalized, country_iso2, organization_form_label, organization_form_label_en,
  response_class, activity_description, statutory_purpose,
  lifecycle_status, registration_status, row_status, payload_hash
) VALUES ($1, $2, $3, $3, $4, lower($4), 'NO', $5, $6, $7, $8, $9, 'active', 'active', $10, $11)
`, companyID, rawRecordID, seed.OrganizationNumber, seed.OrganizationName,
		nullString(seed.OrganizationForm),
		nullString(seed.TranslatedFormLabel),
		nullString(seed.ResponseClass),
		nullString(seed.ActivityDescription),
		nullString(seed.StatutoryPurpose),
		rowStatus,
		brregTranslationTermKey(seed.OrganizationNumber),
	)
	require.NoError(t, err)

	if seed.CapitalType != "" {
		_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id, company_id, raw_record_id, capital_type, capital_type_en, original_amount, original_currency, raw_capital_payload
) VALUES ($1, $2, $3, $4, $5, 30000, 'NOK', '{}'::jsonb)
`, capitalID, companyID, rawRecordID, seed.CapitalType, nullString(seed.TranslatedCapitalType))
		require.NoError(t, err)
	}

	return seededBRREGTranslationCompany{
		RawRecordID: rawRecordID,
		CompanyID:   companyID,
		CapitalID:   capitalID,
	}
}

func seedSucceededTranslationTerm(
	t *testing.T,
	tx pgx.Tx,
	promptVersion string,
	sourceText string,
	translatedText string,
) {
	t.Helper()
	_, err := tx.Exec(t.Context(), `
INSERT INTO brreg_source.translation_terms (
  source, source_lang, target_lang, source_text_normalized, source_text,
  term_key, translated_text, status, prompt_version, metadata
) VALUES (
  'brreg', 'no', 'en', $1, $2, $3, $4, 'succeeded', $5, '{}'::jsonb
)
ON CONFLICT (source, source_lang, target_lang, prompt_version, term_key) DO UPDATE
SET translated_text = EXCLUDED.translated_text,
    status = 'succeeded',
    updated_at = now()
`, strings.ToLower(strings.TrimSpace(sourceText)), sourceText, brregTranslationTermKey(sourceText), translatedText, promptVersion)
	require.NoError(t, err)
}

func nullString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func brregTranslationTermKey(sourceText string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(sourceText))))
	return hex.EncodeToString(sum[:])
}

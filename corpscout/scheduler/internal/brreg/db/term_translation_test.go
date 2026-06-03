package brregdb

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
)

func TestApplyCachedTermTranslationsUpdatesCompanyAndCapital(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	rawRecordID := uuid.New()
	companyID := uuid.New()
	capitalID := uuid.New()

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id, source_native_id, organization_number, organization_name,
  registration_status, country_iso2, raw_payload, payload_hash
) VALUES ($1, '999111222', '999111222', 'TERM TEST AS', 'active', 'NO', '{}'::jsonb, $2)
`, rawRecordID, termKey("TERM TEST AS"))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id, raw_record_id, organization_number, source_native_id, organization_name,
  organization_name_normalized, country_iso2, organization_form_label, response_class,
  activity_description, statutory_purpose,
  lifecycle_status, registration_status, row_status, payload_hash
) VALUES ($1, $2, '999111222', '999111222', 'TERM TEST AS', 'term test as', 'NO',
  'Aksjeselskap', 'Enhet', 'Utvikling av programvare', 'Investering i aksjer',
  'active', 'active', 'active', $3)
`, companyID, rawRecordID, termKey("999111222"))
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.capital (
  id, company_id, raw_record_id, capital_type, original_amount, original_currency, raw_capital_payload
) VALUES ($1, $2, $3, 'Aksjekapital', 30000, 'NOK', '{}'::jsonb)
`, capitalID, companyID, rawRecordID)
	require.NoError(t, err)

	result, err := New(tx).UpsertTranslationTerms(ctx, UpsertTranslationTermsCommand{
		Terms: []TranslationTermResult{
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: termKey("Aksjeselskap"), SourceTextNormalized: "aksjeselskap", SourceText: "Aksjeselskap",
				TranslatedText: "Limited liability company", Status: "succeeded",
			},
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: termKey("Enhet"), SourceTextNormalized: "enhet", SourceText: "Enhet",
				TranslatedText: "Entity", Status: "succeeded",
			},
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: termKey("Utvikling av programvare"), SourceTextNormalized: "utvikling av programvare", SourceText: "Utvikling av programvare",
				TranslatedText: "Software development", Status: "succeeded",
			},
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: termKey("Investering i aksjer"), SourceTextNormalized: "investering i aksjer", SourceText: "Investering i aksjer",
				TranslatedText: "Investment in shares", Status: "succeeded",
			},
			{
				SourceLang: "no", TargetLang: "en", PromptVersion: "v1",
				TermKey: termKey("Aksjekapital"), SourceTextNormalized: "aksjekapital", SourceText: "Aksjekapital",
				TranslatedText: "Share capital", Status: "succeeded",
			},
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 5, result.TermsUpserted)

	applied, err := New(tx).ApplyCachedTermTranslations(ctx, ApplyCachedTermTranslationsCommand{
		PromptVersion: "v1",
		Limit:         100,
	})
	require.NoError(t, err)
	require.EqualValues(t, 5, applied.FieldsApplied)

	var organizationFormLabelEN string
	var responseClassEN string
	var activityDescriptionEN string
	var statutoryPurposeEN string
	err = tx.QueryRow(ctx, `
SELECT organization_form_label_en, response_class_en, activity_description_en, statutory_purpose_en
FROM brreg_source.companies
WHERE id = $1
`, companyID).Scan(
		&organizationFormLabelEN,
		&responseClassEN,
		&activityDescriptionEN,
		&statutoryPurposeEN,
	)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "Software development", activityDescriptionEN)
	require.Equal(t, "Investment in shares", statutoryPurposeEN)

	var capitalTypeEN string
	err = tx.QueryRow(ctx, `
SELECT capital_type_en
FROM brreg_source.capital
WHERE id = $1
`, capitalID).Scan(&capitalTypeEN)
	require.NoError(t, err)
	require.Equal(t, "Share capital", capitalTypeEN)
}

func TestClaimQueuedTranslationTermsUsesSafeDefaults(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()

	_, err := New(tx).UpsertTranslationTerms(ctx, UpsertTranslationTermsCommand{
		Terms: []TranslationTermResult{{
			SourceLang:           "no",
			TargetLang:           "en",
			PromptVersion:        "v1",
			TermKey:              termKey("Aksjeselskap"),
			SourceTextNormalized: "aksjeselskap",
			SourceText:           "Aksjeselskap",
			Status:               "failed_retryable",
		}},
	})
	require.NoError(t, err)

	terms, err := New(tx).ClaimQueuedTranslationTerms(ctx, ClaimQueuedTranslationTermsCommand{})
	require.NoError(t, err)
	require.Len(t, terms, 1)
	require.Equal(t, "Aksjeselskap", terms[0].SourceText)
	require.EqualValues(t, 1, terms[0].AttemptCount)
}

func termKey(sourceText string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(sourceText))))
	return hex.EncodeToString(sum[:])
}

package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/brreg/companydata"
	"github.com/pulsarpoint/corpscout/scheduler/internal/testdb"
	"github.com/pulsarpoint/corpscout/scheduler/internal/translationclient"
)

func TestProcessBrregCompanyTranslationTranslatesMissingTermsAndMarksSucceeded(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	seed := seedCompanyTranslationActionCompany(t, tx, companyTranslationActionSeed{
		OrganizationNumber:    "999222111",
		OrganizationName:      "ACTION TRANSLATION AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	seedCompanyTranslationRunningStatus(t, tx, seed.CompanyID)

	serverURL := startTermTranslationNATSServer(t)
	responder := connectTermTranslationNATS(t, serverURL)
	defer responder.Close()
	requests := make(chan translationclient.TermTranslationRequest, 1)
	_, err := responder.Subscribe(translationclient.DefaultTermTranslationRequestSubject, func(msg *nats.Msg) {
		var request translationclient.TermTranslationRequest
		require.NoError(t, json.Unmarshal(msg.Data, &request))
		requests <- request
		response := translationclient.TermTranslationResult{
			RequestID:     request.RequestID,
			Source:        request.Source,
			SourceLang:    request.SourceLang,
			TargetLang:    request.TargetLang,
			Provider:      request.Provider,
			Model:         request.Model,
			PromptVersion: request.PromptVersion,
			Results: []translationclient.TermTranslationResultItem{
				{
					TermKey:              termKeyForCompanyTranslationAction("Aksjeselskap"),
					SourceText:           "Aksjeselskap",
					SourceTextNormalized: "aksjeselskap",
					TranslatedText:       "Limited liability company",
					Status:               "succeeded",
				},
				{
					TermKey:              termKeyForCompanyTranslationAction("Enhet"),
					SourceText:           "Enhet",
					SourceTextNormalized: "enhet",
					TranslatedText:       "Entity",
					Status:               "succeeded",
				},
			},
		}
		payload, marshalErr := json.Marshal(response)
		require.NoError(t, marshalErr)
		require.NoError(t, msg.Respond(payload))
	})
	require.NoError(t, err)
	require.NoError(t, responder.FlushTimeout(2*time.Second))

	translator, err := translationclient.NewNATSWithSubject(serverURL, translationclient.DefaultTermTranslationRequestSubject)
	require.NoError(t, err)
	defer translator.Close()
	action := NewCompanyTranslationActions(companydata.New(tx), translator)

	result, err := action.ProcessBrregCompanyTranslation(ctx, ProcessBrregCompanyTranslationInput{
		CompanyID:     seed.CompanyID.String(),
		Provider:      "mock",
		Model:         "mock-fast",
		PromptVersion: "v1",
		MaxAttempts:   3,
	})

	require.NoError(t, err)
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.TermsRequested)
	require.EqualValues(t, 2, result.TermsSucceeded)
	require.EqualValues(t, 0, result.RemainingFields)
	request := receiveCompanyTranslationTermRequest(t, requests)
	require.Equal(t, "mock", request.Provider)
	require.Equal(t, "mock-fast", request.Model)
	require.Len(t, request.Terms, 2)

	var organizationFormLabelEN string
	var responseClassEN string
	var translationStatus string
	err = tx.QueryRow(ctx, `
SELECT company.organization_form_label_en, company.response_class_en, status.translation_status
FROM brreg_source.companies company
JOIN brreg_source.company_process_status status ON status.company_id = company.id
WHERE company.id = $1
`, seed.CompanyID).Scan(&organizationFormLabelEN, &responseClassEN, &translationStatus)
	require.NoError(t, err)
	require.Equal(t, "Limited liability company", organizationFormLabelEN)
	require.Equal(t, "Entity", responseClassEN)
	require.Equal(t, "succeeded", translationStatus)
}

func TestProcessBrregCompanyTranslationUsesCacheWithoutTranslator(t *testing.T) {
	tx := testdb.BeginTx(t)
	ctx := t.Context()
	seed := seedCompanyTranslationActionCompany(t, tx, companyTranslationActionSeed{
		OrganizationNumber:    "999222333",
		OrganizationName:      "CACHE TRANSLATION AS",
		OrganizationFormLabel: "Aksjeselskap",
		ResponseClass:         "Enhet",
	})
	seedCompanyTranslationRunningStatus(t, tx, seed.CompanyID)
	_, err := companydata.New(tx).SaveTranslationTerms(ctx, []companydata.TranslationTermResult{
		{
			SourceText:           "Aksjeselskap",
			SourceTextNormalized: "aksjeselskap",
			TermKey:              termKeyForCompanyTranslationAction("Aksjeselskap"),
			TranslatedText:       "Limited liability company",
			Status:               "succeeded",
			Provider:             "cache",
			Model:                "cache",
			PromptVersion:        "v1",
		},
		{
			SourceText:           "Enhet",
			SourceTextNormalized: "enhet",
			TermKey:              termKeyForCompanyTranslationAction("Enhet"),
			TranslatedText:       "Entity",
			Status:               "succeeded",
			Provider:             "cache",
			Model:                "cache",
			PromptVersion:        "v1",
		},
	})
	require.NoError(t, err)

	action := NewCompanyTranslationActions(companydata.New(tx), nil)
	result, err := action.ProcessBrregCompanyTranslation(ctx, ProcessBrregCompanyTranslationInput{
		CompanyID:     seed.CompanyID.String(),
		PromptVersion: "v1",
		MaxAttempts:   3,
	})

	require.NoError(t, err)
	require.Equal(t, "succeeded", result.Status)
	require.EqualValues(t, 2, result.FieldsSeen)
	require.EqualValues(t, 2, result.FieldsApplied)
	require.Zero(t, result.TermsRequested)

	var translationStatus string
	err = tx.QueryRow(ctx, `
SELECT translation_status
FROM brreg_source.company_process_status
WHERE company_id = $1
`, seed.CompanyID).Scan(&translationStatus)
	require.NoError(t, err)
	require.Equal(t, "succeeded", translationStatus)
}

type companyTranslationActionSeed struct {
	OrganizationNumber    string
	OrganizationName      string
	OrganizationFormLabel string
	ResponseClass         string
}

type seededCompanyTranslationActionCompany struct {
	RawRecordID uuid.UUID
	CompanyID   uuid.UUID
}

func seedCompanyTranslationActionCompany(
	t *testing.T,
	tx pgx.Tx,
	seed companyTranslationActionSeed,
) seededCompanyTranslationActionCompany {
	t.Helper()
	ctx := context.Background()
	rawRecordID := uuid.New()
	companyID := uuid.New()
	payloadHash := termKeyForCompanyTranslationAction(seed.OrganizationNumber)

	_, err := tx.Exec(ctx, `
INSERT INTO brreg_workflow.raw_records (
  id, source_native_id, organization_number, organization_name,
  registration_status, country_iso2, raw_payload, payload_hash
) VALUES ($1, $2, $2, $3, 'active', 'NO', '{}'::jsonb, $4)
`, rawRecordID, seed.OrganizationNumber, seed.OrganizationName, payloadHash)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, `
INSERT INTO brreg_source.companies (
  id, raw_record_id, organization_number, source_native_id, organization_name,
  organization_name_normalized, country_iso2, organization_form_label, response_class,
  lifecycle_status, registration_status, row_status, payload_hash
) VALUES ($1, $2, $3, $3, $4, lower($4), 'NO', $5, $6, 'active', 'active', 'active', $7)
`, companyID, rawRecordID, seed.OrganizationNumber, seed.OrganizationName,
		seed.OrganizationFormLabel,
		seed.ResponseClass,
		payloadHash,
	)
	require.NoError(t, err)
	return seededCompanyTranslationActionCompany{RawRecordID: rawRecordID, CompanyID: companyID}
}

func seedCompanyTranslationRunningStatus(t *testing.T, tx pgx.Tx, companyID uuid.UUID) {
	t.Helper()
	_, err := tx.Exec(t.Context(), `
INSERT INTO brreg_source.company_process_status (
  company_id,
  translation_status,
  translation_attempt_count,
  translation_lease_by,
  translation_lease_until
) VALUES ($1, 'running', 1, 'test-worker', now() + interval '15 minutes')
`, companyID)
	require.NoError(t, err)
}

func receiveCompanyTranslationTermRequest(
	t *testing.T,
	requests chan translationclient.TermTranslationRequest,
) translationclient.TermTranslationRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for term translation request")
		return translationclient.TermTranslationRequest{}
	}
}

func termKeyForCompanyTranslationAction(sourceText string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(sourceText))))
	return hex.EncodeToString(sum[:])
}

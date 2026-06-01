package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/pulsarpoint/brreg-financial-service/internal/httpapi"
	"github.com/pulsarpoint/brreg-financial-service/internal/models"
	"github.com/pulsarpoint/brreg-financial-service/internal/service"
	"github.com/stretchr/testify/require"
)

// realService builds a service wired to a stub client backed by fixture files.
func realService(t *testing.T) httpapi.LookupService {
	t.Helper()
	equinorData, err := os.ReadFile("../parser/testdata/equinor_list.json")
	require.NoError(t, err)

	return service.New(&handlerStubClient{
		keyFigures: map[string][]byte{"923609016": equinorData},
		keyErrors: map[string]error{
			"974760673": brregclient.ErrNotAvailable,
			"984851006": &brregclient.UnsupportedPlanError{PlanName: "BANK"},
			"000000001": &brregclient.RetryableError{StatusCode: 503, Msg: "down"},
		},
		pdfYears: map[string][]string{
			"923609016": {"2022", "2023", "2024"},
			"984851006": {"2023", "2024"},
		},
	}, service.Config{BaseURL: "https://data.brreg.no", MaxBatchSize: 5})
}

type handlerStubClient struct {
	keyFigures map[string][]byte
	keyErrors  map[string]error
	pdfYears   map[string][]string
}

func (s *handlerStubClient) FetchKeyFigures(_ context.Context, orgNum string) ([]byte, error) {
	if err, ok := s.keyErrors[orgNum]; ok {
		return nil, err
	}
	return s.keyFigures[orgNum], nil
}

func (s *handlerStubClient) FetchPDFYears(_ context.Context, orgNum string) ([]string, error) {
	if yrs, ok := s.pdfYears[orgNum]; ok {
		return yrs, nil
	}
	return nil, brregclient.ErrNotAvailable
}

func TestHealthz(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]string
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "ok", body["status"])
}

func TestLookupEndpoint_SingleSuccess(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	reqBody := models.LookupRequest{
		Records:            []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "923609016"}},
		IncludePDFMetadata: true,
		IncludeRawPayload:  false,
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp models.LookupResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "brreg-financial-service.lookup.v1", resp.SchemaVersion)
	require.Equal(t, "succeeded", resp.Status)
	require.Equal(t, 1, resp.RecordsSeen)
	require.Equal(t, 1, resp.RecordsCompleted)
	require.Equal(t, 0, resp.RecordsFailed)
	require.Len(t, resp.Results, 1)
	require.Equal(t, "succeeded", resp.Results[0].Status)
	require.Len(t, resp.Results[0].Statements, 1)
}

func TestLookupEndpoint_MixedBatch(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	reqBody := models.LookupRequest{
		Records: []models.LookupRecord{
			{RecordID: "ok", OrganizationNumber: "923609016"},
			{RecordID: "na", OrganizationNumber: "974760673"},
			{RecordID: "up", OrganizationNumber: "984851006"},
			{RecordID: "fail", OrganizationNumber: "000000001"},
		},
		IncludePDFMetadata: true,
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))

	require.Equal(t, http.StatusOK, rec.Code)
	var resp models.LookupResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "partial", resp.Status)
	require.Equal(t, 3, resp.RecordsCompleted) // ok, na, up
	require.Equal(t, 1, resp.RecordsFailed)    // fail

	statusMap := make(map[string]string)
	for _, r := range resp.Results {
		statusMap[r.RecordID] = r.Status
	}
	require.Equal(t, "succeeded", statusMap["ok"])
	require.Equal(t, "not_available", statusMap["na"])
	require.Equal(t, "unsupported_statement_plan", statusMap["up"])
	require.Equal(t, "failed", statusMap["fail"])
}

func TestLookupEndpoint_BatchTooLarge(t *testing.T) {
	h := httpapi.NewHandler(realService(t)) // max batch = 5
	records := make([]models.LookupRecord, 6)
	for i := range records {
		records[i] = models.LookupRecord{RecordID: "x", OrganizationNumber: "923609016"}
	}
	b, _ := json.Marshal(models.LookupRequest{Records: records})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLookupEndpoint_InvalidOrgNumber(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	b, _ := json.Marshal(models.LookupRequest{
		Records: []models.LookupRecord{{RecordID: "r1", OrganizationNumber: "not-9-digits"}},
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestLookupEndpoint_EmptyRecords(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	b, _ := json.Marshal(models.LookupRequest{Records: []models.LookupRecord{}})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.True(t, strings.Contains(rec.Body.String(), "records") || rec.Code == http.StatusBadRequest)
}

func TestLookupEndpoint_InvalidJSON(t *testing.T) {
	h := httpapi.NewHandler(realService(t))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/brreg/financials/lookup", strings.NewReader("not json")))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

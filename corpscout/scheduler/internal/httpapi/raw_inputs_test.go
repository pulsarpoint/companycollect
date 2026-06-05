package httpapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestListRawInputs_includesCVRAndAriregisterRows(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;gleif_company_raw_inputs;;companies_house_company_raw_inputs;;cvr_workflow.raw_records;;ariregister_workflow.raw_records;;company_name ILIKE;;legal_name ILIKE;;!cvr_company_raw_inputs;;!brreg_company_raw_inputs;;!ariregister_company_raw_inputs").
		WithArgs("%Registry%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(2)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;companies_house_company_raw_inputs;;cvr_workflow.raw_records;;ariregister_workflow.raw_records;;company_name ILIKE;;legal_name ILIKE;;!cvr_company_raw_inputs;;!brreg_company_raw_inputs;;!ariregister_company_raw_inputs").
		WithArgs("%Registry%", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("cvr-id", "cvr", "Danish Registry ApS", "12345678", "pending", nil, false, "pending", createdAt).
			AddRow("ari-id", "ariregister", "Estonian Registry OU", "87654321", "pending", nil, false, "pending", createdAt))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?q=Registry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source            string  `json:"source"`
			Name              string  `json:"name"`
			NativeID          string  `json:"native_id"`
			TranslationStatus *string `json:"translation_status"`
		} `json:"items"`
		Total int64 `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(2), body.Total)
	require.Len(t, body.Items, 2)
	assert.Equal(t, "cvr", body.Items[0].Source)
	assert.Equal(t, "Danish Registry ApS", body.Items[0].Name)
	assert.Equal(t, "12345678", body.Items[0].NativeID)
	assert.Nil(t, body.Items[0].TranslationStatus)
	assert.Equal(t, "ariregister", body.Items[1].Source)
	assert.Equal(t, "Estonian Registry OU", body.Items[1].Name)
	assert.Equal(t, "87654321", body.Items[1].NativeID)
	assert.Nil(t, body.Items[1].TranslationStatus)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_includesGLEIFRows(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;gleif_company_raw_inputs;;legal_name ILIKE;;!companies_house_company_raw_inputs;;!brreg_company_raw_inputs;;!cvr_company_raw_inputs;;!ariregister_company_raw_inputs").
		WithArgs("%Acme%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;gleif_company_raw_inputs;;legal_name ILIKE").
		WithArgs("%Acme%", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("gleif-id", "gleif", "Acme Global Ltd", "5493001KJTIIGC8Y1R12", "pending", nil, false, "pending", createdAt))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=gleif&q=Acme", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source            string  `json:"source"`
			Name              string  `json:"name"`
			NativeID          string  `json:"native_id"`
			TranslationStatus *string `json:"translation_status"`
		} `json:"items"`
		Total int64 `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(1), body.Total)
	require.Len(t, body.Items, 1)
	assert.Equal(t, "gleif", body.Items[0].Source)
	assert.Equal(t, "Acme Global Ltd", body.Items[0].Name)
	assert.Equal(t, "5493001KJTIIGC8Y1R12", body.Items[0].NativeID)
	assert.Nil(t, body.Items[0].TranslationStatus)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_ariregisterReadsWorkflowRawRecords(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 6, 4, 16, 43, 35, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;ariregister_workflow.raw_records;;legal_name ILIKE;;!ariregister_company_raw_inputs").
		WithArgs("%007%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;ariregister_workflow.raw_records;;'pending' AS status;;NULL::text AS translation_status;;'pending' AS state;;ri.first_seen_at AS created_at;;!ariregister_company_raw_inputs").
		WithArgs("%007%", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("ari-id", "ariregister", "007 Agent & Partners OÜ", "16752073", "pending", nil, false, "pending", createdAt))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=ariregister&q=007", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source            string  `json:"source"`
			Name              string  `json:"name"`
			NativeID          string  `json:"native_id"`
			Status            string  `json:"status"`
			TranslationStatus *string `json:"translation_status"`
		} `json:"items"`
		Total int64 `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(1), body.Total)
	require.Len(t, body.Items, 1)
	assert.Equal(t, "ariregister", body.Items[0].Source)
	assert.Equal(t, "007 Agent & Partners OÜ", body.Items[0].Name)
	assert.Equal(t, "16752073", body.Items[0].NativeID)
	assert.Equal(t, "pending", body.Items[0].Status)
	assert.Nil(t, body.Items[0].TranslationStatus)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_franceReadsWorkflowRawRecords(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 6, 4, 16, 43, 35, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;france_workflow.raw_legal_units;;france_workflow.raw_establishments;;display_name ILIKE;;!france_company_raw_inputs").
		WithArgs("%PULSAR%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(2)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;france_workflow.raw_legal_units;;france_workflow.raw_establishments;;'france' AS source;;'pending' AS status;;NULL::text AS translation_status;;ri.created_at AS created_at;;!france_company_raw_inputs").
		WithArgs("%PULSAR%", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("legal-id", "france", "PULSAR POINT FRANCE", "552100554", "pending", nil, false, "pending", createdAt).
			AddRow("establishment-id", "france", "PULSAR PARIS", "55210055400042", "pending", nil, false, "pending", createdAt))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=france&q=PULSAR", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source            string  `json:"source"`
			Name              string  `json:"name"`
			NativeID          string  `json:"native_id"`
			Status            string  `json:"status"`
			TranslationStatus *string `json:"translation_status"`
		} `json:"items"`
		Total int64 `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(2), body.Total)
	require.Len(t, body.Items, 2)
	assert.Equal(t, "france", body.Items[0].Source)
	assert.Equal(t, "PULSAR POINT FRANCE", body.Items[0].Name)
	assert.Equal(t, "552100554", body.Items[0].NativeID)
	assert.Equal(t, "pending", body.Items[0].Status)
	assert.Nil(t, body.Items[0].TranslationStatus)
	assert.Equal(t, "55210055400042", body.Items[1].NativeID)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_seReadsWorkflowRawRecords(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;se_workflow.bolagsverket_raw_records;;se_workflow.scb_raw_records;;display_name ILIKE;;!se_company_raw_inputs").
		WithArgs("%Pulsar%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;se_workflow.bolagsverket_raw_records;;se_workflow.scb_raw_records;;'pending' AS status;;NULL::text AS translation_status;;'pending' AS state;;ri.first_seen_at AS created_at;;!se_company_raw_inputs").
		WithArgs("%Pulsar%", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("se-id", "se", "Pulsar Sverige AB", "5599990000", "pending", nil, false, "pending", createdAt))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=se&q=Pulsar", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source            string  `json:"source"`
			Name              string  `json:"name"`
			NativeID          string  `json:"native_id"`
			Status            string  `json:"status"`
			TranslationStatus *string `json:"translation_status"`
		} `json:"items"`
		Total int64 `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(1), body.Total)
	require.Len(t, body.Items, 1)
	assert.Equal(t, "se", body.Items[0].Source)
	assert.Equal(t, "Pulsar Sverige AB", body.Items[0].Name)
	assert.Equal(t, "5599990000", body.Items[0].NativeID)
	assert.Equal(t, "pending", body.Items[0].Status)
	assert.Nil(t, body.Items[0].TranslationStatus)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_translationStatusFilterReturnsEmptyWithoutLegacyTranslatedSources(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?translation_status=translated", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			Source string `json:"source"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Empty(t, body.Items)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_allowsSortingByState(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;gleif_company_raw_inputs;;companies_house_company_raw_inputs;;cvr_workflow.raw_records;;ariregister_workflow.raw_records;;!cvr_company_raw_inputs;;!brreg_company_raw_inputs;;!ariregister_company_raw_inputs").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("raw_input_page;;ORDER BY state asc;;SELECT p.id;;p.state;;gleif_company_raw_inputs;;companies_house_company_raw_inputs;;cvr_workflow.raw_records;;ariregister_workflow.raw_records;;!cvr_company_raw_inputs;;!ariregister_company_raw_inputs").
		WithArgs(50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("gleif-id", "gleif", "Acme Global Ltd", "5493001KJTIIGC8Y1R12", "pending", nil, false, "pending", createdAt))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?sort=state&dir=asc", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_brregUsesDedicatedRawRecordsEndpoint(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs?source=brreg", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct{} `json:"items"`
		Total int64      `json:"total"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Empty(t, body.Items)
	assert.Zero(t, body.Total)
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListBrregRawRecordsRouteExists(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(nil, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/raw-records", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"database querier not available"`)
}

func TestListBrregRawRecordsReturnsWorkflowRawRecords(t *testing.T) {
	q := &stubQuerier{}
	recordID := uuid.New()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	q.On("CountBrregWorkflowRawRecords", mock.Anything, db.CountBrregWorkflowRawRecordsParams{
		Query:             ptrString("BORTIGARD"),
		LifecycleState:    ptrString("input"),
		TranslationStatus: ptrString("succeeded"),
	}).Return(int64(1000), nil)
	q.On("ListBrregWorkflowRawRecords", mock.Anything, db.ListBrregWorkflowRawRecordsParams{
		Query:             ptrString("BORTIGARD"),
		LifecycleState:    ptrString("input"),
		TranslationStatus: ptrString("succeeded"),
		SortBy:            "organization",
		SortDir:           "asc",
		Offset:            50,
		Limit:             50,
	}).Return([]db.BrregWorkflowVRawRecordList{{
		ID:                 recordID,
		OrganizationNumber: "810202572",
		OrganizationName:   ptrString("BORTIGARD AS"),
		Website:            ptrString("https://bortigard.no"),
		RegistrationStatus: ptrString("active"),
		CountryIso2:        "NO",
		PayloadHash:        "hash",
		IsCurrent:          true,
		FirstSeenAt:        now,
		LastSeenAt:         now,
		TranslationStatus:  "succeeded",
		DomainStatus:       "not_started",
		FinancialStatus:    "not_started",
		EnhancedStatus:     "not_started",
		LifecycleState:     "input",
		TaskStatuses:       json.RawMessage(`{"translate":"succeeded"}`),
		TaskErrors:         json.RawMessage(`{}`),
	}}, nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/raw-records?page=2&q=BORTIGARD&state=input&translation_status=succeeded&sort=organization&dir=asc", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []db.BrregWorkflowVRawRecordList `json:"items"`
		Total int64                            `json:"total"`
		Page  int                              `json:"page"`
		Limit int                              `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(1000), body.Total)
	require.Equal(t, 2, body.Page)
	require.Equal(t, 50, body.Limit)
	require.Len(t, body.Items, 1)
	require.Equal(t, "810202572", body.Items[0].OrganizationNumber)
	require.Equal(t, "BORTIGARD AS", *body.Items[0].OrganizationName)
	q.AssertExpectations(t)
}

func TestListBrregRawRecordsReturnsEmptyArrayWhenNoRows(t *testing.T) {
	q := &stubQuerier{}
	q.On("CountBrregWorkflowRawRecords", mock.Anything, db.CountBrregWorkflowRawRecordsParams{
		DomainStatus: ptrString("not_started"),
	}).Return(int64(0), nil)
	q.On("ListBrregWorkflowRawRecords", mock.Anything, db.ListBrregWorkflowRawRecordsParams{
		DomainStatus: ptrString("not_started"),
		SortBy:       "updated_at",
		SortDir:      "desc",
		Offset:       0,
		Limit:        50,
	}).Return([]db.BrregWorkflowVRawRecordList(nil), nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/raw-records?domain_status=not_started", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"items":[]`)

	var body struct {
		Items []db.BrregWorkflowVRawRecordList `json:"items"`
		Total int64                            `json:"total"`
		Page  int                              `json:"page"`
		Limit int                              `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.NotNil(t, body.Items)
	require.Empty(t, body.Items)
	require.Equal(t, int64(0), body.Total)
	require.Equal(t, 1, body.Page)
	require.Equal(t, 50, body.Limit)
	q.AssertExpectations(t)
}

func TestListBrregRawRecordsFiltersByDomainSearchEvidence(t *testing.T) {
	q := &stubQuerier{}
	q.On("CountBrregWorkflowRawRecords", mock.Anything, db.CountBrregWorkflowRawRecordsParams{
		DomainSearch: ptrString("with_markdown"),
	}).Return(int64(0), nil)
	q.On("ListBrregWorkflowRawRecords", mock.Anything, db.ListBrregWorkflowRawRecordsParams{
		DomainSearch: ptrString("with_markdown"),
		SortBy:       "updated_at",
		SortDir:      "desc",
		Offset:       0,
		Limit:        50,
	}).Return([]db.BrregWorkflowVRawRecordList(nil), nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/raw-records?domain_search=with_markdown", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"items":[]`)
	q.AssertExpectations(t)
}

func TestGetBrregRawRecordReturnsWorkflowRawRecordDetail(t *testing.T) {
	q := &stubQuerier{}
	recordID := uuid.New()
	actionAttemptID := uuid.New()
	artifactID := uuid.New()
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	q.On("GetBrregWorkflowRawRecordDetail", mock.Anything, recordID).Return(db.BrregWorkflowVRawRecordDetail{
		ID:                 recordID,
		OrganizationNumber: "810202572",
		OrganizationName:   ptrString("BORTIGARD AS"),
		CountryIso2:        "NO",
		PayloadHash:        "hash",
		IsCurrent:          true,
		FirstSeenAt:        now,
		LastSeenAt:         now,
		TranslationStatus:  "succeeded",
		DomainStatus:       "not_started",
		FinancialStatus:    "not_started",
		EnhancedStatus:     "not_started",
		LifecycleState:     "input",
		TaskStatuses:       json.RawMessage(`{"translate":"succeeded"}`),
		TaskErrors:         json.RawMessage(`{}`),
		RawPayload:         json.RawMessage(`{"navn":"BORTIGARD AS"}`),
		RawMetadata:        json.RawMessage(`{}`),
		TranslationResult:  json.RawMessage(`{"status":"succeeded"}`),
		DomainResult:       json.RawMessage(`{"status":null}`),
		FinancialResult:    json.RawMessage(`{"status":null}`),
		EnhancedResult:     json.RawMessage(`{"status":null}`),
		Tasks:              json.RawMessage(`[{"task_type":"translate","status":"succeeded"}]`),
	}, nil)
	q.On("ListBrregWorkflowDomainSearchEvidenceByRawRecord", mock.Anything, recordID).Return([]db.BrregWorkflowVDomainSearchEvidence{{
		ActionAttemptID:   actionAttemptID,
		RawRecordID:       recordID,
		SearchEngine:      ptrString("duckduckgo"),
		Attempt:           1,
		ActionStatus:      "succeeded",
		StartedAt:         now,
		SearchTerm:        "BORTIGARD AS NO website",
		ActionMetadata:    json.RawMessage(`{"search_engine":"duckduckgo"}`),
		ArtifactID:        artifactID,
		ArtifactCreatedAt: now,
		SearchUrl:         "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
		FinalUrl:          "https://html.duckduckgo.com/html/?q=BORTIGARD%20AS%20NO%20website",
		CrawlStatus:       "succeeded",
		Markdown:          "# Search results",
		MarkdownHash:      "hash",
		Links:             json.RawMessage(`["https://example.com"]`),
		CrawlMetadata:     json.RawMessage(`{}`),
		CrawlError:        json.RawMessage(`{}`),
		ArtifactPayload:   json.RawMessage(`{"markdown":"# Search results"}`),
		ArtifactMetadata:  json.RawMessage(`{}`),
	}}, nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/raw-records/"+recordID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body db.BrregWorkflowVRawRecordDetail
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, recordID, body.ID)
	require.Equal(t, "BORTIGARD AS", *body.OrganizationName)
	require.JSONEq(t, `{"navn":"BORTIGARD AS"}`, string(body.RawPayload))

	var fullBody struct {
		DomainSearchEvidence []db.BrregWorkflowVDomainSearchEvidence `json:"domain_search_evidence"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &fullBody))
	require.Len(t, fullBody.DomainSearchEvidence, 1)
	require.Equal(t, "BORTIGARD AS NO website", fullBody.DomainSearchEvidence[0].SearchTerm)
	require.Equal(t, "# Search results", fullBody.DomainSearchEvidence[0].Markdown)
	q.AssertExpectations(t)
}

func TestGetRawInput_includesGLEIFDetail(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("gleif_company_raw_inputs;;legal_name;;lei;;registration_status;;headquarters_country_code").
		WithArgs("raw-id").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "processing_status", "state", "company_type", "registration_status", "website", "country_iso2",
			"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload",
			"first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
		}).AddRow(
			"raw-id", "gleif", "Acme Global Ltd", "5493001KJTIIGC8Y1R12", "pending", "pending", "", "ACTIVE", "", "US",
			"run-1", 1, "", "hash", []byte(`{"lei":"5493001KJTIIGC8Y1R12"}`),
			now, now, nil, now, now,
		))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/gleif/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "gleif", body["source"])
	assert.Equal(t, "Acme Global Ltd", body["name"])
	assert.Equal(t, "5493001KJTIIGC8Y1R12", body["native_id"])
	assert.Equal(t, "ACTIVE", body["registration_status"])
	assert.Equal(t, "US", body["country_iso2"])
	assert.NotContains(t, body, "translation_status")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestGetRawInput_companiesHouseCoalescesNullableDetailTextFields(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("companies_house_company_raw_inputs;;COALESCE(ri.company_name,'');;COALESCE(ri.processing_status,'');;COALESCE(ri.company_type,'');;COALESCE(ri.country_iso2,'')").
		WithArgs("raw-id").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "processing_status", "state", "company_type", "registration_status", "website", "country_iso2",
			"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload",
			"first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
		}).AddRow(
			"raw-id", "companies_house", "", "12345678", "pending", "pending", "", "", "", "",
			"run-1", 1, "", "hash", []byte(`{"company_number":"12345678"}`),
			now, now, nil, now, now,
		))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/companies_house/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "companies_house", body["source"])
	assert.Equal(t, "", body["name"])
	assert.Equal(t, "12345678", body["native_id"])
	assert.Equal(t, "pending", body["status"])
	assert.NotContains(t, body, "company_type")
	assert.NotContains(t, body, "country_iso2")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestGetRawInput_cvrReadsWorkflowRawRecordDetail(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	now := time.Date(2026, 6, 4, 16, 43, 35, 0, time.UTC)
	pool.ExpectQuery("cvr_workflow.raw_records;;COALESCE(ri.company_name,'');;COALESCE(ri.cvr_number,'');;'pending';;COALESCE(ri.company_type,'');;COALESCE(ri.registration_status,'');;COALESCE(ri.website,'');;COALESCE(ri.country_iso2,'');;0;;ri.raw_payload;;ri.first_seen_at;;ri.last_seen_at;;!raw_payload_en;;!translation_status").
		WithArgs("raw-id").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "state", "company_type", "registration_status", "website", "country_iso2",
			"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload",
			"first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
		}).AddRow(
			"raw-id", "cvr", "Dansk Selskab ApS", "12345678", "pending", "pending", "APS", "NORMAL", "https://example.dk", "DK",
			"", 0, "", "hash", []byte(`{"Vrvirksomhed":{"cvrNummer":"12345678"}}`),
			now, now, nil, now, now,
		))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/cvr/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "cvr", body["source"])
	assert.Equal(t, "Dansk Selskab ApS", body["name"])
	assert.Equal(t, "12345678", body["native_id"])
	assert.Equal(t, "pending", body["status"])
	assert.Equal(t, "APS", body["company_type"])
	assert.Equal(t, "NORMAL", body["registration_status"])
	assert.Equal(t, "DK", body["country_iso2"])
	assert.NotContains(t, body, "translation_status")
	assert.NotContains(t, body, "raw_payload_en")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestGetRawInput_ariregisterReadsWorkflowRawRecordDetail(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	now := time.Date(2026, 6, 4, 16, 43, 35, 0, time.UTC)
	pool.ExpectQuery("ariregister_workflow.raw_records;;COALESCE(ri.legal_name,'');;COALESCE(ri.registry_code,'');;'pending';;COALESCE(ri.legal_form,'');;COALESCE(ri.registration_status,'');;COALESCE(ri.website,'');;COALESCE(ri.country_iso2,'');;0;;ri.raw_payload;;ri.first_seen_at;;ri.last_seen_at;;!raw_payload_en;;!translation_status").
		WithArgs("raw-id").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "state", "company_type", "registration_status", "website", "country_iso2",
			"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload",
			"first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
		}).AddRow(
			"raw-id", "ariregister", "007 Agent & Partners OÜ", "16752073", "pending", "pending", "Osaühing", "Registrisse kantud", "", "EE",
			"", 0, "", "hash", []byte(`{"arinimi":"007 Agent & Partners OÜ"}`),
			now, now, nil, now, now,
		))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/ariregister/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "ariregister", body["source"])
	assert.Equal(t, "007 Agent & Partners OÜ", body["name"])
	assert.Equal(t, "16752073", body["native_id"])
	assert.Equal(t, "pending", body["status"])
	assert.Equal(t, "Osaühing", body["company_type"])
	assert.Equal(t, "Registrisse kantud", body["registration_status"])
	assert.Equal(t, "EE", body["country_iso2"])
	assert.NotContains(t, body, "translation_status")
	assert.NotContains(t, body, "raw_payload_en")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestGetRawInput_franceReadsWorkflowRawRecordDetail(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	now := time.Date(2026, 6, 4, 16, 43, 35, 0, time.UTC)
	pool.ExpectQuery("france_workflow.raw_legal_units;;france_workflow.raw_establishments;;COALESCE(ri.display_name,'');;COALESCE(ri.native_id,'');;'pending';;COALESCE(ri.company_type,'');;COALESCE(ri.registration_status,'');;'FR';;0;;ri.raw_payload;;ri.first_seen_at;;ri.last_seen_at;;!raw_payload_en;;!translation_status").
		WithArgs("raw-id").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "state", "company_type", "registration_status", "website", "country_iso2",
			"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload",
			"first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
		}).AddRow(
			"raw-id", "france", "PULSAR POINT FRANCE", "552100554", "pending", "pending", "legal_unit", "A", "", "FR",
			"", 0, "", "hash", []byte(`{"siren":"552100554"}`),
			now, now, nil, now, now,
		))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/france/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "france", body["source"])
	assert.Equal(t, "PULSAR POINT FRANCE", body["name"])
	assert.Equal(t, "552100554", body["native_id"])
	assert.Equal(t, "pending", body["status"])
	assert.Equal(t, "legal_unit", body["company_type"])
	assert.Equal(t, "A", body["registration_status"])
	assert.Equal(t, "FR", body["country_iso2"])
	assert.NotContains(t, body, "translation_status")
	assert.NotContains(t, body, "raw_payload_en")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestGetRawInput_seReadsWorkflowRawRecordDetail(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	now := time.Date(2026, 6, 5, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("se_workflow.bolagsverket_raw_records;;se_workflow.scb_raw_records;;COALESCE(ri.display_name,'');;COALESCE(ri.native_id,'');;'pending';;COALESCE(ri.company_type,'');;COALESCE(ri.registration_status,'');;COALESCE(ri.country_iso2,'');;0;;ri.raw_payload;;ri.first_seen_at;;ri.last_seen_at;;!raw_payload_en;;!translation_status").
		WithArgs("raw-id").
		WillReturnRows(pgxmock.NewRows([]string{
			"id", "source", "name", "native_id", "status", "state", "company_type", "registration_status", "website", "country_iso2",
			"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload",
			"first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
		}).AddRow(
			"raw-id", "se", "Pulsar Sverige AB", "5599990000", "pending", "pending", "Aktiebolag", "Registrerad", "", "SE",
			"se-bulk-ingest-1", 0, "", "hash", []byte(`{"identitetsbeteckning":"5599990000"}`),
			now, now, nil, now, now,
		))

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/se/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "se", body["source"])
	assert.Equal(t, "Pulsar Sverige AB", body["name"])
	assert.Equal(t, "5599990000", body["native_id"])
	assert.Equal(t, "pending", body["status"])
	assert.Equal(t, "Aktiebolag", body["company_type"])
	assert.Equal(t, "Registrerad", body["registration_status"])
	assert.Equal(t, "SE", body["country_iso2"])
	assert.Equal(t, "se-bulk-ingest-1", body["run_id"])
	assert.NotContains(t, body, "translation_status")
	assert.NotContains(t, body, "raw_payload_en")
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestGetRawInput_unsupportedSourceReturnsSafeClientError(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/unsupported/raw-id", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.NotContains(t, strings.ToLower(w.Body.String()), "stack")
	require.NotContains(t, strings.ToLower(w.Body.String()), "select")
}

func TestRawInputDetailUsesSharedSourceRegistry(t *testing.T) {
	source, err := os.ReadFile("raw_input_detail.go")
	require.NoError(t, err)

	for _, table := range []string{
		"gleif_company_raw_inputs",
		"companies_house_company_raw_inputs",
		"cvr_company_raw_inputs",
		"ariregister_company_raw_inputs",
	} {
		require.NotContains(t, string(source), table)
	}
}

func TestRawInputSourceRegistryIsStandalone(t *testing.T) {
	source, err := os.ReadFile("raw_input_list_query.go")
	require.NoError(t, err)

	require.NotContains(t, string(source), "type rawInputSource struct")
	require.NotContains(t, string(source), "var rawInputSources")
}

func TestRawInputListResponseUsesNamedType(t *testing.T) {
	source, err := os.ReadFile("raw_input_list.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, "writeJSON(w, http.StatusOK, map[string]any{")
	require.Contains(t, body, "type rawInputListResponse struct")
	require.Contains(t, body, "rawInputListResponse{")
}

func newSQLContainsMock(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	pool, err := pgxmock.NewPool(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(matchSQLContains)))
	require.NoError(t, err)
	return pool
}

func rawInputListRows() *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id",
		"source",
		"name",
		"native_id",
		"status",
		"translation_status",
		"has_suggestion",
		"state",
		"created_at",
	})
}

func matchSQLContains(expectedSQL, actualSQL string) error {
	normalized := strings.Join(strings.Fields(actualSQL), " ")
	for _, token := range strings.Split(expectedSQL, ";;") {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		if strings.HasPrefix(token, "!") {
			forbidden := strings.TrimPrefix(token, "!")
			if strings.Contains(normalized, forbidden) {
				return fmt.Errorf("actual sql contains forbidden token %q: %s", forbidden, normalized)
			}
			continue
		}
		if !strings.Contains(normalized, token) {
			return fmt.Errorf("actual sql missing token %q: %s", token, normalized)
		}
	}
	return nil
}

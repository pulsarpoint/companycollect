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
	pool.ExpectQuery("COUNT(*) FROM;;gleif_company_raw_inputs;;companies_house_company_raw_inputs;;cvr_company_raw_inputs;;ariregister_company_raw_inputs;;company_name ILIKE;;legal_name ILIKE;;!brreg_company_raw_inputs").
		WithArgs("%Registry%").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(2)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;companies_house_company_raw_inputs;;cvr_company_raw_inputs;;ariregister_company_raw_inputs;;company_name ILIKE;;legal_name ILIKE;;!brreg_company_raw_inputs").
		WithArgs("%Registry%", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("cvr-id", "cvr", "Danish Registry ApS", "12345678", "pending", "translated", false, "pending", createdAt).
			AddRow("ari-id", "ariregister", "Estonian Registry OU", "87654321", "pending", "failed", false, "pending", createdAt))

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
	require.NotNil(t, body.Items[0].TranslationStatus)
	assert.Equal(t, "translated", *body.Items[0].TranslationStatus)
	assert.Equal(t, "ariregister", body.Items[1].Source)
	assert.Equal(t, "Estonian Registry OU", body.Items[1].Name)
	assert.Equal(t, "87654321", body.Items[1].NativeID)
	require.NotNil(t, body.Items[1].TranslationStatus)
	assert.Equal(t, "failed", *body.Items[1].TranslationStatus)
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

func TestListRawInputs_translationStatusFiltersTranslatedSourcesOnly(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;cvr_company_raw_inputs;;ariregister_company_raw_inputs;;translation_status =;;!brreg_company_raw_inputs;;!companies_house_company_raw_inputs").
		WithArgs("translated").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(2)))
	pool.ExpectQuery("raw_input_page;;SELECT p.id;;p.source;;p.name;;cvr_company_raw_inputs;;ariregister_company_raw_inputs;;translation_status =;;!brreg_company_raw_inputs;;!companies_house_company_raw_inputs").
		WithArgs("translated", 50, 0).
		WillReturnRows(rawInputListRows().
			AddRow("cvr-id", "cvr", "Denmark ApS", "12345678", "pending", "translated", false, "pending", createdAt).
			AddRow("ari-id", "ariregister", "Estonia OU", "87654321", "pending", "translated", false, "pending", createdAt))

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
	require.Len(t, body.Items, 2)
	assert.Equal(t, []string{"cvr", "ariregister"}, []string{body.Items[0].Source, body.Items[1].Source})
	require.NoError(t, pool.ExpectationsWereMet())
}

func TestListRawInputs_allowsSortingByState(t *testing.T) {
	pool := newSQLContainsMock(t)
	defer pool.Close()

	createdAt := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
	pool.ExpectQuery("COUNT(*) FROM;;gleif_company_raw_inputs;;companies_house_company_raw_inputs;;cvr_company_raw_inputs;;ariregister_company_raw_inputs;;!brreg_company_raw_inputs").
		WillReturnRows(pgxmock.NewRows([]string{"count"}).AddRow(int64(1)))
	pool.ExpectQuery("raw_input_page;;ORDER BY state asc;;SELECT p.id;;p.state;;gleif_company_raw_inputs;;companies_house_company_raw_inputs;;cvr_company_raw_inputs;;ariregister_company_raw_inputs").
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

func TestGetBrregRawRecordReturnsWorkflowRawRecordDetail(t *testing.T) {
	q := &stubQuerier{}
	recordID := uuid.New()
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
	pool.ExpectQuery("companies_house_company_raw_inputs;;COALESCE(company_name,'');;COALESCE(processing_status,'');;COALESCE(company_type,'');;COALESCE(country_iso2,'')").
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

func TestGetRawInput_includesTranslatedPayloadAndMetadata(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		tableName  string
		rowName    string
		nativeID   string
		translated string
	}{
		{
			name:       "cvr",
			source:     "cvr",
			tableName:  "cvr_company_raw_inputs",
			rowName:    "Dansk Selskab ApS",
			nativeID:   "12345678",
			translated: `{"company_name":"Danish Company ApS"}`,
		},
		{
			name:       "ariregister",
			source:     "ariregister",
			tableName:  "ariregister_company_raw_inputs",
			rowName:    "Eesti OU",
			nativeID:   "87654321",
			translated: `{"legal_name":"Estonian OU"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newSQLContainsMock(t)
			defer pool.Close()

			now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
			pool.ExpectQuery(tt.tableName + ";;raw_payload_en;;translation_status;;translation_model;;translation_prompt_version;;translation_fx_rate_date").
				WithArgs("raw-id").
				WillReturnRows(pgxmock.NewRows([]string{
					"id", "source", "name", "native_id", "processing_status", "state", "company_type", "registration_status", "website", "country_iso2",
					"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload", "raw_payload_en",
					"translation_status", "translation_attempts", "translation_error", "translation_model", "translation_prompt_version",
					"translation_fx_source", "translation_fx_rate_date", "translated_at", "first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
				}).AddRow(
					"raw-id", tt.source, tt.rowName, tt.nativeID, "pending", "pending", "", "active", "https://example.com", "DK",
					"run-1", 1, "", "hash", []byte(`{"source":"raw"}`), []byte(tt.translated),
					"translated", 2, "", "qwen3:6b", "v1", "exchangerate.host", "2026-05-21", now,
					now, now, nil, now, now,
				))

			r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/"+tt.source+"/raw-id", nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tt.source, body["source"])
			assert.Equal(t, tt.rowName, body["name"])
			assert.Equal(t, tt.nativeID, body["native_id"])
			assert.Equal(t, "translated", body["translation_status"])
			assert.Equal(t, "qwen3:6b", body["translation_model"])
			assert.Equal(t, "v1", body["translation_prompt_version"])
			assert.Equal(t, "2026-05-21", body["translation_fx_rate_date"])
			assert.NotNil(t, body["raw_payload_en"])
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}

func TestGetRawInput_translatedSourcesCoalesceNullableDetailTextFields(t *testing.T) {
	tests := []struct {
		name         string
		source       string
		tableName    string
		nameColumn   string
		nativeColumn string
		typeColumn   string
	}{
		{
			name:         "cvr",
			source:       "cvr",
			tableName:    "cvr_company_raw_inputs",
			nameColumn:   "company_name",
			nativeColumn: "cvr_number",
			typeColumn:   "company_type",
		},
		{
			name:         "ariregister",
			source:       "ariregister",
			tableName:    "ariregister_company_raw_inputs",
			nameColumn:   "legal_name",
			nativeColumn: "registry_code",
			typeColumn:   "legal_form",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := newSQLContainsMock(t)
			defer pool.Close()

			now := time.Date(2026, 5, 22, 10, 0, 0, 0, time.UTC)
			pool.ExpectQuery(tt.tableName + ";;COALESCE(" + tt.nameColumn + ",'');;COALESCE(" + tt.nativeColumn + ",'');;COALESCE(processing_status,'');;COALESCE(" + tt.typeColumn + ",'');;COALESCE(registration_status,'');;raw_payload_en;;translation_status").
				WithArgs("raw-id").
				WillReturnRows(pgxmock.NewRows([]string{
					"id", "source", "name", "native_id", "processing_status", "state", "company_type", "registration_status", "website", "country_iso2",
					"run_id", "processing_attempts", "processing_error", "payload_hash", "raw_payload", "raw_payload_en",
					"translation_status", "translation_attempts", "translation_error", "translation_model", "translation_prompt_version",
					"translation_fx_source", "translation_fx_rate_date", "translated_at", "first_seen_at", "last_seen_at", "processed_at", "created_at", "updated_at",
				}).AddRow(
					"raw-id", tt.source, "", "", "pending", "pending", "", "", "", "",
					"", 1, "", "hash", []byte(`{"source":"raw"}`), []byte(`{"translated":true}`),
					"translated", 2, "", "qwen3:6b", "v1", "exchangerate.host", "2026-05-21", now,
					now, now, nil, now, now,
				))

			r := routerFor(httpapi.NewHandlers(&stubQuerier{}, nil, pool, nil, "", nil, ""))
			req := httptest.NewRequest(http.MethodGet, "/api/v1/raw-inputs/"+tt.source+"/raw-id", nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)

			require.Equal(t, http.StatusOK, w.Code)
			var body map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tt.source, body["source"])
			assert.Equal(t, "", body["name"])
			assert.Equal(t, "translated", body["translation_status"])
			assert.Equal(t, "qwen3:6b", body["translation_model"])
			assert.NotNil(t, body["raw_payload_en"])
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
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

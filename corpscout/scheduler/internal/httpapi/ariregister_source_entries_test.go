package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestListAriregisterSourceEntriesRouteExists(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(nil, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ariregister/source-entries", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"database querier not available"`)
}

func TestGetAriregisterSourceCompanyDetailRouteExists(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(nil, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/ariregister/companies/7ffd5bf3-f96e-4907-9ef3-096eb4056ab8", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"database querier not available"`)
}

func TestGetAriregisterSourceCompanyDetailReturnsSourceCompany(t *testing.T) {
	q := &stubQuerier{}
	companyID := uuid.New()
	rawRecordID := uuid.New()
	now := time.Date(2026, 6, 5, 9, 15, 0, 0, time.UTC)
	q.On("GetAriregisterSourceCompanyDetail", mock.Anything, companyID).Return(db.AriregisterSourceVCompanyDetail{
		ID:                      companyID,
		RawRecordID:             rawRecordID,
		RegistryCode:            "14035143",
		SourceNativeID:          "14035143",
		CountryIso2:             "EE",
		LegalName:               "NORDIC EXAMPLE OU",
		LegalNameNormalized:     "nordic example ou",
		RegistrationStatus:      ptrString("R"),
		RegistrationStatusLabel: ptrString("Registered"),
		LifecycleStatus:         "active",
		LegalFormCode:           ptrString("OU"),
		LegalFormLabel:          ptrString("Private limited company"),
		PayloadHash:             "payload-hash",
		ProfileVersion:          "ariregister.source_profile.v1",
		RowStatus:               "active",
		NormalizedPayload:       []byte(`{}`),
		RawCompanyPayload:       []byte(`{}`),
		Evidence:                []byte(`{}`),
		Metadata:                []byte(`{}`),
		CreatedAt:               now,
		UpdatedAt:               now,
		Names:                   []byte(`[{"name":"NORDIC EXAMPLE OU","name_type":"legal"}]`),
		Statuses:                []byte(`[{"status":"R","status_label":"Registered"}]`),
		LegalForms:              []byte(`[{"legal_form_code":"OU"}]`),
		Addresses:               []byte(`[{"city_or_area":"Tallinn"}]`),
		Contacts:                []byte(`[{"contact_type":"email","value":"info@example.ee"}]`),
		Websites:                []byte(`[]`),
		Domains:                 []byte(`[]`),
		Industries:              []byte(`[{"source_code":"62011"}]`),
		Capital:                 []byte(`[]`),
		AnnualReports:           []byte(`[]`),
		Articles:                []byte(`[]`),
		RegistryNotes:           []byte(`[]`),
	}, nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/ariregister/companies/"+companyID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body db.AriregisterSourceVCompanyDetail
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, companyID, body.ID)
	require.Equal(t, "14035143", body.RegistryCode)
	require.Equal(t, "NORDIC EXAMPLE OU", body.LegalName)
	require.JSONEq(t, `[{"city_or_area":"Tallinn"}]`, string(body.Addresses))
	q.AssertExpectations(t)
}

func TestListAriregisterSourceEntriesReturnsSourceEntries(t *testing.T) {
	q := &stubQuerier{}
	companyID := uuid.New()
	now := time.Date(2026, 6, 4, 11, 30, 0, 0, time.UTC)
	q.On("CountAriregisterSourceEntries", mock.Anything, db.CountAriregisterSourceEntriesParams{
		Query:              ptrString("TALLINN"),
		LifecycleStatus:    ptrString("active"),
		RegistrationStatus: ptrString("R"),
		TranslationStatus:  ptrString("missing"),
	}).Return(int64(42), nil)
	q.On("ListAriregisterSourceEntries", mock.Anything, db.ListAriregisterSourceEntriesParams{
		Query:              ptrString("TALLINN"),
		LifecycleStatus:    ptrString("active"),
		RegistrationStatus: ptrString("R"),
		TranslationStatus:  ptrString("missing"),
		SortBy:             "organization",
		SortDir:            "asc",
		Offset:             50,
		Limit:              50,
	}).Return([]db.AriregisterSourceMvCompanyExplorer{{
		CompanyID:               companyID,
		RegistryCode:            "14035143",
		LegalName:               "NORDIC EXAMPLE OU",
		LegalFormLabel:          ptrString("Private limited company"),
		LifecycleStatus:         "active",
		RegistrationStatus:      ptrString("R"),
		RegistrationStatusLabel: ptrString("Registered"),
		PrimaryIndustryCode:     ptrString("62011"),
		PrimaryIndustryLabel:    ptrString("Programming activities"),
		PrimaryNaceCode:         ptrString("62.011"),
		PrimaryNaceTitle:        ptrString("Computer programming activities"),
		CityOrArea:              ptrString("Tallinn"),
		PostalCode:              ptrString("10111"),
		NormalizedFullAddress:   ptrString("Harju maakond, Tallinn"),
		EmployeeCount:           ptrInt32(7),
		LatestFinancialYear:     ptrInt32(2024),
		WebsiteCount:            1,
		DomainCount:             1,
		ContactCount:            2,
		TranslationMissingCount: 3,
		UpdatedAt:               now,
	}}, nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/ariregister/source-entries?page=2&q=TALLINN&lifecycle_status=active&registration_status=R&translation_status=missing&sort=organization&dir=asc", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []db.AriregisterSourceMvCompanyExplorer `json:"items"`
		Total int64                                   `json:"total"`
		Page  int                                     `json:"page"`
		Limit int                                     `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(42), body.Total)
	require.Equal(t, 2, body.Page)
	require.Equal(t, 50, body.Limit)
	require.Len(t, body.Items, 1)
	require.Equal(t, companyID, body.Items[0].CompanyID)
	require.Equal(t, "14035143", body.Items[0].RegistryCode)
	require.Equal(t, "NORDIC EXAMPLE OU", body.Items[0].LegalName)
	require.Equal(t, "Tallinn", *body.Items[0].CityOrArea)
	require.EqualValues(t, 3, body.Items[0].TranslationMissingCount)
	q.AssertExpectations(t)
}

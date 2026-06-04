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

func TestListBrregSourceEntriesRouteExists(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(nil, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/source-entries", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"database querier not available"`)
}

func TestGetBrregSourceCompanyDetailRouteExists(t *testing.T) {
	r := routerFor(httpapi.NewHandlers(nil, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg/companies/7ffd5bf3-f96e-4907-9ef3-096eb4056ab8", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), `"error":"database querier not available"`)
}

func TestGetBrregSourceCompanyDetailReturnsSourceCompany(t *testing.T) {
	q := &stubQuerier{}
	companyID := uuid.New()
	rawRecordID := uuid.New()
	now := time.Date(2026, 6, 2, 11, 45, 0, 0, time.UTC)
	q.On("GetBrregSourceCompanyDetail", mock.Anything, companyID).Return(db.BrregSourceVCompanyDetail{
		ID:                         companyID,
		RawRecordID:                rawRecordID,
		OrganizationNumber:         "810202572",
		OrganizationName:           "BORTIGARD AS",
		OrganizationNameNormalized: "bortigard as",
		RegistrationStatus:         ptrString("active"),
		LifecycleStatus:            "active",
		PayloadHash:                "payload-hash",
		ProfileVersion:             "brreg.source_profile.v1",
		RowStatus:                  "active",
		NormalizedPayload:          []byte(`{}`),
		RawCompanyPayload:          []byte(`{}`),
		Evidence:                   []byte(`{}`),
		Metadata:                   []byte(`{}`),
		CreatedAt:                  now,
		UpdatedAt:                  now,
		Addresses:                  []byte(`[]`),
		Industries:                 []byte(`[]`),
		Websites:                   []byte(`[]`),
		Domains:                    []byte(`[]`),
		Contacts:                   []byte(`[]`),
		FinancialYears:             []byte(`[]`),
		Roles:                      []byte(`[]`),
		Shareholdings:              []byte(`[]`),
		TranslationStatus:          []byte(`{}`),
	}, nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sources/brreg/companies/"+companyID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body db.BrregSourceVCompanyDetail
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, companyID, body.ID)
	require.Equal(t, "810202572", body.OrganizationNumber)
	require.Equal(t, "BORTIGARD AS", body.OrganizationName)
	q.AssertExpectations(t)
}

func TestListBrregSourceEntriesReturnsSourceEntries(t *testing.T) {
	q := &stubQuerier{}
	companyID := uuid.New()
	now := time.Date(2026, 6, 2, 10, 30, 0, 0, time.UTC)
	q.On("CountBrregSourceEntries", mock.Anything, db.CountBrregSourceEntriesParams{
		Query:             ptrString("BORTIGARD"),
		LifecycleStatus:   ptrString("active"),
		TranslationStatus: ptrString("missing"),
		FinancialStatus:   ptrString("skipped"),
		WebsiteStatus:     ptrString("with"),
	}).Return(int64(321), nil)
	q.On("ListBrregSourceEntries", mock.Anything, db.ListBrregSourceEntriesParams{
		Query:             ptrString("BORTIGARD"),
		LifecycleStatus:   ptrString("active"),
		TranslationStatus: ptrString("missing"),
		FinancialStatus:   ptrString("skipped"),
		WebsiteStatus:     ptrString("with"),
		SortBy:            "organization",
		SortDir:           "asc",
		Offset:            50,
		Limit:             50,
	}).Return([]db.ListBrregSourceEntriesRow{{
		CompanyID:                 companyID,
		OrganizationNumber:        "810202572",
		OrganizationName:          "BORTIGARD AS",
		DescriptionEn:             ptrString("Real estate company"),
		LifecycleStatus:           "active",
		RegistrationStatus:        ptrString("active"),
		OrganizationFormCode:      ptrString("AS"),
		OrganizationFormLabel:     ptrString("Limited company"),
		PrimaryIndustryCode:       ptrString("41.000"),
		PrimaryIndustryLabel:      ptrString("Construction of buildings"),
		City:                      ptrString("HOLMESTRAND"),
		Municipality:              ptrString("HOLMESTRAND"),
		EmployeeCount:             ptrInt32(12),
		WebsiteUrl:                "https://bortigard.no",
		WebsiteHost:               ptrString("bortigard.no"),
		WebsiteCount:              1,
		DomainCount:               1,
		ContactCount:              2,
		FinancialStatus:           "skipped",
		TranslationMissingCount:   3,
		TranslationPendingCount:   2,
		TranslationRunningCount:   1,
		TranslationSucceededCount: 8,
		TranslationFailedCount:    5,
		DomainPendingCount:        4,
		DomainRunningCount:        1,
		DomainSucceededCount:      6,
		LatestFinancialYear:       ptrInt32(2024),
		LatestRevenueUsdCents:     ptrInt64(123456),
		LatestTotalAssetsUsdCents: ptrInt64(456789),
		LatestNetIncomeUsdCents:   ptrInt64(12345),
		UpdatedAt:                 now,
	}}, nil)

	r := routerFor(httpapi.NewHandlers(q, nil, nil, nil, "", nil, ""))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/brreg/source-entries?page=2&q=BORTIGARD&lifecycle_status=active&translation_status=missing&financial_status=skipped&website_status=with&sort=organization&dir=asc", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []db.ListBrregSourceEntriesRow `json:"items"`
		Total int64                          `json:"total"`
		Page  int                            `json:"page"`
		Limit int                            `json:"limit"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, int64(321), body.Total)
	require.Equal(t, 2, body.Page)
	require.Equal(t, 50, body.Limit)
	require.Len(t, body.Items, 1)
	require.Equal(t, companyID, body.Items[0].CompanyID)
	require.Equal(t, "810202572", body.Items[0].OrganizationNumber)
	require.Equal(t, "https://bortigard.no", body.Items[0].WebsiteUrl)
	require.Equal(t, "bortigard.no", *body.Items[0].WebsiteHost)
	require.Equal(t, "skipped", body.Items[0].FinancialStatus)
	require.EqualValues(t, 3, body.Items[0].TranslationMissingCount)
	require.EqualValues(t, 2, body.Items[0].TranslationPendingCount)
	require.EqualValues(t, 1, body.Items[0].TranslationRunningCount)
	require.EqualValues(t, 8, body.Items[0].TranslationSucceededCount)
	require.EqualValues(t, 5, body.Items[0].TranslationFailedCount)
	require.EqualValues(t, 4, body.Items[0].DomainPendingCount)
	require.EqualValues(t, 1, body.Items[0].DomainRunningCount)
	require.EqualValues(t, 6, body.Items[0].DomainSucceededCount)
	q.AssertExpectations(t)
}

func ptrInt32(value int32) *int32 {
	return &value
}

func ptrInt64(value int64) *int64 {
	return &value
}

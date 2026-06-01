package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestGetStats_returns_counts(t *testing.T) {
	q := &stubQuerier{}

	q.On("GetStats", mock.Anything).Return(db.GetStatsRow{
		TotalCompanies: 100,
		TotalDomains:   200,
		ActiveDomains:  150,
		PendingReview:  10,
		EnabledSources: 5,
	}, nil)

	r := routerForHandlers(q)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/stats", nil)

	r.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))

	assert.Equal(t, float64(100), body["total_companies"])
	assert.Equal(t, float64(200), body["total_domains"])
	assert.Equal(t, float64(150), body["active_domains"])
	assert.Equal(t, float64(10), body["pending_review"])
	assert.Equal(t, float64(5), body["enabled_sources"])

	q.AssertExpectations(t)
}

func TestStatsResponseUsesNamedType(t *testing.T) {
	source, err := os.ReadFile("stats.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, "writeJSON(w, http.StatusOK, map[string]any{")
	require.Contains(t, body, "type statsResponse struct")
	require.Contains(t, body, "statsResponse{")
}

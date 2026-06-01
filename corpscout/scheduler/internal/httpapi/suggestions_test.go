package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
	"github.com/pulsarpoint/corpscout/scheduler/internal/httpapi"
)

func TestHandleListCompanySuggestions_ReturnsPendingSuggestions(t *testing.T) {
	q := &stubQuerier{}
	sugID := uuid.New()
	q.On("ListCompanySuggestionReviews", mock.Anything, mock.Anything).
		Return([]db.ListCompanySuggestionReviewsRow{
			{ID: sugID, ProposedName: ptrString("Test Corp"), Status: "pending"},
		}, nil)
	q.On("CountCompanySuggestionReviews", mock.Anything, mock.Anything).
		Return(int32(1), nil)

	r := chi.NewRouter()
	httpapi.NewHandlers(q, nil, nil, nil, "", nil, "").RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/suggestions/companies", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.NewDecoder(w.Body).Decode(&resp))
	items, ok := resp["items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)
}

func TestSuggestionResponsesUseNamedTypes(t *testing.T) {
	source, err := os.ReadFile("suggestions.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, "writeJSON(w, http.StatusOK, map[string]any{")
	require.NotContains(t, body, `map[string]any{"ids": uuidStrings(ids)}`)
	require.NotContains(t, body, `map[string]any{"updated": updated, "skipped": skipped}`)
	require.Contains(t, body, "type companySuggestionListResponse struct")
	require.Contains(t, body, "type companySuggestionIDsResponse struct")
	require.Contains(t, body, "type bulkCompanySuggestionsResponse struct")
	require.True(t, strings.Contains(body, "companySuggestionListResponse{") &&
		strings.Contains(body, "companySuggestionIDsResponse{") &&
		strings.Contains(body, "bulkCompanySuggestionsResponse{"))
}

package httpapi_test

import (
	"os"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

// routerForHandlers returns a chi router with all /api/v1 routes registered.
func routerForHandlers(q *stubQuerier) *chi.Mux {
	h := newTestHandlers(q)
	r := chi.NewRouter()
	h.RegisterRoutes(r)
	return r
}

func TestCompanyEnrichmentResponsesUseNamedTypes(t *testing.T) {
	source, err := os.ReadFile("companies.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, "writeJSON(w, http.StatusOK, map[string]any{")
	require.NotContains(t, body, `map[string]any{"job_id": riverJob.Job.ID}`)
	require.Contains(t, body, "type companyEnrichmentSourcesResponse struct")
	require.Contains(t, body, "type companyEnrichmentSource struct")
	require.Contains(t, body, "type enrichCompanyFromSourceResponse struct")
	require.True(t, strings.Contains(body, "companyEnrichmentSourcesResponse{") &&
		strings.Contains(body, "enrichCompanyFromSourceResponse{"))
}

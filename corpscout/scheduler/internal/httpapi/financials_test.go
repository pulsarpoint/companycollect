package httpapi_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFinancialResponsesUseNamedTypes(t *testing.T) {
	source, err := os.ReadFile("financials.go")
	require.NoError(t, err)
	body := string(source)

	require.NotContains(t, body, "writeJSON(w, http.StatusOK, map[string]any{")
	require.NotContains(t, body, `map[string]any{"ids": uuidStrings(ids)}`)
	require.NotContains(t, body, `map[string]any{"items": items}`)
	require.Contains(t, body, "type pendingFinancialsResponse struct")
	require.Contains(t, body, "type pendingFinancialIDsResponse struct")
	require.Contains(t, body, "type companyFinancialsResponse struct")
	require.True(t, strings.Contains(body, "pendingFinancialsResponse{") &&
		strings.Contains(body, "pendingFinancialIDsResponse{") &&
		strings.Contains(body, "companyFinancialsResponse{"))
}

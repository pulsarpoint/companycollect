package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestNewPostgRESTProxyRejectsInvalidURL(t *testing.T) {
	proxy, err := newPostgRESTProxy("not-a-url")

	require.Error(t, err)
	require.Nil(t, proxy)
}

func TestRegisterRoutesReturnsUnavailableWhenPostgRESTProxyIsInvalid(t *testing.T) {
	router := chi.NewRouter()
	NewHandlers(nil, nil, nil, nil, "not-a-url", nil, "").RegisterRoutes(router)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/db/companies", nil)
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.JSONEq(t, `{"error":"database proxy not configured"}`, rec.Body.String())
}

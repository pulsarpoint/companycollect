package httpapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
)

func TestListCountries_returns_all(t *testing.T) {
	q := &stubQuerier{}

	countries := []db.Country{
		{ID: uuid.New(), IsoAlpha2: "NO", IsoAlpha3: "NOR", Name: "Norway"},
		{ID: uuid.New(), IsoAlpha2: "GB", IsoAlpha3: "GBR", Name: "United Kingdom"},
	}

	q.On("ListCountries", mock.Anything).Return(countries, nil)

	r := routerForHandlers(q)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/countries", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)

	var body []any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Len(t, body, 2)

	q.AssertExpectations(t)
}

package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pulsarpoint/currency-service/internal/httpapi"
	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/pulsarpoint/currency-service/internal/service"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

type stubProvider struct{}

func (s *stubProvider) Name() string { return "ecb" }
func (s *stubProvider) FetchRates(_ context.Context, _ time.Time) (*rates.RateSheet, error) {
	return &rates.RateSheet{
		EffectiveDate: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC),
		BaseCurrency:  "EUR",
		Rates: map[string]decimal.Decimal{
			"EUR": decimal.NewFromInt(1),
			"USD": mustD("1.09"),
			"NOK": mustD("11.50"),
		},
		FetchedAt: time.Now(),
	}, nil
}

func mustD(s string) decimal.Decimal {
	d, _ := decimal.NewFromString(s)
	return d
}

func newTestHandler() http.Handler {
	svc := service.New(service.Config{
		Providers:    []rates.Provider{&stubProvider{}},
		TodayTTL:     time.Hour,
		MaxBatchSize: 100,
	})
	return httpapi.NewHandler(svc)
}

func TestHealthz(t *testing.T) {
	h := newTestHandler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var body httpapi.HealthResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&body))
	require.Equal(t, "ok", body.Status)
}

func TestConvertEndpoint(t *testing.T) {
	h := newTestHandler()
	reqBody := httpapi.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []httpapi.ConvertItemJSON{
			{ID: "test-1", Amount: "100.00", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/convert", bytes.NewReader(b)))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp httpapi.ConvertResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "currency-service.convert.v1", resp.SchemaVersion)
	require.Equal(t, 1, resp.ItemsSeen)
	require.Equal(t, 1, resp.ItemsCompleted)
	require.Equal(t, 0, resp.ItemsFailed)
	require.Equal(t, "succeeded", resp.Results[0].Status)

	result := resp.Results[0]
	require.NotNil(t, result.Metadata)
	require.Equal(t, "ecb", result.Metadata["provider"])
	// NOK→USD is a cross-currency conversion so base_currency should be present
	require.Equal(t, "EUR", result.Metadata["base_currency"])
}

func TestConvertEndpointMixedBatch(t *testing.T) {
	h := newTestHandler()
	reqBody := httpapi.ConvertRequest{
		Provider:   "ecb",
		DatePolicy: "latest_on_or_before",
		Items: []httpapi.ConvertItemJSON{
			{ID: "ok", Amount: "100.00", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
			{ID: "bad", Amount: "bad", SourceCurrency: "NOK", TargetCurrency: "USD", Date: "2024-12-31"},
		},
	}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/convert", bytes.NewReader(b)))
	require.Equal(t, http.StatusOK, rec.Code)
	var resp httpapi.ConvertResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, 1, resp.ItemsCompleted)
	require.Equal(t, 1, resp.ItemsFailed)
}

func TestRatesEndpoint(t *testing.T) {
	h := newTestHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/rates?source=NOK&target=USD&date=2024-12-31&provider=ecb&date_policy=latest_on_or_before", nil)
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp httpapi.RatesResponse
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	require.Equal(t, "currency-service.rates.v1", resp.SchemaVersion)
	require.Equal(t, "NOK", resp.SourceCurrency)
	require.Equal(t, "USD", resp.TargetCurrency)
	require.NotEmpty(t, resp.Rate)

	require.Equal(t, "ecb:2024-12-31:latest_on_or_before", resp.Cache.Key)
	// First call is always a cache miss
	require.False(t, resp.Cache.Hit)
}

func TestConvertBatchTooLarge(t *testing.T) {
	h := newTestHandler()
	items := make([]httpapi.ConvertItemJSON, 101)
	for i := range items {
		items[i] = httpapi.ConvertItemJSON{ID: "x", Amount: "1", SourceCurrency: "USD", TargetCurrency: "USD", Date: "2024-12-31"}
	}
	reqBody := httpapi.ConvertRequest{Provider: "ecb", DatePolicy: "latest_on_or_before", Items: items}
	b, _ := json.Marshal(reqBody)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/convert", bytes.NewReader(b)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

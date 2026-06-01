package fxrates_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pulsarpoint/corpscout/scheduler/internal/fxrates"
)

const ecbXML = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01"
                 xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2024-01-15">
      <Cube currency="USD" rate="1.0900"/>
      <Cube currency="NOK" rate="11.5000"/>
      <Cube currency="GBP" rate="0.8600"/>
    </Cube>
  </Cube>
</gesmes:Envelope>`

func mockECB(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(ecbXML))
	}))
}

func TestLoad(t *testing.T) {
	srv := mockECB(t)
	defer srv.Close()

	r, err := fxrates.LoadFrom(context.Background(), srv.URL)
	require.NoError(t, err)
	assert.NotNil(t, r)
}

func TestToUSD_NOK(t *testing.T) {
	srv := mockECB(t)
	defer srv.Close()

	r, err := fxrates.LoadFrom(context.Background(), srv.URL)
	require.NoError(t, err)

	// 11500 NOK: 11500 / 11.50 EUR = 1000 EUR; 1000 * 1.09 USD = 1090 USD
	// In cents: 11500_00 NOK-cents → 109000 USD-cents
	usd, err := r.ToUSD(11500_00, "NOK")
	require.NoError(t, err)
	assert.InDelta(t, int64(109000), usd, 1)
}

func TestToUSD_USD(t *testing.T) {
	srv := mockECB(t)
	defer srv.Close()

	r, err := fxrates.LoadFrom(context.Background(), srv.URL)
	require.NoError(t, err)

	usd, err := r.ToUSD(500_00, "USD")
	require.NoError(t, err)
	assert.Equal(t, int64(500_00), usd)
}

func TestToUSD_UnknownCurrency(t *testing.T) {
	srv := mockECB(t)
	defer srv.Close()

	r, err := fxrates.LoadFrom(context.Background(), srv.URL)
	require.NoError(t, err)

	_, err = r.ToUSD(100, "XYZ")
	assert.Error(t, err)
}

func TestLoad_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	_, err := fxrates.LoadFrom(context.Background(), srv.URL)
	assert.Error(t, err)
}

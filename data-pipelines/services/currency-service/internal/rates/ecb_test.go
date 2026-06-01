package rates_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pulsarpoint/currency-service/internal/rates"
	"github.com/stretchr/testify/require"
)

const ecbFixture = `<?xml version="1.0" encoding="UTF-8"?>
<gesmes:Envelope xmlns:gesmes="http://www.gesmes.org/xml/2002-08-01" xmlns="http://www.ecb.int/vocabulary/2002-08-01/eurofxref">
  <Cube>
    <Cube time="2026-05-20">
      <Cube currency="USD" rate="1.0900"/>
      <Cube currency="NOK" rate="11.5000"/>
    </Cube>
    <Cube time="2026-05-17">
      <Cube currency="USD" rate="1.0800"/>
      <Cube currency="NOK" rate="11.4000"/>
    </Cube>
  </Cube>
</gesmes:Envelope>`

func testServer(body string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body))
	}))
}

func TestECBFetchRatesLatest(t *testing.T) {
	srv := testServer(ecbFixture)
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	sheet, err := p.FetchRates(context.Background(), time.Time{})
	require.NoError(t, err)
	require.Equal(t, "2026-05-20", sheet.EffectiveDate.Format("2006-01-02"))
	require.Equal(t, "EUR", sheet.BaseCurrency)
	usd := sheet.Rates["USD"]
	require.Equal(t, "1.09", usd.String())
}

func TestECBFetchRatesForDate(t *testing.T) {
	srv := testServer(ecbFixture)
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	date, _ := time.Parse("2006-01-02", "2026-05-19")
	sheet, err := p.FetchRates(context.Background(), date)
	require.NoError(t, err)
	// 2026-05-19 is a Monday; latest available on or before is 2026-05-17
	require.Equal(t, "2026-05-17", sheet.EffectiveDate.Format("2006-01-02"))
	nok := sheet.Rates["NOK"]
	require.Equal(t, "11.4", nok.String())
}

func TestECBFetchRatesNotFound(t *testing.T) {
	srv := testServer(ecbFixture)
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	date, _ := time.Parse("2006-01-02", "2020-01-01")
	_, err := p.FetchRates(context.Background(), date)
	require.Error(t, err)
	require.Contains(t, err.Error(), "rate_not_found")
}

func TestECBFetchRatesProviderError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	p := rates.NewECBProvider(rates.ECBConfig{DailyURL: srv.URL, HistoricalURL: srv.URL})
	_, err := p.FetchRates(context.Background(), time.Time{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "provider_unavailable")
}

func TestECBName(t *testing.T) {
	p := rates.NewECBProvider(rates.ECBConfig{})
	require.Equal(t, "ecb", p.Name())
}


package brregclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/pulsarpoint/brreg-financial-service/internal/brregclient"
	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("../parser/testdata/" + name)
	require.NoError(t, err)
	return b
}

func TestFetchKeyFigures_200(t *testing.T) {
	equinorData := fixture(t, "equinor_list.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/regnskapsregisteret/regnskap/923609016", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write(equinorData)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	raw, err := c.FetchKeyFigures(context.Background(), "923609016")
	require.NoError(t, err)
	require.Equal(t, equinorData, raw)
}

func TestFetchKeyFigures_404_ReturnsErrNotAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "974760673")
	require.ErrorIs(t, err, brregclient.ErrNotAvailable)
}

func TestFetchKeyFigures_500_UnsupportedPlan_BANK(t *testing.T) {
	body := fixture(t, "dnb_500.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(body)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "984851006")

	var planErr *brregclient.UnsupportedPlanError
	require.ErrorAs(t, err, &planErr)
	require.Equal(t, "BANK", planErr.PlanName)
}

func TestFetchKeyFigures_500_UnsupportedPlan_SKADE(t *testing.T) {
	body := fixture(t, "storebrand_500.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(body)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "930553506")

	var planErr *brregclient.UnsupportedPlanError
	require.ErrorAs(t, err, &planErr)
	require.Equal(t, "SKADE", planErr.PlanName)
}

func TestFetchKeyFigures_429_ReturnsRetryableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "000000000")

	var retryErr *brregclient.RetryableError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, 429, retryErr.StatusCode)
}

func TestFetchKeyFigures_503_ReturnsRetryableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchKeyFigures(context.Background(), "000000000")

	var retryErr *brregclient.RetryableError
	require.ErrorAs(t, err, &retryErr)
	require.Equal(t, 503, retryErr.StatusCode)
}

func TestFetchPDFYears_200(t *testing.T) {
	yearsData := fixture(t, "equinor_pdf_years.json")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/regnskapsregisteret/regnskap/aarsregnskap/kopi/923609016/aar", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		w.Write(yearsData)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	years, err := c.FetchPDFYears(context.Background(), "923609016")
	require.NoError(t, err)
	require.Len(t, years, 14)
	require.Equal(t, "2011", years[0])
}

func TestFetchPDFYears_404_ReturnsErrNotAvailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := brregclient.New(brregclient.Config{BaseURL: srv.URL})
	_, err := c.FetchPDFYears(context.Background(), "974760673")
	require.ErrorIs(t, err, brregclient.ErrNotAvailable)
}

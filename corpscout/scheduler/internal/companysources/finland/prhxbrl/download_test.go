package prhxbrl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestBuildAllFinancialStatementsURL(t *testing.T) {
	got, err := buildAllFinancialStatementsURL("https://avoindata.prh.fi/opendata-xbrl-api/v3", "2026-06-01", "2026-06-03", 2)

	require.NoError(t, err)
	require.Equal(t, "https://avoindata.prh.fi/opendata-xbrl-api/v3/all_financial_statements?page=2&registeredDateEnd=2026-06-03&registeredDateStart=2026-06-01", got)
}

func TestBuildFinancialStatementURLPreservesHostAndBasePath(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()

	got, err := buildFinancialStatementURL(server.URL+"/opendata-xbrl-api/v3", "0100130-4", "2024-12-31")

	require.NoError(t, err)
	require.Equal(t, server.URL+"/opendata-xbrl-api/v3/financial?businessId=0100130-4&financialDate=2024-12-31", got)
}

func TestDownloadDiscoveryPageDecodesFinancials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/opendata-xbrl-api/v3/all_financial_statements", r.URL.Path)
		require.Equal(t, "2026-06-01", r.URL.Query().Get("registeredDateStart"))
		require.Equal(t, "2026-06-03", r.URL.Query().Get("registeredDateEnd"))
		require.Equal(t, "1", r.URL.Query().Get("page"))
		require.Equal(t, companysources.DownloadUserAgent, r.Header.Get("User-Agent"))
		_, _ = w.Write([]byte(`{"totalResults":1,"financials":[{"businessId":"0100130-4","financialDate":"2024-12-31","registrationDate":"2025-04-15"}]}`))
	}))
	defer server.Close()

	page, err := downloadDiscoveryPage(context.Background(), server.Client(), server.URL+"/opendata-xbrl-api/v3", "2026-06-01", "2026-06-03", 1, true)

	require.NoError(t, err)
	require.Equal(t, int64(1), page.TotalResults)
	require.Len(t, page.Financials, 1)
	require.Equal(t, "0100130-4", page.Financials[0].BusinessID)
}

func TestDownloadStatementXMLWritesHashAndSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/opendata-xbrl-api/v3/financial", r.URL.Path)
		require.Equal(t, "0100130-4", r.URL.Query().Get("businessId"))
		require.Equal(t, "2024-12-31", r.URL.Query().Get("financialDate"))
		require.Equal(t, companysources.DownloadUserAgent, r.Header.Get("User-Agent"))
		w.Header().Set("Content-Type", "text/xml")
		_, _ = w.Write([]byte(`<xbrl><fact>100</fact></xbrl>`))
	}))
	defer server.Close()

	tmp := t.TempDir()
	result, err := downloadStatementXML(context.Background(), server.Client(), server.URL+"/opendata-xbrl-api/v3", "0100130-4", "2024-12-31", tmp, true)

	require.NoError(t, err)
	require.FileExists(t, result.Path)
	require.Equal(t, filepath.Join(tmp, "statements", "0100130-4", "2024-12-31.xml"), result.Path)
	require.NotEmpty(t, result.SHA256)
	require.Equal(t, int64(len(`<xbrl><fact>100</fact></xbrl>`)), result.SizeBytes)
	require.Equal(t, server.URL+"/opendata-xbrl-api/v3/financial?businessId=0100130-4&financialDate=2024-12-31", result.SourceURL)
}

func TestWriteStatementsManifest(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "statements.ndjson")
	rows := []ManifestStatement{{
		BusinessID:       "0100130-4",
		FinancialDate:    "2024-12-31",
		RegistrationDate: "2025-04-15",
		SourceURL:        "https://example.test/financial?businessId=0100130-4&financialDate=2024-12-31",
		DownloadStatus:   "succeeded",
		XMLPath:          filepath.Join(tmp, "statements", "0100130-4", "2024-12-31.xml"),
		XMLSHA256:        "abc",
		XMLSizeBytes:     12,
	}}

	result, err := writeStatementsManifest(path, rows)

	require.NoError(t, err)
	require.Equal(t, int64(1), result.RecordsWritten)
	require.Equal(t, path, result.SourceFilePath)
	require.NotEmpty(t, result.ContentSHA256)

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var decoded ManifestStatement
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &decoded))
	require.Equal(t, rows[0].BusinessID, decoded.BusinessID)
}

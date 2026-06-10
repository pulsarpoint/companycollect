package prhxbrl

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	pgxmock "github.com/pashagolub/pgxmock/v3"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	db "github.com/pulsarpoint/corpscout/scheduler/internal/db/gen"
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

func TestDownloadWritesManifestAndReturnsErrorWhenStatementXMLFails(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	sourceID := uuid.New()
	actionRunID := uuid.New()
	windowID := uuid.New()
	artifactID := uuid.New()
	now := time.Now().UTC()
	startDate := mustPgDate(t, "2026-06-01")
	endDate := mustPgDate(t, "2026-06-03")
	financialDate := mustPgDate(t, "2024-12-31")
	registrationDate := mustPgDate(t, "2026-06-02")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/opendata-xbrl-api/v3/all_financial_statements":
			require.Equal(t, "2026-06-01", r.URL.Query().Get("registeredDateStart"))
			require.Equal(t, "2026-06-03", r.URL.Query().Get("registeredDateEnd"))
			require.Equal(t, companysources.DownloadUserAgent, r.Header.Get("User-Agent"))
			_, _ = w.Write([]byte(`{"totalResults":1,"financials":[{"businessId":"0100130-4","financialDate":"2024-12-31","registrationDate":"2026-06-02"}]}`))
		case "/opendata-xbrl-api/v3/financial":
			require.Equal(t, "0100130-4", r.URL.Query().Get("businessId"))
			require.Equal(t, "2024-12-31", r.URL.Query().Get("financialDate"))
			require.Equal(t, companysources.DownloadUserAgent, r.Header.Get("User-Agent"))
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	sourceURL := server.URL + "/opendata-xbrl-api/v3"
	statementURL := sourceURL + "/financial?businessId=0100130-4&financialDate=2024-12-31"
	actionRunPGUUID := pgUUID(actionRunID)
	window := db.FinancialXbrlFinlandPrhXbrlDiscoveryWindow{
		ID:                   windowID,
		SourceID:             sourceID,
		RegisteredDateStart:  startDate,
		RegisteredDateEnd:    endDate,
		ActionRunID:          actionRunPGUUID,
		TotalResults:         1,
		PagesDiscovered:      1,
		StatementsDiscovered: 1,
		LastCompletedPage:    1,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	pendingArtifact := db.FinancialXbrlFinlandPrhXbrlStatementArtifact{
		ID:                   artifactID,
		SourceID:             sourceID,
		BusinessID:           "0100130-4",
		FinancialDate:        financialDate,
		RegistrationDate:     registrationDate,
		SourceUrl:            statementURL,
		DownloadStatus:       "pending",
		FirstDiscoveredRunID: actionRunPGUUID,
		LatestActionRunID:    actionRunPGUUID,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	downloadingArtifact := pendingArtifact
	downloadingArtifact.DownloadStatus = "downloading"
	downloadingArtifact.Attempts = 1
	downloadingArtifact.LastAttemptAt = pgtype.Timestamptz{Time: now, Valid: true}
	failedArtifact := downloadingArtifact
	failedArtifact.DownloadStatus = "failed"

	mock.ExpectQuery(`INSERT INTO financial_xbrl\.finland_prh_xbrl_discovery_windows`).
		WithArgs(sourceID, startDate, endDate, actionRunPGUUID, pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(finlandPRHXBRLDiscoveryWindowRows(window))
	mock.ExpectQuery(`INSERT INTO financial_xbrl\.finland_prh_xbrl_statement_artifacts`).
		WithArgs(sourceID, "0100130-4", financialDate, registrationDate, statementURL, actionRunPGUUID, actionRunPGUUID).
		WillReturnRows(finlandPRHXBRLStatementArtifactRows(pendingArtifact))
	mock.ExpectQuery(`UPDATE financial_xbrl\.finland_prh_xbrl_discovery_windows`).
		WithArgs(int64(1), int32(1), int64(1), int32(1), windowID).
		WillReturnRows(finlandPRHXBRLDiscoveryWindowRows(window))
	mock.ExpectQuery(`UPDATE financial_xbrl\.finland_prh_xbrl_discovery_windows`).
		WithArgs(int64(1), int32(1), int64(1), int32(1), windowID).
		WillReturnRows(finlandPRHXBRLDiscoveryWindowRows(window))
	mock.ExpectQuery(`FROM financial_xbrl\.finland_prh_xbrl_statement_artifacts`).
		WithArgs(sourceID, startDate, endDate, false, actionRunPGUUID, int32(10)).
		WillReturnRows(finlandPRHXBRLStatementArtifactRows(pendingArtifact))
	mock.ExpectQuery(`UPDATE financial_xbrl\.finland_prh_xbrl_statement_artifacts`).
		WithArgs(actionRunPGUUID, artifactID, sourceID, startDate, endDate, false).
		WillReturnRows(finlandPRHXBRLStatementArtifactRows(downloadingArtifact))
	mock.ExpectQuery(`UPDATE financial_xbrl\.finland_prh_xbrl_statement_artifacts`).
		WithArgs(actionRunPGUUID, pgxmock.AnyArg(), artifactID, sourceID).
		WillReturnRows(finlandPRHXBRLStatementArtifactRows(failedArtifact))

	runDir := t.TempDir()
	downloaded, err := Download(context.Background(), DownloadOptions{
		Queries:              db.New(mock),
		HTTPClient:           server.Client(),
		SourceID:             sourceID,
		ActionRunID:          actionRunID,
		RunDir:               runDir,
		ManifestRelativePath: "statements.ndjson",
		SourceURL:            sourceURL,
		UserAgentRequired:    true,
		RegisteredDateStart:  "2026-06-01",
		RegisteredDateEnd:    "2026-06-03",
		MaxStatements:        10,
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "1 PRH XBRL statement XML downloads failed")
	require.Equal(t, companysources.DownloadedFile{}, downloaded)
	data, readErr := os.ReadFile(filepath.Join(runDir, "statements.ndjson"))
	require.NoError(t, readErr)
	var row ManifestStatement
	require.NoError(t, json.Unmarshal(data[:len(data)-1], &row))
	require.Equal(t, "failed", row.DownloadStatus)
	require.Equal(t, "0100130-4", row.BusinessID)
	require.Contains(t, row.ErrorMessage, "status 503")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateStatementPathSegment(t *testing.T) {
	for _, value := range []string{"0100130-4", "2024-12-31"} {
		t.Run("valid "+value, func(t *testing.T) {
			require.NoError(t, validateStatementPathSegment("field", value))
		})
	}

	for _, value := range []string{"", ".", "..", "/0100130-4", `C:\0100130-4`, "0100130-4/2024", `0100130-4\2024`, "0100130-4/."} {
		t.Run("invalid "+value, func(t *testing.T) {
			require.Error(t, validateStatementPathSegment("field", value))
		})
	}
}

func TestDownloadStatementXMLRejectsUnsafePathSegmentsWithoutSideEffects(t *testing.T) {
	tests := []struct {
		name          string
		businessID    string
		financialDate string
	}{
		{
			name:          "business id parent segment",
			businessID:    "..",
			financialDate: "2024-12-31",
		},
		{
			name:          "financial date separator",
			businessID:    "0100130-4",
			financialDate: "../2024-12-31",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var requestCount atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			tmp := t.TempDir()
			_, err := downloadStatementXML(context.Background(), server.Client(), server.URL+"/opendata-xbrl-api/v3", tt.businessID, tt.financialDate, tmp, true)

			require.Error(t, err)
			require.Contains(t, err.Error(), "safe path segment")
			require.Zero(t, requestCount.Load())
			entries, readErr := os.ReadDir(tmp)
			require.NoError(t, readErr)
			require.Empty(t, entries)
		})
	}
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

func mustPgDate(t *testing.T, value string) pgtype.Date {
	t.Helper()
	date, err := parseDateToPgtype(value)
	require.NoError(t, err)
	return date
}

func finlandPRHXBRLDiscoveryWindowRows(window db.FinancialXbrlFinlandPrhXbrlDiscoveryWindow) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "source_id", "registered_date_start", "registered_date_end", "action_run_id",
		"temporal_workflow_id", "temporal_run_id", "total_results", "pages_discovered",
		"statements_discovered", "last_completed_page", "completed_at", "created_at", "updated_at",
	}).AddRow(
		window.ID,
		window.SourceID,
		window.RegisteredDateStart,
		window.RegisteredDateEnd,
		window.ActionRunID,
		window.TemporalWorkflowID,
		window.TemporalRunID,
		window.TotalResults,
		window.PagesDiscovered,
		window.StatementsDiscovered,
		window.LastCompletedPage,
		window.CompletedAt,
		window.CreatedAt,
		window.UpdatedAt,
	)
}

func finlandPRHXBRLStatementArtifactRows(artifact db.FinancialXbrlFinlandPrhXbrlStatementArtifact) *pgxmock.Rows {
	return pgxmock.NewRows([]string{
		"id", "source_id", "business_id", "financial_date", "registration_date", "source_url",
		"xml_path", "xml_sha256", "xml_size_bytes", "download_status", "attempts",
		"last_attempt_at", "downloaded_at", "last_error_message", "first_discovered_run_id",
		"latest_action_run_id", "created_at", "updated_at",
	}).AddRow(
		artifact.ID,
		artifact.SourceID,
		artifact.BusinessID,
		artifact.FinancialDate,
		artifact.RegistrationDate,
		artifact.SourceUrl,
		artifact.XmlPath,
		artifact.XmlSha256,
		artifact.XmlSizeBytes,
		artifact.DownloadStatus,
		artifact.Attempts,
		artifact.LastAttemptAt,
		artifact.DownloadedAt,
		artifact.LastErrorMessage,
		artifact.FirstDiscoveredRunID,
		artifact.LatestActionRunID,
		artifact.CreatedAt,
		artifact.UpdatedAt,
	)
}

package fx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestSyncExchangeRatesActivityRejectsMissingSourceURL(t *testing.T) {
	actions := NewActions(nil, nil, "")

	_, err := actions.SyncExchangeRatesActivity(t.Context(), SyncExchangeRatesActivityInput{
		TemporalWorkflowID: "fx-test-" + uuid.NewString(),
		Provider:           DefaultProvider,
	})

	require.ErrorContains(t, err, "exchange rate source url is required")
}

func TestSyncExchangeRatesActivityImportsNewHash(t *testing.T) {
	pool := newFXTestPool(t)
	server := newFXFixtureServer(t, validECBXML)
	actions := NewActions(pool, server.Client(), "")

	result, err := actions.SyncExchangeRatesActivity(t.Context(), SyncExchangeRatesActivityInput{
		TemporalWorkflowID: "fx-test-" + uuid.NewString(),
		Provider:           DefaultProvider,
		SourceURL:          server.URL,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.Equal(t, SyncStatusSucceeded, result.Status)
	require.NotEmpty(t, result.SyncRunID)
	require.NotEmpty(t, result.SourceFileID)
	require.NotEmpty(t, result.SheetID)
	require.Len(t, result.ContentSHA256, 64)
	require.Equal(t, "2026-06-03", result.RateDate)
	require.Equal(t, int32(4), result.CurrenciesSeen)
	require.Equal(t, int32(4), result.CurrenciesImported)

	var rate string
	err = pool.QueryRow(t.Context(), `
SELECT rate_per_base::text
FROM exchange_rates rate
JOIN exchange_rate_sheets sheet ON sheet.id = rate.sheet_id
WHERE sheet.provider = 'ecb'
  AND sheet.rate_date = '2026-06-03'
  AND rate.currency = 'NOK'
`).Scan(&rate)
	require.NoError(t, err)
	require.Equal(t, "10.707500000000", rate)
}

func TestSyncExchangeRatesActivitySkipsAlreadyProcessedHash(t *testing.T) {
	pool := newFXTestPool(t)
	server := newFXFixtureServer(t, validECBXML)
	actions := NewActions(pool, server.Client(), "")

	first, err := actions.SyncExchangeRatesActivity(t.Context(), SyncExchangeRatesActivityInput{
		TemporalWorkflowID: "fx-test-" + uuid.NewString(),
		Provider:           DefaultProvider,
		SourceURL:          server.URL,
		Trigger:            "test",
	})
	require.NoError(t, err)
	require.Equal(t, SyncStatusSucceeded, first.Status)

	second, err := actions.SyncExchangeRatesActivity(t.Context(), SyncExchangeRatesActivityInput{
		TemporalWorkflowID: "fx-test-" + uuid.NewString(),
		Provider:           DefaultProvider,
		SourceURL:          server.URL,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.Equal(t, SyncStatusSkipped, second.Status)
	require.Equal(t, first.SourceFileID, second.SourceFileID)
	require.Equal(t, first.ContentSHA256, second.ContentSHA256)
	require.Contains(t, second.Message, "already processed")
}

func TestSyncExchangeRatesActivityReturnsPreviousTerminalRunForWorkflowRetry(t *testing.T) {
	pool := newFXTestPool(t)
	server := newFXFixtureServer(t, validECBXML)
	actions := NewActions(pool, server.Client(), "")
	workflowID := "fx-test-" + uuid.NewString()

	first, err := actions.SyncExchangeRatesActivity(t.Context(), SyncExchangeRatesActivityInput{
		TemporalWorkflowID: workflowID,
		Provider:           DefaultProvider,
		SourceURL:          server.URL,
		Trigger:            "test",
	})
	require.NoError(t, err)
	require.Equal(t, SyncStatusSucceeded, first.Status)

	second, err := actions.SyncExchangeRatesActivity(t.Context(), SyncExchangeRatesActivityInput{
		TemporalWorkflowID: workflowID,
		Provider:           DefaultProvider,
		SourceURL:          server.URL,
		Trigger:            "test",
	})

	require.NoError(t, err)
	require.Equal(t, SyncStatusSucceeded, second.Status)
	require.Equal(t, first.SyncRunID, second.SyncRunID)
	require.Equal(t, first.SheetID, second.SheetID)
	require.Equal(t, int32(4), second.CurrenciesImported)
}

func newFXTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("CORPSCOUT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("CORPSCOUT_TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	var sourceFilesRegclass *string
	err = pool.QueryRow(context.Background(), "SELECT to_regclass('exchange_rate_source_files')::text").Scan(&sourceFilesRegclass)
	require.NoError(t, err)
	if sourceFilesRegclass == nil {
		t.Skip("CORPSCOUT_TEST_DATABASE_URL must point to a migrated database with exchange_rate_source_files")
	}
	return pool
}

func newFXFixtureServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("ETag", "fx-test-etag")
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

package actions

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	brregbulk "github.com/pulsarpoint/corpscout/scheduler/internal/brreg/bulk"
)

func TestDownloadBrregBulkPayloadStagesCompletePayload(t *testing.T) {
	body := gzipBytes(t, []byte(`[
		{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS"}
	]`))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		_, err := w.Write(body)
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	staged, err := downloadBrregBulkPayload(context.Background(), http.DefaultClient, server.URL)
	require.NoError(t, err)
	defer staged.Close()

	var records []brregbulk.Record
	result, err := brregbulk.StreamRecords(context.Background(), staged.Reader, 0, func(record brregbulk.Record) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.EqualValues(t, 1, result.RowsSeen)
	require.Len(t, records, 1)
	require.Equal(t, "810202572", records[0].OrganizationNumber)
	require.Equal(t, int64(len(body)), staged.BytesDownloaded)
}

func TestDownloadBrregBulkPayloadRejectsIncompleteContentLength(t *testing.T) {
	body := gzipBytes(t, []byte(`[
		{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS"}
	]`))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		w.Header().Set("Content-Length", "999999")
		_, err := w.Write(body)
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	staged, err := downloadBrregBulkPayload(context.Background(), http.DefaultClient, server.URL)

	require.Error(t, err)
	require.Contains(t, err.Error(), "download brreg bulk payload")
	require.Nil(t, staged)
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	_, err := writer.Write(data)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return buf.Bytes()
}

package companysources

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadDirectFileWritesSourceFileAndHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Corpscout Company Source Downloader", r.Header.Get("User-Agent"))
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := DownloadDirectFile(context.Background(), http.DefaultClient, DirectFileDownload{
		URL:               server.URL,
		RunDir:            runDir,
		RelativePath:      "raw/source.json",
		UserAgentRequired: true,
	})

	require.NoError(t, err)
	require.Equal(t, filepath.Join(runDir, "raw/source.json"), result.SourceFilePath)
	require.Equal(t, int64(len(`{"ok":true}`)), result.ContentLengthBytes)
	require.Len(t, result.ContentSHA256, 64)
	body, err := os.ReadFile(result.SourceFilePath)
	require.NoError(t, err)
	require.JSONEq(t, `{"ok":true}`, string(body))
}

func TestDownloadDirectFileRejectsUnsafeRelativePath(t *testing.T) {
	runDir := t.TempDir()
	for _, relativePath := range []string{"../source.json", filepath.Join(t.TempDir(), "source.json")} {
		t.Run(relativePath, func(t *testing.T) {
			_, err := DownloadDirectFile(context.Background(), http.DefaultClient, DirectFileDownload{
				URL:          "https://example.test/source.json",
				RunDir:       runDir,
				RelativePath: relativePath,
			})

			require.Error(t, err)
			require.Contains(t, err.Error(), "outside run dir")
		})
	}
}

func TestWriteJSONArrayAsNDJSONPreservesEachObject(t *testing.T) {
	runDir := t.TempDir()
	path := filepath.Join(runDir, "source.ndjson")
	records := []json.RawMessage{json.RawMessage(`{"a":1}`), json.RawMessage(`{"b":2}`)}

	written, err := WriteRawMessagesAsNDJSON(path, records)

	require.NoError(t, err)
	require.Equal(t, int64(2), written.RecordsWritten)
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"a\":1}\n{\"b\":2}\n", strings.ReplaceAll(string(body), " ", ""))
}

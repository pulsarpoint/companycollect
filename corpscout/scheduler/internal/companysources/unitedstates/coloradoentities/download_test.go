package coloradoentities

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesSocrataArrayAsNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"entityid":"1","entityname":"One"},{"entityid":"2","entityname":"Two"}]`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKey:      "source",
		FileKind:     "source_snapshot",
		RunDir:       runDir,
		SourceURL:    server.URL,
		RelativePath: "source.ndjson",
	})

	require.NoError(t, err)
	require.Equal(t, "source", result.FileKey)
	require.Equal(t, "source_snapshot", result.Kind)
	require.Equal(t, int64(2), result.RecordsWritten)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(body), "\n"))
}

func TestDownloadWritesAllSocrataPages(t *testing.T) {
	var offsets []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "2", r.URL.Query().Get("$limit"))
		require.Equal(t, "entityid", r.URL.Query().Get("$order"))
		offset := r.URL.Query().Get("$offset")
		offsets = append(offsets, offset)
		switch offset {
		case "0":
			_, _ = w.Write([]byte(`[{"entityid":"1","entityname":"One"},{"entityid":"2","entityname":"Two"}]`))
		case "2":
			_, _ = w.Write([]byte(`[{"entityid":"3","entityname":"Three"}]`))
		default:
			http.Error(w, "unexpected offset", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKind:     "source_snapshot",
		RunDir:       runDir,
		SourceURL:    server.URL,
		RelativePath: "source.ndjson",
		Config:       map[string]any{"page_limit": 2},
	})

	require.NoError(t, err)
	require.Equal(t, int64(3), result.RecordsWritten)
	require.Equal(t, []string{"0", "2"}, offsets)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Contains(t, string(body), `"entityid":"3"`)
	require.Equal(t, 3, strings.Count(string(body), "\n"))
}

func TestDownloadRejectsUnsafeNDJSONRelativePath(t *testing.T) {
	runDir := t.TempDir()
	outsidePath := filepath.Join(filepath.Dir(runDir), "outside.ndjson")

	_, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKind:     "source_snapshot",
		RunDir:       runDir,
		RelativePath: "../outside.ndjson",
		SourceURL:    "https://example.test/source.json",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "outside run dir")
	require.NoFileExists(t, outsidePath)
}

func TestDownloadRejectsUnsupportedFileKind(t *testing.T) {
	_, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKind:     "code_list",
		RunDir:       t.TempDir(),
		RelativePath: "source.ndjson",
		SourceURL:    "https://example.test/source.json",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported Colorado entities file kind")
}

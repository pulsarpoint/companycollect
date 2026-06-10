package prhytj

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

func TestDownloadWritesPRHCompaniesAsNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "1", r.URL.Query().Get("page"))
		_, _ = w.Write([]byte(`{"totalResults":2,"companies":[{"businessId":{"type":"businessId","value":"1234567-8"}},{"businessId":{"type":"businessId","value":"2345678-9"}}]}`))
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
	require.Contains(t, string(body), `"1234567-8"`)
	require.Equal(t, 2, strings.Count(string(body), "\n"))
}

func TestDownloadWritesAllPRHCompanyPages(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		pages = append(pages, page)
		switch page {
		case "1":
			_, _ = w.Write([]byte(`{"totalResults":3,"companies":[{"businessId":{"type":"businessId","value":"1234567-8"}},{"businessId":{"type":"businessId","value":"2345678-9"}}]}`))
		case "2":
			_, _ = w.Write([]byte(`{"totalResults":3,"companies":[{"businessId":{"type":"businessId","value":"3456789-0"}}]}`))
		default:
			http.Error(w, "unexpected page", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKind:     "source_snapshot",
		RunDir:       runDir,
		SourceURL:    server.URL,
		RelativePath: "source.ndjson",
	})

	require.NoError(t, err)
	require.Equal(t, int64(3), result.RecordsWritten)
	require.Equal(t, []string{"1", "2"}, pages)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Contains(t, string(body), `"3456789-0"`)
	require.Equal(t, 3, strings.Count(string(body), "\n"))
}

func TestDownloadFileRoutesPRHCodeListDescription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/opendata-ytj-api/v3/description", r.URL.Path)
		require.Equal(t, "REK", r.URL.Query().Get("code"))
		require.Equal(t, "en", r.URL.Query().Get("lang"))
		_, _ = w.Write([]byte("code\tvalue\n1\tRegistered\n"))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKey:      "codelist_REK_en",
		FileKind:     "code_list",
		RunDir:       runDir,
		RelativePath: "codelists/REK.en.tsv",
		SourceURL:    server.URL + "/ignored?old=true",
		Config:       map[string]any{"code": "REK"},
	})

	require.NoError(t, err)
	require.Equal(t, "codelist_REK_en", result.FileKey)
	require.Equal(t, "code_list", result.Kind)
	require.Equal(t, filepath.Join(runDir, "codelists/REK.en.tsv"), result.Path)
	body, err := os.ReadFile(result.Path)
	require.NoError(t, err)
	require.Equal(t, "code\tvalue\n1\tRegistered\n", string(body))
}

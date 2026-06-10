package irseobmf

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesIRSCSVFilesAsNDJSON(t *testing.T) {
	header := strings.Join(csvColumns, ",")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/eo1.csv", r.URL.Path)
		_, _ = w.Write([]byte(header + "\n010011694,ORG ONE,,1 A ST,BOSTON,MA,02101,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,1,2,3,S19,\n200011695,ORG TWO,,2 B ST,DENVER,CO,80201,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,4,5,6,B20,\n"))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKey:      "source",
		FileKind:     "source_snapshot",
		RunDir:       runDir,
		SourceURL:    server.URL,
		RelativePath: "source.ndjson",
		Config:       map[string]any{"download_urls": []string{server.URL + "/eo1.csv"}},
	})

	require.NoError(t, err)
	require.Equal(t, "source", result.FileKey)
	require.Equal(t, "source_snapshot", result.Kind)
	require.Equal(t, int64(2), result.RecordsWritten)
	body, err := os.ReadFile(filepath.Join(runDir, "source.ndjson"))
	require.NoError(t, err)
	require.Equal(t, 2, strings.Count(string(body), "\n"))

	var first IrsEoBmfRecord
	require.NoError(t, json.Unmarshal([]byte(strings.Split(strings.TrimSpace(string(body)), "\n")[0]), &first))
	require.Equal(t, "010011694", first.EIN)
	require.Equal(t, "ORG ONE", first.Name)
	require.Equal(t, "S19", first.NTEECD)
}

func TestDownloadAggregatesIRSRegionalCSVFiles(t *testing.T) {
	header := strings.Join(csvColumns, ",")
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
		switch r.URL.Path {
		case "/base/eo1.csv":
			_, _ = w.Write([]byte(header + "\n010011694,ORG ONE,,1 A ST,BOSTON,MA,02101,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,1,2,3,S19,\n"))
		case "/base/eo2.csv":
			_, _ = w.Write([]byte(header + "\n200011695,ORG TWO,,2 B ST,DENVER,CO,80201,0000,03,3,2000,202109,1,16,0,5,01,202509,3,3,01,0,09,4,5,6,B20,\n"))
		case "/base/eo3.csv", "/base/eo4.csv":
			_, _ = w.Write([]byte(header + "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKind:     "source_snapshot",
		RunDir:       runDir,
		SourceURL:    server.URL + "/base/",
		RelativePath: "source.ndjson",
	})

	require.NoError(t, err)
	require.Equal(t, int64(2), result.RecordsWritten)
	require.Equal(t, []string{"/base/eo1.csv", "/base/eo2.csv", "/base/eo3.csv", "/base/eo4.csv"}, requested)
}

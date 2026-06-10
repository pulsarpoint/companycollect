package irseobmf

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

func TestDownloadWritesIRSRecordsAsNDJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"ein":"1","name":"One"},{"ein":"2","name":"Two"}]`))
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

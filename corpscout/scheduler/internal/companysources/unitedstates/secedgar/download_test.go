package secedgar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesSECJSONFileWithUserAgent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, companysources.DownloadUserAgent, r.Header.Get("User-Agent"))
		_, _ = w.Write([]byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}}`))
	}))
	defer server.Close()

	runDir := t.TempDir()
	result, err := Source{}.DownloadFile(context.Background(), companysources.DownloadFileOptions{
		FileKey:           "source",
		FileKind:          "source_snapshot",
		RunDir:            runDir,
		SourceURL:         server.URL,
		RelativePath:      "source.json",
		UserAgentRequired: true,
	})

	require.NoError(t, err)
	require.Equal(t, "source", result.FileKey)
	require.Equal(t, "source_snapshot", result.Kind)
	require.Equal(t, filepath.Join(runDir, "source.json"), result.Path)
	body, err := os.ReadFile(result.Path)
	require.NoError(t, err)
	require.Contains(t, string(body), `"AAPL"`)
}

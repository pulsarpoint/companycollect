package prhytj

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	sourcespec "github.com/pulsarpoint/companycollect/companies/companysource/internal/source"
	"github.com/stretchr/testify/require"
)

func TestDownloadWritesFlatSourceFile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "1", r.URL.Query().Get("page"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"totalResults": 1,
			"companies": []map[string]any{{
				"businessId": "1234567-8",
				"name":       "Example Oy",
			}},
		})
	}))
	defer server.Close()

	runDir := t.TempDir()
	source := NewSource(Config{BaseURL: server.URL, HTTPClient: server.Client()})
	result, err := source.Download(context.Background(), sourcespec.DownloadOptions{
		RunDir:   runDir,
		MaxPages: 1,
	})
	require.NoError(t, err)
	require.Equal(t, runDir, result.RunDir)
	require.Equal(t, filepath.Join(runDir, "source.ndjson"), result.SourcePath)
	require.EqualValues(t, 1, result.RecordsSeen)

	body, err := os.ReadFile(result.SourcePath)
	require.NoError(t, err)
	require.True(t, strings.Contains(string(body), `"businessId":"1234567-8"`))
}

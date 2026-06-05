package actions

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDownloadPayloadWithProgressWritesTargetPathAndKeepsFile(t *testing.T) {
	payload := []byte("sirene parquet bytes")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-parquet")
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	targetPath := filepath.Join(t.TempDir(), "france-sirene", "workflow-id", "stock_unite_legale.parquet")
	var progress []int64

	staged, err := downloadPayloadWithProgress(context.Background(), server.Client(), server.URL, targetPath, func(bytesDownloaded int64) {
		progress = append(progress, bytesDownloaded)
	})

	require.NoError(t, err)
	require.Equal(t, targetPath, staged.Path)
	require.EqualValues(t, len(payload), staged.BytesDownloaded)
	require.Equal(t, "application/x-parquet", staged.ContentType)
	require.Equal(t, server.URL, staged.ResolvedURL)
	sum := sha256.Sum256(payload)
	require.Equal(t, hex.EncodeToString(sum[:]), staged.PayloadHash)
	require.NotEmpty(t, progress)

	got, err := os.ReadFile(targetPath)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

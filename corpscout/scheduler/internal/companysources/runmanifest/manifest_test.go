package runmanifest

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteReadAndHashManifest(t *testing.T) {
	runDir := t.TempDir()
	downloadedAt := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	manifest := Manifest{
		Country:      "finland",
		Source:       "prhytj",
		RunID:        "20260609T120000Z-prhytj",
		DownloadedAt: downloadedAt,
		Files: []File{
			{Path: "source.ndjson", Kind: "ndjson", Rows: 2, SHA256: "abc123"},
		},
	}

	require.NoError(t, Write(runDir, manifest))

	loaded, err := Read(runDir)
	require.NoError(t, err)
	require.Equal(t, manifest, loaded)

	hash1, err := Hash(runDir)
	require.NoError(t, err)
	require.Len(t, hash1, 64)

	require.NoError(t, os.WriteFile(filepath.Join(runDir, FileName), []byte(`{"country":"finland"}`), 0o644))
	hash2, err := Hash(runDir)
	require.NoError(t, err)
	require.NotEqual(t, hash1, hash2)
}

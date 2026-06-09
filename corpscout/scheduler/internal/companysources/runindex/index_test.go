package runindex

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShouldImportWhenRunIsMissingOrChanged(t *testing.T) {
	index := Index{}
	require.True(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-a"}))

	index.MarkImported(Entry{
		Country:        "finland",
		Source:         "prhytj",
		RunID:          "run-1",
		ManifestHash:   "manifest-a",
		RawFileHashes:  []string{"file-a"},
		SourceExportID: "export-1",
		ImportedAt:     time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		Status:         "imported",
	})

	require.False(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-a"}))
	require.False(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-a"}))
	require.True(t, index.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-b"}))
	require.True(t, index.ShouldImport("finland", "prhytj", "run-2", "manifest-b", []string{"file-a"}))
}

func TestSaveAndLoadIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run-index.lock.yaml")
	index := Index{}
	index.MarkImported(Entry{
		Country:       "finland",
		Source:        "prhytj",
		RunID:         "run-1",
		ManifestHash:  "manifest-a",
		RawFileHashes: []string{"file-a", "file-b"},
		Status:        StatusImported,
	})

	require.NoError(t, Save(path, index))
	loaded, err := Load(path)
	require.NoError(t, err)
	require.False(t, loaded.ShouldImport("finland", "prhytj", "run-1", "manifest-a", []string{"file-b", "file-a"}))
}

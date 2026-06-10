package companysources

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runindex"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runmanifest"
	"github.com/stretchr/testify/require"
)

type importingSource struct {
	key Key
}

func (s importingSource) Key() Key { return s.key }
func (s importingSource) DisplayName() string {
	return "Importing Source"
}
func (s importingSource) DownloadFile(context.Context, DownloadFileOptions) (DownloadedFile, error) {
	return DownloadedFile{}, nil
}
func (s importingSource) Import(ctx context.Context, opts ImportOptions) (ImportResult, error) {
	return ImportResult{RunDir: opts.RunDir, ImportedTables: []string{"table"}, ImportedRows: 12}, nil
}

func TestImportRunCallsRegisteredSource(t *testing.T) {
	registry := NewRegistry(importingSource{key: Key{Country: "finland", Source: "prhytj"}})

	result, err := ImportRun(context.Background(), registry, ImportRunRequest{
		Country:             "finland",
		Source:              "prhytj",
		RunDir:              "/tmp/run",
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
	})

	require.NoError(t, err)
	require.Equal(t, "/tmp/run", result.RunDir)
	require.Equal(t, int64(12), result.ImportedRows)
}

func TestImportChangedRunsSkipsUnchangedRun(t *testing.T) {
	root := t.TempDir()
	runDir := filepath.Join(root, "finland", "prhytj", "runs", "20260609T120000Z-prhytj")
	manifest := runmanifest.Manifest{
		Country: "finland",
		Source:  "prhytj",
		RunID:   "20260609T120000Z-prhytj",
		Files:   []runmanifest.File{{Path: "source.ndjson", Kind: "ndjson", Rows: 1, SHA256: "file-a"}},
	}
	require.NoError(t, runmanifest.Write(runDir, manifest))
	manifestHash, err := runmanifest.Hash(runDir)
	require.NoError(t, err)

	indexPath := filepath.Join(root, "run-index.lock.yaml")
	index := runindex.Index{}
	index.MarkImported(runindex.Entry{
		Country:       "finland",
		Source:        "prhytj",
		RunID:         manifest.RunID,
		ManifestHash:  manifestHash,
		RawFileHashes: []string{"file-a"},
		ImportedAt:    time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC),
		Status:        runindex.StatusImported,
	})
	require.NoError(t, runindex.Save(indexPath, index))

	registry := NewRegistry(importingSource{key: Key{Country: "finland", Source: "prhytj"}})
	result, err := ImportChangedRuns(context.Background(), registry, ImportChangedRunsRequest{
		RunsRoot:            root,
		RunIndexPath:        indexPath,
		ClickHouseNativeURL: "clickhouse://companycollect:9002?username=default&database=corpscout_sources",
		ChangedOnly:         true,
	})

	require.NoError(t, err)
	require.Equal(t, []ImportChangedSourceResult{{Source: "finland/prhytj", RunID: manifest.RunID, Status: "skipped"}}, result.Sources)
}

package prhytj

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
	"github.com/stretchr/testify/require"
)

func TestParseCodeListFilePreservesCodesAndDescriptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "KIELI.en.tsv")
	require.NoError(t, os.WriteFile(path, []byte("1\tFinnish\n2\tSwedish\n3\tEnglish\n"), 0o644))

	run := codeListRun{
		SourceRunID:    "file-run-1",
		IngestedAt:     time.Date(2026, 6, 10, 12, 0, 0, 0, time.UTC),
		SourceExportID: uuid.New(),
	}
	meta := codeListMetadata{
		FileRunID:    "file-run-1",
		FileKey:      "codelist_KIELI_en",
		CodeList:     "KIELI",
		LanguageCode: "en",
	}
	var rows []CodeListRow
	err := parseCodeListFile(context.Background(), path, run, meta, func(row CodeListRow) error {
		rows = append(rows, row)
		return nil
	})

	require.NoError(t, err)
	require.Len(t, rows, 3)
	require.Equal(t, "1", rows[0].Code)
	require.Equal(t, "Finnish", rows[0].Description)
	require.Equal(t, "KIELI", rows[2].CodeList)
	require.Equal(t, "en", rows[2].LanguageCode)
	require.Equal(t, int64(3), rows[2].SourceLineNumber)
	require.NotEmpty(t, rows[2].SourcePayloadHash)
}

func TestCodeListFileMetadataFallsBackToFileKey(t *testing.T) {
	meta := codeListFileMetadata(companysources.SelectedSourceFile{
		FileRunID: "file-run-1",
		FileKey:   "codelist_REK_KDI_en",
		Kind:      "code_list",
	})

	require.Equal(t, "REK_KDI", meta.CodeList)
	require.Equal(t, "en", meta.LanguageCode)
}

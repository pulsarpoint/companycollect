package companysources

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type downloadingSource struct {
	got DownloadFileOptions
}

func (s *downloadingSource) Key() Key {
	return Key{Country: "finland", Source: "prhytj"}
}
func (s downloadingSource) DisplayName() string {
	return "Finland PRH YTJ"
}
func (s *downloadingSource) DownloadFile(ctx context.Context, opts DownloadFileOptions) (DownloadedFile, error) {
	s.got = opts
	return DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               filepath.Join(opts.RunDir, opts.RelativePath),
		RelativePath:       opts.RelativePath,
		ContentSHA256:      "abc123",
		ContentLengthBytes: 10,
		RecordsWritten:     2,
	}, nil
}
func (s downloadingSource) Import(ctx context.Context, opts ImportOptions) (ImportResult, error) {
	return ImportResult{RunDir: opts.RunDir, ImportedRows: 0}, nil
}

func TestDownloadFilePassesFileDefinitionToSource(t *testing.T) {
	source := &downloadingSource{}
	registry := NewRegistry(source)

	result, err := DownloadFile(context.Background(), registry, DownloadFileRequest{
		Country:           "finland",
		Source:            "prhytj",
		FileKey:           "codelist_REK_en",
		FileKind:          "code_list",
		RunDir:            t.TempDir(),
		RelativePath:      "codelists/REK.en.tsv",
		SourceURL:         "https://avoindata.prh.fi/opendata-ytj-api/v3/description?code=REK&lang=en",
		UserAgentRequired: false,
		Config:            map[string]any{"code": "REK", "lang": "en"},
	})

	require.NoError(t, err)
	require.Equal(t, "codelist_REK_en", result.FileKey)
	require.Equal(t, "code_list", result.Kind)
	require.Equal(t, "codelists/REK.en.tsv", source.got.RelativePath)
}

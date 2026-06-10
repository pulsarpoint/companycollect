package irseobmf

import (
	"context"
	"net/http"
	"path/filepath"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.ndjson"
	}

	records, err := companysources.DownloadJSONArray(ctx, http.DefaultClient, opts.SourceURL, opts.UserAgentRequired)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	path := filepath.Join(opts.RunDir, relativePath)
	written, err := companysources.WriteRawMessagesAsNDJSON(path, records)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
	return companysources.DownloadedFile{
		FileKey:            opts.FileKey,
		Kind:               opts.FileKind,
		RunDir:             opts.RunDir,
		Path:               written.SourceFilePath,
		RelativePath:       relativePath,
		ContentSHA256:      written.ContentSHA256,
		ContentLengthBytes: written.ContentLengthBytes,
		RecordsWritten:     written.RecordsWritten,
	}, nil
}

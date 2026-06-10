package secedgar

import (
	"context"
	"net/http"

	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.json"
	}
	written, err := companysources.DownloadDirectFile(ctx, http.DefaultClient, companysources.DirectFileDownload{
		URL:               opts.SourceURL,
		RunDir:            opts.RunDir,
		RelativePath:      relativePath,
		UserAgentRequired: opts.UserAgentRequired,
	})
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

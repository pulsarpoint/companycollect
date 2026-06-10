package irseobmf

import (
	"context"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources"
)

func (Source) DownloadFile(ctx context.Context, opts companysources.DownloadFileOptions) (companysources.DownloadedFile, error) {
	if opts.FileKind != "" && opts.FileKind != "source_snapshot" {
		return companysources.DownloadedFile{}, errors.Errorf("unsupported IRS EO BMF file kind %q", opts.FileKind)
	}
	relativePath := opts.RelativePath
	if relativePath == "" {
		relativePath = "source.ndjson"
	}
	path, err := companysources.SafeRunRelativePath(opts.RunDir, relativePath)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}

	records, err := companysources.DownloadJSONArray(ctx, http.DefaultClient, opts.SourceURL, opts.UserAgentRequired)
	if err != nil {
		return companysources.DownloadedFile{}, err
	}
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

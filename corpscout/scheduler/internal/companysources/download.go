package companysources

import "context"

func DownloadFile(ctx context.Context, registry Registry, req DownloadFileRequest) (DownloadedFile, error) {
	source, err := registry.Get(req.Country, req.Source)
	if err != nil {
		return DownloadedFile{}, err
	}
	return source.DownloadFile(ctx, DownloadFileOptions{
		FileKey:           req.FileKey,
		FileKind:          req.FileKind,
		RunDir:            req.RunDir,
		RelativePath:      req.RelativePath,
		SourceURL:         req.SourceURL,
		UserAgentRequired: req.UserAgentRequired,
		Config:            req.Config,
	})
}

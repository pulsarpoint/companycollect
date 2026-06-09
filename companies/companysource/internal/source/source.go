package source

import "context"

type Key struct {
	Country string `json:"country"`
	Source  string `json:"source"`
}

type DownloadOptions struct {
	RunDir   string
	RunID    string
	MaxPages int
}

type ExportParquetOptions struct {
	RunDir string
	Limit  int64
}

type Adapter interface {
	Key() Key
	DisplayName() string
	Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
	ExportParquet(ctx context.Context, opts ExportParquetOptions) (ExportParquetResult, error)
	Status(ctx context.Context, runDir string) (StatusResult, error)
}

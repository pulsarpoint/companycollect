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

type ClickHouseMigrationOptions struct {
	RunDir   string
	Database string
	Out      string
	DownOut  string
}

type ClickHouseImportOptions struct {
	RunDir              string
	Database            string
	ClickHouseNativeURL string
	SourceExportID      string
	ClickHouseImage     string
	DockerMount         string
}

type Adapter interface {
	Key() Key
	DisplayName() string
	Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
	ExportParquet(ctx context.Context, opts ExportParquetOptions) (ExportParquetResult, error)
	GenerateClickHouseMigration(ctx context.Context, opts ClickHouseMigrationOptions) (ClickHouseMigrationResult, error)
	ImportClickHouse(ctx context.Context, opts ClickHouseImportOptions) (ClickHouseImportResult, error)
	Status(ctx context.Context, runDir string) (StatusResult, error)
}

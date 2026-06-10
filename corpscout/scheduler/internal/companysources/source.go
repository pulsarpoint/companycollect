package companysources

import "context"

type Key struct {
	Country string
	Source  string
}

type Source interface {
	Key() Key
	DisplayName() string
	DownloadFile(ctx context.Context, opts DownloadFileOptions) (DownloadedFile, error)
	Import(ctx context.Context, opts ImportOptions) (ImportResult, error)
}

type DownloadFileOptions struct {
	FileKey           string
	FileKind          string
	RunDir            string
	RelativePath      string
	SourceURL         string
	UserAgentRequired bool
	Config            map[string]any
}

type DownloadedFile struct {
	FileKey            string `json:"file_key"`
	Kind               string `json:"kind"`
	RunDir             string `json:"run_dir"`
	Path               string `json:"path"`
	RelativePath       string `json:"relative_path"`
	ContentSHA256      string `json:"content_sha256"`
	ContentLengthBytes int64  `json:"content_length_bytes"`
	RecordsWritten     int64  `json:"records_written"`
}

type DownloadFileRequest struct {
	Country           string
	Source            string
	FileKey           string
	FileKind          string
	RunDir            string
	RelativePath      string
	SourceURL         string
	UserAgentRequired bool
	Config            map[string]any
}

type ImportOptions struct {
	RunDir              string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	Truncate            bool
}

type ImportResult struct {
	RunDir         string
	ImportedTables []string
	ImportedRows   int64
}

type ImportRunRequest struct {
	Country             string
	Source              string
	RunDir              string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	Truncate            bool
}

type ImportChangedRunsRequest struct {
	RunsRoot            string
	RunIndexPath        string
	ClickHouseNativeURL string
	BatchSize           int
	Limit               int64
	ChangedOnly         bool
	Truncate            bool
}

type ImportChangedRunsResult struct {
	Sources []ImportChangedSourceResult
}

type ImportChangedSourceResult struct {
	Source       string
	RunID        string
	Status       string
	ImportedRows int64
}

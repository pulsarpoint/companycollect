package source

type DownloadResult struct {
	RunDir      string `json:"run_dir"`
	SourcePath  string `json:"source_path"`
	RecordsSeen int64  `json:"records_seen"`
}

type ExportParquetResult struct {
	RunDir          string   `json:"run_dir"`
	ManifestPath    string   `json:"manifest_path"`
	ParquetFiles    []string `json:"parquet_files"`
	RecordsSeen     int64    `json:"records_seen"`
	RecordsExported int64    `json:"records_exported"`
	DecodeErrors    int64    `json:"decode_errors"`
}

type StatusResult struct {
	Status       string `json:"status"`
	RunDir       string `json:"run_dir"`
	ManifestPath string `json:"manifest_path,omitempty"`
}

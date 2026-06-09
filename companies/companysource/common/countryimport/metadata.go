package countryimport

import "time"

type DownloadMetadata struct {
	SourceSlug           string         `json:"source_slug"`
	SourceName           string         `json:"source_name,omitempty"`
	BaseURL              string         `json:"base_url,omitempty"`
	SnapshotPath         string         `json:"snapshot_path"`
	StartedAt            time.Time      `json:"started_at"`
	FinishedAt           time.Time      `json:"finished_at"`
	DurationMS           int64          `json:"duration_ms"`
	BytesDownloaded      int64          `json:"bytes_downloaded"`
	RecordsSeen          int64          `json:"records_seen"`
	PagesDownloaded      int            `json:"pages_downloaded"`
	FirstPage            int            `json:"first_page,omitempty"`
	LastPage             int            `json:"last_page,omitempty"`
	TotalResultsReported *int64         `json:"total_results_reported,omitempty"`
	SHA256               string         `json:"sha256"`
	HTTPStatuses         map[string]int `json:"http_statuses,omitempty"`
	License              string         `json:"license,omitempty"`
	Attribution          string         `json:"attribution,omitempty"`
}

type ProcessMetadata struct {
	SourceSlug       string    `json:"source_slug"`
	SnapshotPath     string    `json:"snapshot_path"`
	StartedAt        time.Time `json:"started_at"`
	FinishedAt       time.Time `json:"finished_at"`
	DurationMS       int64     `json:"duration_ms"`
	RecordsSeen      int64     `json:"records_seen"`
	RecordsProcessed int64     `json:"records_processed"`
	RecordsStored    int64     `json:"records_stored"`
	DecodeErrors     int64     `json:"decode_errors"`
	ChunksProcessed  int64     `json:"chunks_processed"`
}

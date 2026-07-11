package rawstate

import (
	"time"

	"cc-enrich-worker/internal/rawstore"
)

const SchemaVersion = 1

type ProcessingMarker struct {
	SchemaVersion     int             `json:"schema_version"`
	CrawlID           string          `json:"crawl_id"`
	Selection         string          `json:"selection"`
	Part              int             `json:"part"`
	Processor         string          `json:"processor"`
	ProcessingVersion string          `json:"processing_version"`
	GitCommit         string          `json:"git_commit"`
	RunID             string          `json:"run_id"`
	WorkerHost        string          `json:"worker_host"`
	PID               int             `json:"pid"`
	InputReadySHA256  rawstore.SHA256 `json:"input_ready_sha256"`
	StartedAt         time.Time       `json:"started_at"`
	HeartbeatAt       time.Time       `json:"heartbeat_at"`
	LeaseExpiresAt    time.Time       `json:"lease_expires_at"`
}

type ManifestReference struct {
	Chunk          int             `json:"chunk"`
	ManifestKey    string          `json:"manifest_key"`
	ManifestSHA256 rawstore.SHA256 `json:"manifest_sha256"`
}

type ProcessedCounts struct {
	InputRecords      int64 `json:"input_records"`
	DownloadedRecords int64 `json:"downloaded_records"`
	FailedRecords     int64 `json:"failed_records"`
	ProcessedRecords  int64 `json:"processed_records"`
	SkippedRecords    int64 `json:"skipped_records"`
}

type OutputArtifact struct {
	Name      string          `json:"name"`
	Location  string          `json:"location"`
	SizeBytes int64           `json:"size_bytes"`
	RowCount  int64           `json:"row_count"`
	SHA256    rawstore.SHA256 `json:"sha256"`
}

type ProcessedMarker struct {
	SchemaVersion     int                 `json:"schema_version"`
	CrawlID           string              `json:"crawl_id"`
	Selection         string              `json:"selection"`
	Part              int                 `json:"part"`
	Processor         string              `json:"processor"`
	ProcessingVersion string              `json:"processing_version"`
	GitCommit         string              `json:"git_commit"`
	RunID             string              `json:"run_id"`
	WorkerHost        string              `json:"worker_host"`
	InputReadySHA256  rawstore.SHA256     `json:"input_ready_sha256"`
	InputManifests    []ManifestReference `json:"input_manifests"`
	Counts            ProcessedCounts     `json:"counts"`
	Outputs           []OutputArtifact    `json:"outputs"`
	StartedAt         time.Time           `json:"started_at"`
	CompletedAt       time.Time           `json:"completed_at"`
}

type LoadedMarker struct {
	SchemaVersion     int             `json:"schema_version"`
	CrawlID           string          `json:"crawl_id"`
	Selection         string          `json:"selection"`
	Part              int             `json:"part"`
	Processor         string          `json:"processor"`
	ProcessingVersion string          `json:"processing_version"`
	GitCommit         string          `json:"git_commit"`
	RunID             string          `json:"run_id"`
	SourceRunID       string          `json:"source_run_id"`
	WorkerHost        string          `json:"worker_host"`
	ProcessedSHA256   rawstore.SHA256 `json:"processed_sha256"`
	Destination       string          `json:"destination"`
	CommittedRows     int64           `json:"committed_rows"`
	CommittedObjects  int64           `json:"committed_objects"`
	StartedAt         time.Time       `json:"started_at"`
	CompletedAt       time.Time       `json:"completed_at"`
}

type ReclaimedMarker struct {
	SchemaVersion      int             `json:"schema_version"`
	CrawlID            string          `json:"crawl_id"`
	Selection          string          `json:"selection"`
	Part               int             `json:"part"`
	InputReadySHA256   rawstore.SHA256 `json:"input_ready_sha256"`
	DeletedObjectCount int64           `json:"deleted_object_count"`
	DeletedBytes       int64           `json:"deleted_bytes"`
	RunID              string          `json:"run_id"`
	Operator           string          `json:"operator"`
	WorkerHost         string          `json:"worker_host"`
	GitCommit          string          `json:"git_commit"`
	StartedAt          time.Time       `json:"started_at"`
	CompletedAt        time.Time       `json:"completed_at"`
}

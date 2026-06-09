package countryimport

import (
	"context"
	"time"
)

const (
	DefaultPageStart      = 1
	DefaultChunkSize      = 500
	DefaultRequestTimeout = 60 * time.Second
	DefaultPageDelay      = 500 * time.Millisecond
	DefaultUserAgent      = "corpscout-countrydata/1.0"
)

type BulkSource[T any] interface {
	Download(ctx context.Context, opts DownloadOptions) (DownloadResult, error)
	Process(ctx context.Context, opts ProcessOptions) (ProcessResult, error)
	Store(ctx context.Context, records []T) (StoreResult, error)
}

type DownloadOptions struct {
	DataDir        string
	MaxPages       int
	PageStart      int
	PageDelay      time.Duration
	RequestTimeout time.Duration
	UserAgent      string
	Force          bool
}

type DownloadResult struct {
	SourceSlug      string
	SnapshotPath    string
	BytesDownloaded int64
	RecordsSeen     int64
	PagesDownloaded int
	SHA256          string
	StartedAt       time.Time
	FinishedAt      time.Time
	Duration        time.Duration
}

type ProcessOptions struct {
	DataDir      string
	SnapshotPath string
	ChunkSize    int
	Limit        int64
}

type ProcessResult struct {
	SourceSlug       string
	SnapshotPath     string
	RecordsSeen      int64
	RecordsProcessed int64
	RecordsStored    int64
	DecodeErrors     int64
	ChunksProcessed  int64
	StartedAt        time.Time
	FinishedAt       time.Time
	Duration         time.Duration
}

type StoreResult struct {
	RecordsReceived int64
	RecordsStored   int64
}

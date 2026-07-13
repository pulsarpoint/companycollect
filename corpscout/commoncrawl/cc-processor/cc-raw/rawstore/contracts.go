package rawstore

import "time"

const SchemaVersion = 1

type SHA256 string

type DownloadStatus string

const (
	Downloaded DownloadStatus = "downloaded"
	NotFound   DownloadStatus = "not_found"
	Failed     DownloadStatus = "failed"
)

// IndexRow locates one selected compressed WARC record inside a raw pack. Failed
// downloads remain in the index with null pack coordinates so worklist coverage is auditable.
type IndexRow struct {
	WorklistOrdinal  int64          `parquet:"worklist_ordinal" json:"worklist_ordinal"`
	DomainRank       int64          `parquet:"domain_rank" json:"domain_rank"`
	RootDomain       string         `parquet:"root_domain" json:"root_domain"`
	URL              string         `parquet:"url" json:"url"`
	IsPrimary        bool           `parquet:"is_primary" json:"is_primary"`
	ContentLanguages *string        `parquet:"content_languages,optional" json:"content_languages"`
	WARCFilename     string         `parquet:"warc_filename" json:"warc_filename"`
	WARCOffset       int64          `parquet:"warc_offset" json:"warc_offset"`
	WARCLength       int64          `parquet:"warc_length" json:"warc_length"`
	DownloadStatus   DownloadStatus `parquet:"download_status" json:"download_status"`
	DownloadAttempts int32          `parquet:"download_attempts" json:"download_attempts"`
	PackOffset       *int64         `parquet:"pack_offset,optional" json:"pack_offset"`
	PackLength       *int64         `parquet:"pack_length,optional" json:"pack_length"`
	RecordChecksum   *string        `parquet:"record_checksum,optional" json:"record_checksum"`
	ErrorCode        *string        `parquet:"error_code,optional" json:"error_code"`
}

type ChunkWorklist struct {
	Key          string `json:"key"`
	SHA256       SHA256 `json:"sha256"`
	FirstOrdinal int64  `json:"first_ordinal"`
	RecordCount  int64  `json:"record_count"`
}

type ObjectDescriptor struct {
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    SHA256 `json:"sha256"`
}

type DownloadErrorCounts struct {
	NotFound int64 `json:"not_found"`
	Timeout  int64 `json:"timeout"`
	Other    int64 `json:"other"`
}

type DownloadResults struct {
	RequestedRecords  int64               `json:"requested_records"`
	DownloadedRecords int64               `json:"downloaded_records"`
	FailedRecords     int64               `json:"failed_records"`
	SourceBytes       int64               `json:"source_bytes"`
	PackedBytes       int64               `json:"packed_bytes"`
	Errors            DownloadErrorCounts `json:"errors"`
	FailureReasons    map[string]int64    `json:"failure_reasons,omitempty"`
}

type DownloadRun struct {
	RunID       string    `json:"run_id"`
	WorkerHost  string    `json:"worker_host"`
	GitCommit   string    `json:"git_commit"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
}

type ChunkManifest struct {
	SchemaVersion int              `json:"schema_version"`
	CrawlID       string           `json:"crawl_id"`
	Selection     string           `json:"selection"`
	Part          int              `json:"part"`
	Chunk         int              `json:"chunk"`
	Worklist      ChunkWorklist    `json:"worklist"`
	Pack          ObjectDescriptor `json:"pack"`
	Index         ObjectDescriptor `json:"index"`
	Results       DownloadResults  `json:"results"`
	Download      DownloadRun      `json:"download"`
}

type ReadyWorklist struct {
	Key         string `json:"key"`
	SizeBytes   int64  `json:"size_bytes"`
	SHA256      SHA256 `json:"sha256"`
	RecordCount int64  `json:"record_count"`
}

type ReadyChunk struct {
	Chunk          int    `json:"chunk"`
	ManifestKey    string `json:"manifest_key"`
	ManifestSHA256 SHA256 `json:"manifest_sha256"`
	FirstOrdinal   int64  `json:"first_ordinal"`
	RecordCount    int64  `json:"record_count"`
	RawBytes       int64  `json:"raw_bytes"` // pack + index + manifest object sizes
}

// CommittedChunkManifest carries the manifest body together with the checksum and
// object size observed after it was committed. It is used to validate ready.json.
type CommittedChunkManifest struct {
	Manifest          ChunkManifest
	ManifestSHA256    SHA256
	ManifestSizeBytes int64
}

type ReadyTotals struct {
	ChunkCount        int   `json:"chunk_count"`
	RequestedRecords  int64 `json:"requested_records"`
	DownloadedRecords int64 `json:"downloaded_records"`
	FailedRecords     int64 `json:"failed_records"`
	RawBytes          int64 `json:"raw_bytes"`
}

type ReadyManifest struct {
	SchemaVersion int           `json:"schema_version"`
	CrawlID       string        `json:"crawl_id"`
	Selection     string        `json:"selection"`
	Part          int           `json:"part"`
	Worklist      ReadyWorklist `json:"worklist"`
	Chunks        []ReadyChunk  `json:"chunks"`
	Totals        ReadyTotals   `json:"totals"`
	DownloadRunID string        `json:"download_run_id"`
	CompletedAt   time.Time     `json:"completed_at"`
}

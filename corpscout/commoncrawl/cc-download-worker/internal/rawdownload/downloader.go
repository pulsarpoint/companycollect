package rawdownload

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"cc-raw/fetch"
	"cc-raw/rawstate"
	"cc-raw/rawstore"
	"github.com/cockroachdb/errors"
	"github.com/dustin/go-humanize"
)

type Config struct {
	WorklistPath    string
	WorklistKey     string
	CrawlID         string
	Selection       string
	Part            int
	SourceBucket    string
	Concurrency     int
	MaxPackBytes    int64
	MaxRecords      int
	MaxFailureRate  float64
	RecordAttempts  int
	RecordTimeout   time.Duration
	TempDir         string
	RunID           string
	WorkerHost      string
	GitCommit       string
	ForceRedownload bool
}

type Result struct {
	Skipped           bool
	ChunkCount        int
	RequestedRecords  int64
	DownloadedRecords int64
	FailedRecords     int64
	RawBytes          int64
}

type Downloader struct {
	Source fetch.RangeGetter
	Store  *rawstore.Store
	Logger *slog.Logger
	Config Config
}

func (downloader *Downloader) Run(ctx context.Context) (Result, error) {
	if err := downloader.validate(); err != nil {
		return Result{}, err
	}
	worklist, err := readWorklist(downloader.Config.WorklistPath, downloader.Config.WorklistKey)
	if err != nil {
		return Result{}, err
	}
	plans := planChunks(worklist.Records, downloader.Config.MaxPackBytes, downloader.Config.MaxRecords)

	reclaimedKey, err := rawstate.ReclaimedKey(downloader.Config.CrawlID, downloader.Config.Selection, downloader.Config.Part)
	if err != nil {
		return Result{}, err
	}
	reclaimed, err := downloader.Store.Exists(ctx, reclaimedKey)
	if err != nil {
		return Result{}, err
	}
	if reclaimed && !downloader.Config.ForceRedownload {
		return Result{}, errors.New("raw part was reclaimed; explicit force-redownload is required")
	}

	readyKey, err := rawstate.DownloadReadyKey(downloader.Config.CrawlID, downloader.Config.Selection, downloader.Config.Part)
	if err != nil {
		return Result{}, err
	}
	if !reclaimed {
		ready, valid, err := downloader.loadReady(ctx, readyKey, worklist, plans)
		if err != nil {
			return Result{}, err
		}
		if valid {
			return resultFromReady(ready, true), nil
		}
	}

	committed := make([]rawstore.CommittedChunkManifest, 0, len(plans))
	readyChunks := make([]rawstore.ReadyChunk, 0, len(plans))
	var downloadedRecords, failedRecords, rawBytes int64
	for _, plan := range plans {
		chunk, valid, err := downloader.loadCommittedChunk(ctx, worklist, plan)
		if err != nil {
			return Result{}, err
		}
		reused := valid
		if !reused {
			chunk, err = downloader.downloadChunk(ctx, worklist, plan)
			if err != nil {
				return Result{}, errors.Wrapf(err, "download chunk %d", plan.Number)
			}
		}
		manifest := chunk.Manifest
		committed = append(committed, chunk)
		chunkRawBytes := manifest.Pack.SizeBytes + manifest.Index.SizeBytes + chunk.ManifestSizeBytes
		keys, err := rawstore.KeysForChunk(downloader.Config.CrawlID, downloader.Config.Selection, downloader.Config.Part, plan.Number)
		if err != nil {
			return Result{}, err
		}
		readyChunks = append(readyChunks, rawstore.ReadyChunk{
			Chunk:          plan.Number,
			ManifestKey:    keys.Manifest,
			ManifestSHA256: chunk.ManifestSHA256,
			FirstOrdinal:   manifest.Worklist.FirstOrdinal,
			RecordCount:    manifest.Worklist.RecordCount,
			RawBytes:       chunkRawBytes,
		})
		downloadedRecords += manifest.Results.DownloadedRecords
		failedRecords += manifest.Results.FailedRecords
		rawBytes += chunkRawBytes
		downloader.Logger.Info("chunk ready",
			"crawl", downloader.Config.CrawlID,
			"selection", downloader.Config.Selection,
			"part", downloader.Config.Part,
			"chunk", plan.Number,
			"reused", reused,
			"requested_records", manifest.Results.RequestedRecords,
			"downloaded_records", manifest.Results.DownloadedRecords,
			"failed_records", manifest.Results.FailedRecords,
			"failed_not_found", manifest.Results.Errors.NotFound,
			"failed_timeout", manifest.Results.Errors.Timeout,
			"failed_other", manifest.Results.Errors.Other,
			"failure_reasons", manifest.Results.FailureReasons,
			"raw_bytes", chunkRawBytes,
			"raw_size", humanize.IBytes(uint64(chunkRawBytes)),
		)
	}

	ready := rawstore.ReadyManifest{
		SchemaVersion: rawstore.SchemaVersion,
		CrawlID:       downloader.Config.CrawlID,
		Selection:     downloader.Config.Selection,
		Part:          downloader.Config.Part,
		Worklist: rawstore.ReadyWorklist{
			Key:         worklist.Key,
			SizeBytes:   worklist.Size,
			SHA256:      worklist.Checksum,
			RecordCount: int64(len(worklist.Records)),
		},
		Chunks: readyChunks,
		Totals: rawstore.ReadyTotals{
			ChunkCount:        len(readyChunks),
			RequestedRecords:  int64(len(worklist.Records)),
			DownloadedRecords: downloadedRecords,
			FailedRecords:     failedRecords,
			RawBytes:          rawBytes,
		},
		DownloadRunID: downloader.Config.RunID,
		CompletedAt:   time.Now().UTC(),
	}
	if err := ready.ValidateCommittedChunks(committed); err != nil {
		return Result{}, errors.Wrap(err, "validate ready manifest")
	}
	readyBody, err := rawstore.EncodeReadyManifest(ready)
	if err != nil {
		return Result{}, errors.Wrap(err, "encode ready manifest")
	}
	readyChecksum := rawstore.ChecksumBytes(readyBody)
	if err := downloader.Store.PutBytes(ctx, readyKey, "application/json", readyBody, readyChecksum); err != nil {
		return Result{}, err
	}
	readyObject := rawstore.ObjectDescriptor{Key: readyKey, SizeBytes: int64(len(readyBody)), SHA256: readyChecksum}
	matches, err := downloader.Store.ObjectMatches(ctx, readyObject)
	if err != nil {
		return Result{}, err
	}
	if !matches {
		return Result{}, errors.New("uploaded ready manifest failed size/checksum verification")
	}
	if reclaimed {
		if err := downloader.Store.Delete(ctx, reclaimedKey); err != nil {
			return Result{}, err
		}
	}
	return resultFromReady(ready, false), nil
}

func (downloader *Downloader) validate() error {
	config := downloader.Config
	if downloader.Source == nil || downloader.Store == nil || downloader.Logger == nil {
		return errors.New("source getter, RustFS store, and logger are required")
	}
	if err := rawstore.ValidatePartIdentity(config.CrawlID, config.Selection, config.Part); err != nil {
		return err
	}
	if strings.TrimSpace(config.WorklistPath) == "" || strings.TrimSpace(config.WorklistKey) == "" || strings.TrimSpace(config.SourceBucket) == "" {
		return errors.New("worklist path, worklist key, and source bucket are required")
	}
	if config.Concurrency <= 0 || config.MaxPackBytes <= 0 || config.MaxRecords <= 0 || config.RecordTimeout <= 0 {
		return errors.New("concurrency, pack limits, and record timeout must be positive")
	}
	if config.RecordAttempts < 1 || config.RecordAttempts > 10 {
		return errors.Newf("record attempts must be between 1 and 10, got %d", config.RecordAttempts)
	}
	if config.MaxFailureRate < 0 || config.MaxFailureRate > 1 {
		return errors.Newf("max failure rate must be between 0 and 1, got %f", config.MaxFailureRate)
	}
	if strings.TrimSpace(config.RunID) == "" || strings.TrimSpace(config.WorkerHost) == "" || strings.TrimSpace(config.GitCommit) == "" {
		return errors.New("run ID, worker host, and git commit are required")
	}
	return nil
}

func (downloader *Downloader) loadReady(ctx context.Context, key string, worklist worklistData, plans []chunkPlan) (rawstore.ReadyManifest, bool, error) {
	exists, err := downloader.Store.Exists(ctx, key)
	if err != nil || !exists {
		return rawstore.ReadyManifest{}, false, err
	}
	body, err := downloader.Store.ReadBytes(ctx, key)
	if err != nil {
		return rawstore.ReadyManifest{}, false, err
	}
	ready, err := rawstore.DecodeReadyManifest(body)
	if err != nil || !readyMatchesWorklist(ready, downloader.Config, worklist, len(plans)) {
		return rawstore.ReadyManifest{}, false, nil
	}
	committed := make([]rawstore.CommittedChunkManifest, 0, len(plans))
	for _, plan := range plans {
		chunk, valid, err := downloader.loadCommittedChunk(ctx, worklist, plan)
		if err != nil {
			return rawstore.ReadyManifest{}, false, err
		}
		if !valid {
			return rawstore.ReadyManifest{}, false, nil
		}
		committed = append(committed, chunk)
	}
	if err := ready.ValidateCommittedChunks(committed); err != nil {
		return rawstore.ReadyManifest{}, false, nil
	}
	return ready, true, nil
}

func (downloader *Downloader) loadCommittedChunk(ctx context.Context, worklist worklistData, plan chunkPlan) (rawstore.CommittedChunkManifest, bool, error) {
	keys, err := rawstore.KeysForChunk(downloader.Config.CrawlID, downloader.Config.Selection, downloader.Config.Part, plan.Number)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, false, err
	}
	exists, err := downloader.Store.Exists(ctx, keys.Manifest)
	if err != nil || !exists {
		return rawstore.CommittedChunkManifest{}, false, err
	}
	body, err := downloader.Store.ReadBytes(ctx, keys.Manifest)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, false, err
	}
	manifest, err := rawstore.DecodeChunkManifest(body)
	if err != nil || !manifestMatchesPlan(manifest, downloader.Config, worklist, plan) {
		return rawstore.CommittedChunkManifest{}, false, nil
	}
	packMatches, err := downloader.Store.ObjectMatches(ctx, manifest.Pack)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, false, err
	}
	indexMatches, err := downloader.Store.ObjectMatches(ctx, manifest.Index)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, false, err
	}
	if !packMatches || !indexMatches {
		return rawstore.CommittedChunkManifest{}, false, nil
	}
	return rawstore.CommittedChunkManifest{
		Manifest:          manifest,
		ManifestSHA256:    rawstore.ChecksumBytes(body),
		ManifestSizeBytes: int64(len(body)),
	}, true, nil
}

func manifestMatchesPlan(manifest rawstore.ChunkManifest, config Config, worklist worklistData, plan chunkPlan) bool {
	return manifest.CrawlID == config.CrawlID && manifest.Selection == config.Selection && manifest.Part == config.Part &&
		manifest.Chunk == plan.Number && manifest.Worklist.Key == worklist.Key && manifest.Worklist.SHA256 == worklist.Checksum &&
		manifest.Worklist.FirstOrdinal == plan.Records[0].Ordinal && manifest.Worklist.RecordCount == int64(len(plan.Records))
}

func readyMatchesWorklist(ready rawstore.ReadyManifest, config Config, worklist worklistData, chunkCount int) bool {
	return ready.CrawlID == config.CrawlID && ready.Selection == config.Selection && ready.Part == config.Part &&
		ready.Worklist.Key == worklist.Key && ready.Worklist.SHA256 == worklist.Checksum && ready.Worklist.SizeBytes == worklist.Size &&
		ready.Worklist.RecordCount == int64(len(worklist.Records)) && len(ready.Chunks) == chunkCount
}

func resultFromReady(ready rawstore.ReadyManifest, skipped bool) Result {
	return Result{
		Skipped:           skipped,
		ChunkCount:        ready.Totals.ChunkCount,
		RequestedRecords:  ready.Totals.RequestedRecords,
		DownloadedRecords: ready.Totals.DownloadedRecords,
		FailedRecords:     ready.Totals.FailedRecords,
		RawBytes:          ready.Totals.RawBytes,
	}
}

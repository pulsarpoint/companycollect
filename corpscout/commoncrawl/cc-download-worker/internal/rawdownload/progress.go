package rawdownload

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cc-raw/fetch"
	"cc-raw/rawstore"
	"github.com/dustin/go-humanize"
)

const bytesPerMiB = 1024 * 1024

// throttleCooldown pauses new logical record attempts after the source has exhausted its own retry
// budget with a throttling error. Signed S3 requests are also governed by the AWS adaptive retry
// limiter, which reacts to every 429/503 before an error reaches this layer.
type throttleCooldown struct {
	mu    sync.Mutex
	until time.Time
}

func (cooldown *throttleCooldown) slowDown(attempt int) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 4 {
		shift = 4
	}
	delay := time.Duration(1<<shift) * time.Second
	cooldown.mu.Lock()
	if until := time.Now().Add(delay); until.After(cooldown.until) {
		cooldown.until = until
	}
	cooldown.mu.Unlock()
	return delay
}

func (cooldown *throttleCooldown) wait(ctx context.Context) error {
	for {
		cooldown.mu.Lock()
		delay := time.Until(cooldown.until)
		cooldown.mu.Unlock()
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (cooldown *throttleCooldown) remaining() time.Duration {
	cooldown.mu.Lock()
	defer cooldown.mu.Unlock()
	remaining := time.Until(cooldown.until)
	if remaining < 0 {
		return 0
	}
	return remaining
}

type downloadProgress struct {
	logger      *slog.Logger
	source      *fetch.S3Getter
	cooldown    *throttleCooldown
	crawlID     string
	selection   string
	part        int
	requested   int64
	totalChunks int
	concurrency int
	interval    time.Duration
	startedAt   time.Time
	sourceStart fetch.S3Stats

	active, downloaded, failed, reused, downloadedBytes atomic.Int64
	recordAttempts, recordRetries, throttleErrors       atomic.Int64
	readyChunks, downloadedChunks, reusedChunks         atomic.Int64
	committedRawBytes                                   atomic.Int64
	currentChunk                                        atomic.Int64
	currentChunkRequested, currentChunkPlannedBytes     atomic.Int64
	currentChunkDownloaded, currentChunkFailed          atomic.Int64
	currentChunkDownloadedBytes, currentChunkRawBytes   atomic.Int64
	phase                                               atomic.Value
}

type progressSnapshot struct {
	downloaded, failed, reused, downloadedBytes       int64
	recordAttempts, recordRetries, throttleErrors     int64
	readyChunks, downloadedChunks, reusedChunks       int64
	committedRawBytes                                 int64
	currentChunk                                      int64
	currentChunkRequested, currentChunkPlannedBytes   int64
	currentChunkDownloaded, currentChunkFailed        int64
	currentChunkDownloadedBytes, currentChunkRawBytes int64
	phase                                             string
	source                                            fetch.S3Stats
	at                                                time.Time
}

func newDownloadProgress(downloader *Downloader, requested int64, totalChunks int, cooldown *throttleCooldown) *downloadProgress {
	progress := &downloadProgress{
		logger:      downloader.Logger,
		cooldown:    cooldown,
		crawlID:     downloader.Config.CrawlID,
		selection:   downloader.Config.Selection,
		part:        downloader.Config.Part,
		requested:   requested,
		totalChunks: totalChunks,
		concurrency: downloader.Config.Concurrency,
		interval:    downloader.Config.ProgressInterval,
		startedAt:   time.Now(),
	}
	progress.currentChunk.Store(-1)
	progress.phase.Store("preparing")
	if source, ok := downloader.Source.(*fetch.S3Getter); ok {
		progress.source = source
		progress.sourceStart = source.Stats()
	}
	return progress
}

func (progress *downloadProgress) beginChunk(plan chunkPlan) {
	var plannedBytes int64
	for _, record := range plan.Records {
		plannedBytes += record.WARCRecordLength
	}
	progress.currentChunk.Store(int64(plan.Number))
	progress.currentChunkRequested.Store(int64(len(plan.Records)))
	progress.currentChunkPlannedBytes.Store(plannedBytes)
	progress.currentChunkDownloaded.Store(0)
	progress.currentChunkFailed.Store(0)
	progress.currentChunkDownloadedBytes.Store(0)
	progress.currentChunkRawBytes.Store(0)
	progress.setPhase("checking_chunk")
}

func (progress *downloadProgress) setPhase(phase string) {
	progress.phase.Store(phase)
}

func (progress *downloadProgress) chunkReady(reused bool, rawBytes int64) {
	progress.readyChunks.Add(1)
	progress.committedRawBytes.Add(rawBytes)
	progress.currentChunkRawBytes.Store(rawBytes)
	if reused {
		progress.reusedChunks.Add(1)
	} else {
		progress.downloadedChunks.Add(1)
	}
	progress.setPhase("chunk_ready")
}

func (progress *downloadProgress) chunkPrepared(rawBytes int64) {
	progress.currentChunkRawBytes.Store(rawBytes)
}

func (progress *downloadProgress) start(ctx context.Context) func() {
	if progress.interval == 0 {
		return func() { progress.log(ctx, progress.snapshot(), progress.snapshot(), true) }
	}
	stopped := make(chan struct{})
	done := make(chan struct{})
	previous := progress.snapshot()
	go func() {
		defer close(done)
		ticker := time.NewTicker(progress.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				current := progress.snapshot()
				progress.log(ctx, previous, current, true)
				return
			case <-stopped:
				current := progress.snapshot()
				progress.log(ctx, previous, current, true)
				return
			case <-ticker.C:
				current := progress.snapshot()
				progress.log(ctx, previous, current, false)
				previous = current
			}
		}
	}()
	return func() {
		close(stopped)
		<-done
	}
}

func (progress *downloadProgress) recordStarted() {
	progress.active.Add(1)
}

func (progress *downloadProgress) recordAttempted(attempt int) {
	progress.recordAttempts.Add(1)
	if attempt > 1 {
		progress.recordRetries.Add(1)
	}
}

func (progress *downloadProgress) recordThrottled() {
	progress.throttleErrors.Add(1)
}

func (progress *downloadProgress) recordFinished(download recordDownload) {
	progress.active.Add(-1)
	if download.status == rawstore.Downloaded {
		progress.downloaded.Add(1)
		progress.downloadedBytes.Add(int64(len(download.raw)))
		progress.currentChunkDownloaded.Add(1)
		progress.currentChunkDownloadedBytes.Add(int64(len(download.raw)))
		return
	}
	progress.failed.Add(1)
	progress.currentChunkFailed.Add(1)
}

func (progress *downloadProgress) recordsReused(downloaded, failed, downloadedBytes int64) {
	progress.reused.Add(downloaded + failed)
	progress.currentChunkDownloaded.Store(downloaded)
	progress.currentChunkFailed.Store(failed)
	progress.currentChunkDownloadedBytes.Store(downloadedBytes)
}

func (progress *downloadProgress) snapshot() progressSnapshot {
	snapshot := progressSnapshot{
		downloaded:                  progress.downloaded.Load(),
		failed:                      progress.failed.Load(),
		reused:                      progress.reused.Load(),
		downloadedBytes:             progress.downloadedBytes.Load(),
		recordAttempts:              progress.recordAttempts.Load(),
		recordRetries:               progress.recordRetries.Load(),
		throttleErrors:              progress.throttleErrors.Load(),
		readyChunks:                 progress.readyChunks.Load(),
		downloadedChunks:            progress.downloadedChunks.Load(),
		reusedChunks:                progress.reusedChunks.Load(),
		committedRawBytes:           progress.committedRawBytes.Load(),
		currentChunk:                progress.currentChunk.Load(),
		currentChunkRequested:       progress.currentChunkRequested.Load(),
		currentChunkPlannedBytes:    progress.currentChunkPlannedBytes.Load(),
		currentChunkDownloaded:      progress.currentChunkDownloaded.Load(),
		currentChunkFailed:          progress.currentChunkFailed.Load(),
		currentChunkDownloadedBytes: progress.currentChunkDownloadedBytes.Load(),
		currentChunkRawBytes:        progress.currentChunkRawBytes.Load(),
		phase:                       progress.phase.Load().(string),
		at:                          time.Now(),
	}
	if progress.source != nil {
		snapshot.source = progress.source.Stats()
	}
	return snapshot
}

func (progress *downloadProgress) log(ctx context.Context, previous, current progressSnapshot, final bool) {
	interval := current.at.Sub(previous.at).Seconds()
	if interval <= 0 {
		interval = 1
	}
	elapsed := current.at.Sub(progress.startedAt).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	completed := current.downloaded + current.failed
	previousCompleted := previous.downloaded + previous.failed
	recordsPerSecond := float64(completed-previousCompleted) / interval
	averageRecordsPerSecond := float64(completed) / elapsed

	intervalBytes := current.downloadedBytes - previous.downloadedBytes
	totalBytes := current.downloadedBytes
	sourceDelta := current.source.Delta(previous.source)
	sourceTotal := current.source.Delta(progress.sourceStart)
	if progress.source != nil {
		intervalBytes = sourceDelta.BodyBytes
		totalBytes = sourceTotal.BodyBytes
	}
	sourceMiBPerSecond := float64(intervalBytes) / bytesPerMiB / interval
	averageSourceMiBPerSecond := float64(totalBytes) / bytesPerMiB / elapsed
	sdkRetryAttempts := sourceTotal.HTTPAttempts - sourceTotal.GetObjectCalls
	if sdkRetryAttempts < 0 {
		sdkRetryAttempts = 0
	}
	throttleResponses := sourceTotal.HTTP429s + sourceTotal.HTTP503s
	intervalThrottles := sourceDelta.HTTP429s + sourceDelta.HTTP503s + current.throttleErrors - previous.throttleErrors

	attributes := []slog.Attr{
		slog.String("crawl", progress.crawlID),
		slog.String("selection", progress.selection),
		slog.Int("part", progress.part),
		slog.Bool("final", final),
		slog.String("phase", current.phase),
		slog.Int("chunks_total", progress.totalChunks),
		slog.Int64("chunks_ready", current.readyChunks),
		slog.Int64("chunks_remaining", int64(progress.totalChunks)-current.readyChunks),
		slog.Int64("chunks_downloaded", current.downloadedChunks),
		slog.Int64("chunks_reused", current.reusedChunks),
		slog.Int64("current_chunk", current.currentChunk),
		slog.Int64("current_chunk_requested_records", current.currentChunkRequested),
		slog.Int64("current_chunk_completed_records", current.currentChunkDownloaded+current.currentChunkFailed),
		slog.Int64("current_chunk_downloaded_records", current.currentChunkDownloaded),
		slog.Int64("current_chunk_failed_records", current.currentChunkFailed),
		slog.Int64("current_chunk_planned_bytes", current.currentChunkPlannedBytes),
		slog.String("current_chunk_planned_size", humanize.IBytes(uint64(current.currentChunkPlannedBytes))),
		slog.Int64("current_chunk_downloaded_bytes", current.currentChunkDownloadedBytes),
		slog.String("current_chunk_downloaded_size", humanize.IBytes(uint64(current.currentChunkDownloadedBytes))),
		slog.Int64("current_chunk_raw_bytes", current.currentChunkRawBytes),
		slog.String("current_chunk_raw_size", humanize.IBytes(uint64(current.currentChunkRawBytes))),
		slog.Int64("committed_raw_bytes", current.committedRawBytes),
		slog.String("committed_raw_size", humanize.IBytes(uint64(current.committedRawBytes))),
		slog.Int64("requested_records", progress.requested),
		slog.Int64("completed_records", completed+current.reused),
		slog.Int64("downloaded_records", current.downloaded),
		slog.Int64("failed_records", current.failed),
		slog.Int64("reused_records", current.reused),
		slog.Int64("active_downloads", progress.active.Load()),
		slog.Int("configured_concurrency", progress.concurrency),
		slog.Float64("records_per_second", recordsPerSecond),
		slog.Float64("average_records_per_second", averageRecordsPerSecond),
		slog.Float64("source_mib_per_second", sourceMiBPerSecond),
		slog.Float64("average_source_mib_per_second", averageSourceMiBPerSecond),
		slog.Int64("source_bytes", totalBytes),
		slog.String("source_size", humanize.IBytes(uint64(totalBytes))),
		slog.Int64("record_attempts", current.recordAttempts),
		slog.Int64("record_retries", current.recordRetries),
		slog.Int64("logical_throttle_errors", current.throttleErrors),
		slog.Int64("http_429", sourceTotal.HTTP429s),
		slog.Int64("http_503", sourceTotal.HTTP503s),
		slog.Int64("sdk_retry_attempts", sdkRetryAttempts),
		slog.Int64("body_read_errors", sourceTotal.BodyReadErrors),
		slog.Int64("body_read_retries", sourceTotal.BodyReadRetries),
		slog.Int64("cooldown_remaining_ms", progress.cooldown.remaining().Milliseconds()),
		slog.Float64("elapsed_seconds", elapsed),
	}
	if intervalThrottles > 0 || final && throttleResponses+current.throttleErrors > 0 {
		progress.logger.LogAttrs(ctx, slog.LevelWarn, "download throttled", attributes...)
		return
	}
	progress.logger.LogAttrs(ctx, slog.LevelInfo, "download progress", attributes...)
}

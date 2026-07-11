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
	concurrency int
	interval    time.Duration
	startedAt   time.Time
	sourceStart fetch.S3Stats

	active, downloaded, failed, reused, downloadedBytes atomic.Int64
	recordAttempts, recordRetries, throttleErrors       atomic.Int64
}

type progressSnapshot struct {
	downloaded, failed, reused, downloadedBytes   int64
	recordAttempts, recordRetries, throttleErrors int64
	source                                        fetch.S3Stats
	at                                            time.Time
}

func newDownloadProgress(downloader *Downloader, requested int64, cooldown *throttleCooldown) *downloadProgress {
	progress := &downloadProgress{
		logger:      downloader.Logger,
		cooldown:    cooldown,
		crawlID:     downloader.Config.CrawlID,
		selection:   downloader.Config.Selection,
		part:        downloader.Config.Part,
		requested:   requested,
		concurrency: downloader.Config.Concurrency,
		interval:    downloader.Config.ProgressInterval,
		startedAt:   time.Now(),
	}
	if source, ok := downloader.Source.(*fetch.S3Getter); ok {
		progress.source = source
		progress.sourceStart = source.Stats()
	}
	return progress
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
		return
	}
	progress.failed.Add(1)
}

func (progress *downloadProgress) recordsReused(count int64) {
	progress.reused.Add(count)
}

func (progress *downloadProgress) snapshot() progressSnapshot {
	snapshot := progressSnapshot{
		downloaded:      progress.downloaded.Load(),
		failed:          progress.failed.Load(),
		reused:          progress.reused.Load(),
		downloadedBytes: progress.downloadedBytes.Load(),
		recordAttempts:  progress.recordAttempts.Load(),
		recordRetries:   progress.recordRetries.Load(),
		throttleErrors:  progress.throttleErrors.Load(),
		at:              time.Now(),
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

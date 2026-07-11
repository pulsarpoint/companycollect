package rawdownload

import (
	"context"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cc-raw/fetch"
	"cc-raw/rawstore"
	"github.com/aws/smithy-go"
	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
	"golang.org/x/sync/errgroup"
)

type recordDownload struct {
	raw       []byte
	status    rawstore.DownloadStatus
	errorCode string
	attempts  int32
}

func (downloader *Downloader) downloadChunk(
	ctx context.Context,
	worklist worklistData,
	plan chunkPlan,
	cooldown *throttleCooldown,
) (rawstore.CommittedChunkManifest, error) {
	startedAt := time.Now().UTC()
	downloads := make([]recordDownload, len(plan.Records))
	var group errgroup.Group
	group.SetLimit(downloader.Config.Concurrency)
	for index := range plan.Records {
		index := index
		group.Go(func() error {
			downloads[index] = downloader.downloadRecord(ctx, plan.Records[index], cooldown)
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "wait for record downloads")
	}
	if err := ctx.Err(); err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "download chunk context")
	}

	results := summarizeDownloads(downloads)
	if results.FailedRecords == int64(len(downloads)) {
		return rawstore.CommittedChunkManifest{}, errors.Newf(
			"all records in chunk failed (not_found=%d timeout=%d other=%d reasons=%v)",
			results.Errors.NotFound,
			results.Errors.Timeout,
			results.Errors.Other,
			results.FailureReasons,
		)
	}
	allowedFailures := int64(math.Ceil(downloader.Config.MaxFailureRate * float64(len(downloads))))
	if results.FailedRecords > allowedFailures {
		return rawstore.CommittedChunkManifest{}, errors.Newf(
			"failed records %d exceeds allowed %d at failure-rate limit %.4f (not_found=%d timeout=%d other=%d reasons=%v)",
			results.FailedRecords,
			allowedFailures,
			downloader.Config.MaxFailureRate,
			results.Errors.NotFound,
			results.Errors.Timeout,
			results.Errors.Other,
			results.FailureReasons,
		)
	}

	tempDir, err := os.MkdirTemp(downloader.Config.TempDir, "cc-download-chunk-")
	if err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "create chunk temp directory")
	}
	defer os.RemoveAll(tempDir)
	packPath := filepath.Join(tempDir, "records.pack")
	indexPath := filepath.Join(tempDir, "index.parquet")
	indexRows, results, err := writePackAndIndex(packPath, indexPath, plan, downloads, results)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}

	packChecksum, packSize, err := rawstore.ChecksumFile(packPath)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	indexChecksum, indexSize, err := rawstore.ChecksumFile(indexPath)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	keys, err := rawstore.KeysForChunk(downloader.Config.CrawlID, downloader.Config.Selection, downloader.Config.Part, plan.Number)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	manifest := rawstore.ChunkManifest{
		SchemaVersion: rawstore.SchemaVersion,
		CrawlID:       downloader.Config.CrawlID,
		Selection:     downloader.Config.Selection,
		Part:          downloader.Config.Part,
		Chunk:         plan.Number,
		Worklist: rawstore.ChunkWorklist{
			Key:          worklist.Key,
			SHA256:       worklist.Checksum,
			FirstOrdinal: plan.Records[0].Ordinal,
			RecordCount:  int64(len(plan.Records)),
		},
		Pack:    rawstore.ObjectDescriptor{Key: keys.Pack, SizeBytes: packSize, SHA256: packChecksum},
		Index:   rawstore.ObjectDescriptor{Key: keys.Index, SizeBytes: indexSize, SHA256: indexChecksum},
		Results: results,
		Download: rawstore.DownloadRun{
			RunID:       downloader.Config.RunID,
			WorkerHost:  downloader.Config.WorkerHost,
			GitCommit:   downloader.Config.GitCommit,
			StartedAt:   startedAt,
			CompletedAt: time.Now().UTC(),
		},
	}
	if err := rawstore.ValidateIndexRows(indexRows, manifest); err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "validate chunk index")
	}
	manifestBody, err := rawstore.EncodeChunkManifest(manifest)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "encode chunk manifest")
	}
	manifestChecksum := rawstore.ChecksumBytes(manifestBody)

	if err := downloader.Store.PutFile(ctx, keys.Pack, packPath, "application/octet-stream", packChecksum); err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	if err := downloader.Store.PutFile(ctx, keys.Index, indexPath, "application/vnd.apache.parquet", indexChecksum); err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	for _, object := range []rawstore.ObjectDescriptor{manifest.Pack, manifest.Index} {
		matches, err := downloader.Store.ObjectMatches(ctx, object)
		if err != nil {
			return rawstore.CommittedChunkManifest{}, err
		}
		if !matches {
			return rawstore.CommittedChunkManifest{}, errors.Newf("uploaded object %s failed size/checksum verification", object.Key)
		}
	}
	if err := downloader.Store.PutBytes(ctx, keys.Manifest, "application/json", manifestBody, manifestChecksum); err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	manifestObject := rawstore.ObjectDescriptor{Key: keys.Manifest, SizeBytes: int64(len(manifestBody)), SHA256: manifestChecksum}
	matches, err := downloader.Store.ObjectMatches(ctx, manifestObject)
	if err != nil {
		return rawstore.CommittedChunkManifest{}, err
	}
	if !matches {
		return rawstore.CommittedChunkManifest{}, errors.Newf("uploaded manifest %s failed size/checksum verification", keys.Manifest)
	}
	return rawstore.CommittedChunkManifest{
		Manifest:          manifest,
		ManifestSHA256:    manifestChecksum,
		ManifestSizeBytes: int64(len(manifestBody)),
	}, nil
}

func (downloader *Downloader) downloadRecord(
	ctx context.Context,
	record selectedRecord,
	cooldown *throttleCooldown,
) (result recordDownload) {
	for attempt := 1; attempt <= downloader.Config.RecordAttempts; attempt++ {
		if err := cooldown.wait(ctx); err != nil {
			result.status = rawstore.Failed
			result.errorCode = "canceled"
			result.attempts = int32(attempt)
			return result
		}
		recordContext, cancel := context.WithTimeout(ctx, downloader.Config.RecordTimeout)
		raw, err := fetch.FetchRawRecord(
			recordContext,
			downloader.Source,
			downloader.Config.SourceBucket,
			record.WARCFilename,
			record.WARCRecordOffset,
			record.WARCRecordLength,
		)
		cancel()

		result.attempts = int32(attempt)
		if err == nil {
			result.raw = raw
			result.status = rawstore.Downloaded
			result.errorCode = ""
			return result
		}
		result.status, result.errorCode = classifyDownloadError(err)
		if result.errorCode == "throttled" {
			cooldown.slowDown(attempt)
		}
		if result.status == rawstore.NotFound || attempt == downloader.Config.RecordAttempts {
			return result
		}
		if result.errorCode == "throttled" {
			continue
		}

		timer := time.NewTimer(time.Duration(attempt) * 250 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result
		case <-timer.C:
		}
	}
	return result
}

func summarizeDownloads(downloads []recordDownload) rawstore.DownloadResults {
	results := rawstore.DownloadResults{RequestedRecords: int64(len(downloads))}
	for _, download := range downloads {
		if download.status == rawstore.Downloaded {
			results.DownloadedRecords++
			continue
		}
		results.FailedRecords++
		if results.FailureReasons == nil {
			results.FailureReasons = make(map[string]int64)
		}
		results.FailureReasons[download.errorCode]++
		switch {
		case download.status == rawstore.NotFound:
			results.Errors.NotFound++
		case download.errorCode == "timeout":
			results.Errors.Timeout++
		default:
			results.Errors.Other++
		}
	}
	return results
}

func writePackAndIndex(packPath, indexPath string, plan chunkPlan, downloads []recordDownload, results rawstore.DownloadResults) ([]rawstore.IndexRow, rawstore.DownloadResults, error) {
	pack, err := os.Create(packPath)
	if err != nil {
		return nil, rawstore.DownloadResults{}, errors.Wrap(err, "create raw pack")
	}
	indexRows := make([]rawstore.IndexRow, 0, len(plan.Records))
	var packOffset int64
	for index, record := range plan.Records {
		download := downloads[index]
		row := rawstore.IndexRow{
			WorklistOrdinal:  record.Ordinal,
			DomainRank:       record.DomainRank,
			RootDomain:       record.RootDomain,
			URL:              record.URL,
			IsPrimary:        record.Primary,
			ContentLanguages: record.ContentLanguages,
			WARCFilename:     record.WARCFilename,
			WARCOffset:       record.WARCRecordOffset,
			WARCLength:       record.WARCRecordLength,
			DownloadStatus:   download.status,
			DownloadAttempts: download.attempts,
		}
		if download.status == rawstore.Downloaded {
			if _, err := pack.Write(download.raw); err != nil {
				_ = pack.Close()
				return nil, rawstore.DownloadResults{}, errors.Wrap(err, "write raw pack")
			}
			length := int64(len(download.raw))
			offset := packOffset
			checksum := string(rawstore.ChecksumBytes(download.raw))
			row.PackOffset = &offset
			row.PackLength = &length
			row.RecordChecksum = &checksum
			packOffset += length
		} else {
			errorCode := download.errorCode
			row.ErrorCode = &errorCode
		}
		indexRows = append(indexRows, row)
	}
	if err := pack.Close(); err != nil {
		return nil, rawstore.DownloadResults{}, errors.Wrap(err, "close raw pack")
	}
	results.SourceBytes = packOffset
	results.PackedBytes = packOffset
	if err := parquet.WriteFile(indexPath, indexRows); err != nil {
		return nil, rawstore.DownloadResults{}, errors.Wrap(err, "write chunk index parquet")
	}
	return indexRows, results, nil
}

func classifyDownloadError(err error) (rawstore.DownloadStatus, string) {
	if errors.Is(err, context.DeadlineExceeded) {
		return rawstore.Failed, "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return rawstore.Failed, "canceled"
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return rawstore.Failed, "timeout"
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "404", "NoSuchKey", "NotFound":
			return rawstore.NotFound, "not_found"
		case "RequestTimeout", "RequestTimeoutException":
			return rawstore.Failed, "timeout"
		case "429", "SlowDown", "Throttling", "ThrottlingException", "TooManyRequestsException":
			return rawstore.Failed, "throttled"
		case "403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch":
			return rawstore.Failed, "access_denied"
		case "InvalidRange", "RequestedRangeNotSatisfiable":
			return rawstore.Failed, "invalid_range"
		}
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "http 404"), strings.Contains(message, "status code: 404"), strings.Contains(message, "no such key"):
		return rawstore.NotFound, "not_found"
	case strings.Contains(message, "http 429"), strings.Contains(message, "status code: 429"), strings.Contains(message, "slowdown"), strings.Contains(message, "throttl"):
		return rawstore.Failed, "throttled"
	case strings.Contains(message, "http 403"), strings.Contains(message, "status code: 403"), strings.Contains(message, "access denied"):
		return rawstore.Failed, "access_denied"
	case strings.Contains(message, "short warc range"):
		return rawstore.Failed, "short_read"
	case strings.Contains(message, "unexpected eof"):
		return rawstore.Failed, "unexpected_eof"
	case strings.Contains(message, "connection reset"):
		return rawstore.Failed, "connection_reset"
	case strings.Contains(message, "connection refused"):
		return rawstore.Failed, "connection_refused"
	case strings.Contains(message, "no route to host"), strings.Contains(message, "network is unreachable"):
		return rawstore.Failed, "network_unreachable"
	}
	if errors.As(err, &networkError) {
		return rawstore.Failed, "network"
	}
	return rawstore.Failed, "other"
}

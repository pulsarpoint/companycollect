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
}

func (downloader *Downloader) downloadChunk(ctx context.Context, worklist worklistData, plan chunkPlan) (rawstore.CommittedChunkManifest, error) {
	startedAt := time.Now().UTC()
	downloads := make([]recordDownload, len(plan.Records))
	var group errgroup.Group
	group.SetLimit(downloader.Config.Concurrency)
	for index := range plan.Records {
		index := index
		group.Go(func() error {
			record := plan.Records[index]
			recordContext, cancel := context.WithTimeout(ctx, downloader.Config.RecordTimeout)
			defer cancel()
			raw, err := fetch.FetchRawRecord(
				recordContext,
				downloader.Source,
				downloader.Config.SourceBucket,
				record.WARCFilename,
				record.WARCRecordOffset,
				record.WARCRecordLength,
			)
			if err == nil {
				downloads[index] = recordDownload{raw: raw, status: rawstore.Downloaded}
				return nil
			}
			status, errorCode := classifyDownloadError(err)
			downloads[index] = recordDownload{status: status, errorCode: errorCode}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "wait for record downloads")
	}
	if err := ctx.Err(); err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "download chunk context")
	}

	var failed int64
	for _, download := range downloads {
		if download.status != rawstore.Downloaded {
			failed++
		}
	}
	if failed == int64(len(downloads)) {
		return rawstore.CommittedChunkManifest{}, errors.New("all records in chunk failed")
	}
	allowedFailures := int64(math.Ceil(downloader.Config.MaxFailureRate * float64(len(downloads))))
	if failed > allowedFailures {
		return rawstore.CommittedChunkManifest{}, errors.Newf("failed records %d exceeds allowed %d at failure-rate limit %.4f", failed, allowedFailures, downloader.Config.MaxFailureRate)
	}

	tempDir, err := os.MkdirTemp(downloader.Config.TempDir, "cc-download-chunk-")
	if err != nil {
		return rawstore.CommittedChunkManifest{}, errors.Wrap(err, "create chunk temp directory")
	}
	defer os.RemoveAll(tempDir)
	packPath := filepath.Join(tempDir, "records.pack")
	indexPath := filepath.Join(tempDir, "index.parquet")
	indexRows, results, err := writePackAndIndex(packPath, indexPath, plan, downloads)
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

func writePackAndIndex(packPath, indexPath string, plan chunkPlan, downloads []recordDownload) ([]rawstore.IndexRow, rawstore.DownloadResults, error) {
	pack, err := os.Create(packPath)
	if err != nil {
		return nil, rawstore.DownloadResults{}, errors.Wrap(err, "create raw pack")
	}
	indexRows := make([]rawstore.IndexRow, 0, len(plan.Records))
	results := rawstore.DownloadResults{RequestedRecords: int64(len(plan.Records))}
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
			DownloadAttempts: 1,
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
			results.DownloadedRecords++
		} else {
			errorCode := download.errorCode
			row.ErrorCode = &errorCode
			results.FailedRecords++
			switch download.status {
			case rawstore.NotFound:
				results.Errors.NotFound++
			case rawstore.Failed:
				if download.errorCode == "timeout" {
					results.Errors.Timeout++
				} else {
					results.Errors.Other++
				}
			}
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
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return rawstore.Failed, "timeout"
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "404", "NoSuchKey", "NotFound":
			return rawstore.NotFound, "not_found"
		}
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "http 404") || strings.Contains(message, "status code: 404") || strings.Contains(message, "no such key") {
		return rawstore.NotFound, "not_found"
	}
	return rawstore.Failed, "other"
}

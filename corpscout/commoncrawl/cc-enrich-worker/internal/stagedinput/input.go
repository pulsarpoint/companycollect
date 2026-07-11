package stagedinput

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"cc-enrich-worker/internal/model"
	"cc-raw/rawstate"
	"cc-raw/rawstore"
	"github.com/cockroachdb/errors"
	"github.com/parquet-go/parquet-go"
)

type sourceRange struct {
	key        string
	start, end int64
}

type packLocation struct {
	packKey string
	offset  int64
}

type LocalPacks struct {
	files   map[string]*os.File
	records map[sourceRange]packLocation
}

type Input struct {
	Items            []model.WorklistItem
	Getter           *LocalPacks
	RequestedRecords int64
	FailedRecords    int64
	RawBytes         int64
	ChunkCount       int
	cacheDirectory   string
}

func Open(ctx context.Context, store *rawstore.Store, crawlID, selection string, part int, cacheDirectory string) (*Input, error) {
	if store == nil {
		return nil, errors.New("RustFS store is required")
	}
	if err := rawstore.ValidatePartIdentity(crawlID, selection, part); err != nil {
		return nil, err
	}
	if err := os.RemoveAll(cacheDirectory); err != nil {
		return nil, errors.Wrap(err, "clear RustFS input cache")
	}
	if err := os.MkdirAll(cacheDirectory, 0o755); err != nil {
		return nil, errors.Wrap(err, "create RustFS input cache")
	}
	input := &Input{
		Getter:         &LocalPacks{files: make(map[string]*os.File), records: make(map[sourceRange]packLocation)},
		cacheDirectory: cacheDirectory,
	}
	cleanupOnError := func(err error) (*Input, error) {
		_ = input.Close()
		return nil, err
	}

	readyKey, err := rawstate.DownloadReadyKey(crawlID, selection, part)
	if err != nil {
		return cleanupOnError(err)
	}
	readyBody, err := store.ReadBytes(ctx, readyKey)
	if err != nil {
		return cleanupOnError(errors.Wrap(err, "read download ready manifest"))
	}
	ready, err := rawstore.DecodeReadyManifest(readyBody)
	if err != nil {
		return cleanupOnError(errors.Wrap(err, "decode download ready manifest"))
	}
	if err := ready.Validate(); err != nil {
		return cleanupOnError(errors.Wrap(err, "validate download ready manifest"))
	}
	if ready.CrawlID != crawlID || ready.Selection != selection || ready.Part != part {
		return cleanupOnError(errors.New("download ready manifest identifies a different part"))
	}

	input.RequestedRecords = ready.Totals.RequestedRecords
	input.FailedRecords = ready.Totals.FailedRecords
	input.RawBytes = ready.Totals.RawBytes
	input.ChunkCount = ready.Totals.ChunkCount
	committed := make([]rawstore.CommittedChunkManifest, 0, len(ready.Chunks))
	for _, readyChunk := range ready.Chunks {
		manifestBody, err := store.ReadBytes(ctx, readyChunk.ManifestKey)
		if err != nil {
			return cleanupOnError(errors.Wrapf(err, "read chunk %d manifest", readyChunk.Chunk))
		}
		if rawstore.ChecksumBytes(manifestBody) != readyChunk.ManifestSHA256 {
			return cleanupOnError(errors.Newf("chunk %d manifest checksum does not match ready manifest", readyChunk.Chunk))
		}
		manifest, err := rawstore.DecodeChunkManifest(manifestBody)
		if err != nil {
			return cleanupOnError(errors.Wrapf(err, "decode chunk %d manifest", readyChunk.Chunk))
		}

		indexBody, err := store.ReadBytes(ctx, manifest.Index.Key)
		if err != nil {
			return cleanupOnError(errors.Wrapf(err, "read chunk %d index", readyChunk.Chunk))
		}
		if int64(len(indexBody)) != manifest.Index.SizeBytes || rawstore.ChecksumBytes(indexBody) != manifest.Index.SHA256 {
			return cleanupOnError(errors.Newf("chunk %d index failed size/checksum validation", readyChunk.Chunk))
		}
		rows, err := parquet.Read[rawstore.IndexRow](bytes.NewReader(indexBody), int64(len(indexBody)))
		if err != nil {
			return cleanupOnError(errors.Wrapf(err, "decode chunk %d index", readyChunk.Chunk))
		}
		if err := rawstore.ValidateIndexRows(rows, manifest); err != nil {
			return cleanupOnError(errors.Wrapf(err, "validate chunk %d index", readyChunk.Chunk))
		}

		packPath := filepath.Join(cacheDirectory, fmt.Sprintf("chunk_%06d.pack", readyChunk.Chunk))
		if err := store.DownloadFile(ctx, manifest.Pack, packPath); err != nil {
			return cleanupOnError(errors.Wrapf(err, "cache chunk %d pack", readyChunk.Chunk))
		}
		pack, err := os.Open(packPath)
		if err != nil {
			return cleanupOnError(errors.Wrapf(err, "open cached chunk %d pack", readyChunk.Chunk))
		}
		input.Getter.files[manifest.Pack.Key] = pack
		for _, row := range rows {
			if row.DownloadStatus != rawstore.Downloaded {
				continue
			}
			rangeKey := sourceRange{key: row.WARCFilename, start: row.WARCOffset, end: row.WARCOffset + row.WARCLength - 1}
			input.Getter.records[rangeKey] = packLocation{packKey: manifest.Pack.Key, offset: *row.PackOffset}
			input.Items = append(input.Items, model.WorklistItem{
				RootDomain:   row.RootDomain,
				URL:          row.URL,
				WarcFilename: row.WARCFilename,
				Offset:       row.WARCOffset,
				Length:       row.WARCLength,
				Primary:      row.IsPrimary,
			})
		}
		committed = append(committed, rawstore.CommittedChunkManifest{
			Manifest:          manifest,
			ManifestSHA256:    readyChunk.ManifestSHA256,
			ManifestSizeBytes: int64(len(manifestBody)),
		})
		slog.InfoContext(ctx, "RustFS input pack cached",
			"crawl", crawlID,
			"selection", selection,
			"part", part,
			"chunk", readyChunk.Chunk,
			"chunks", len(ready.Chunks),
			"pack_bytes", manifest.Pack.SizeBytes,
		)
	}
	if err := ready.ValidateCommittedChunks(committed); err != nil {
		return cleanupOnError(errors.Wrap(err, "validate ready manifest against chunks"))
	}
	if len(input.Items) == 0 {
		return cleanupOnError(errors.New("RustFS part contains no downloaded records"))
	}
	return input, nil
}

func (packs *LocalPacks) GetRange(ctx context.Context, _ string, key string, start, end int64) ([]byte, error) {
	if end < start {
		return nil, errors.Newf("invalid cached record range %d-%d", start, end)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	location, exists := packs.records[sourceRange{key: key, start: start, end: end}]
	if !exists {
		return nil, errors.Newf("record range not found in RustFS part: key=%s offset=%d length=%d", key, start, end-start+1)
	}
	pack, exists := packs.files[location.packKey]
	if !exists {
		return nil, errors.Newf("cached RustFS pack %s is not open", location.packKey)
	}
	length := end - start + 1
	if length > int64(int(^uint(0)>>1)) {
		return nil, errors.Newf("record range length %d exceeds platform capacity", length)
	}
	body := make([]byte, int(length))
	read, err := pack.ReadAt(body, location.offset)
	if err != nil {
		return nil, errors.Wrapf(err, "read cached RustFS record key=%s offset=%d length=%d", key, start, length)
	}
	if read != len(body) {
		return nil, errors.Newf("short cached RustFS record key=%s: read %d bytes, want %d", key, read, len(body))
	}
	return body, nil
}

func (input *Input) Close() error {
	var closeErr error
	if input.Getter != nil {
		for key, file := range input.Getter.files {
			if err := file.Close(); err != nil && closeErr == nil {
				closeErr = errors.Wrapf(err, "close cached RustFS pack %s", key)
			}
		}
	}
	if err := os.RemoveAll(input.cacheDirectory); err != nil && closeErr == nil {
		closeErr = errors.Wrap(err, "remove RustFS input cache")
	}
	return closeErr
}

package rawstore

import (
	"encoding/hex"
	"strings"

	"github.com/cockroachdb/errors"
)

func (checksum SHA256) Validate() error {
	if len(checksum) != 64 {
		return errors.Newf("SHA-256 must contain 64 hexadecimal characters, got %d", len(checksum))
	}
	if _, err := hex.DecodeString(string(checksum)); err != nil {
		return errors.Wrap(err, "decode SHA-256")
	}
	return nil
}

func (row IndexRow) Validate() error {
	switch {
	case row.WorklistOrdinal < 0:
		return errors.Newf("worklist ordinal must be non-negative, got %d", row.WorklistOrdinal)
	case row.DomainRank < 0:
		return errors.Newf("domain rank must be non-negative, got %d", row.DomainRank)
	case strings.TrimSpace(row.RootDomain) == "":
		return errors.New("root domain is required")
	case strings.TrimSpace(row.URL) == "":
		return errors.New("URL is required")
	case strings.TrimSpace(row.WARCFilename) == "":
		return errors.New("WARC filename is required")
	case row.WARCOffset < 0:
		return errors.Newf("WARC offset must be non-negative, got %d", row.WARCOffset)
	case row.WARCLength <= 0:
		return errors.Newf("WARC length must be positive, got %d", row.WARCLength)
	case row.DownloadAttempts <= 0:
		return errors.Newf("download attempts must be positive, got %d", row.DownloadAttempts)
	}

	switch row.DownloadStatus {
	case Downloaded:
		if row.PackOffset == nil || row.PackLength == nil {
			return errors.New("downloaded row requires pack offset and length")
		}
		if *row.PackOffset < 0 || *row.PackLength <= 0 {
			return errors.Newf("invalid pack range offset=%d length=%d", *row.PackOffset, *row.PackLength)
		}
		if row.ErrorCode != nil {
			return errors.New("downloaded row cannot contain an error code")
		}
		if row.RecordChecksum != nil {
			if err := SHA256(*row.RecordChecksum).Validate(); err != nil {
				return errors.Wrap(err, "record checksum")
			}
		}
	case NotFound, Failed:
		if row.PackOffset != nil || row.PackLength != nil {
			return errors.Newf("%s row cannot contain pack coordinates", row.DownloadStatus)
		}
		if row.RecordChecksum != nil {
			return errors.Newf("%s row cannot contain a record checksum", row.DownloadStatus)
		}
		if row.ErrorCode == nil || strings.TrimSpace(*row.ErrorCode) == "" {
			return errors.Newf("%s row requires an error code", row.DownloadStatus)
		}
	default:
		return errors.Newf("invalid download status %q", row.DownloadStatus)
	}
	return nil
}

func ValidateIndexRows(rows []IndexRow, manifest ChunkManifest) error {
	if err := manifest.Validate(); err != nil {
		return errors.Wrap(err, "chunk manifest")
	}
	if int64(len(rows)) != manifest.Worklist.RecordCount {
		return errors.Newf("index row count is %d; want %d", len(rows), manifest.Worklist.RecordCount)
	}

	var nextPackOffset, downloaded, failed, notFound, timeout, other int64
	for i, row := range rows {
		if err := row.Validate(); err != nil {
			return errors.Wrapf(err, "index row %d", i)
		}
		expectedOrdinal := manifest.Worklist.FirstOrdinal + int64(i)
		if row.WorklistOrdinal != expectedOrdinal {
			return errors.Newf("index row %d has ordinal %d; want %d", i, row.WorklistOrdinal, expectedOrdinal)
		}
		switch row.DownloadStatus {
		case Downloaded:
			if *row.PackOffset != nextPackOffset {
				return errors.Newf("index row %d pack offset is %d; want %d", i, *row.PackOffset, nextPackOffset)
			}
			if *row.PackLength != row.WARCLength {
				return errors.Newf("index row %d pack length is %d; want WARC length %d", i, *row.PackLength, row.WARCLength)
			}
			nextPackOffset += *row.PackLength
			downloaded++
		case NotFound:
			notFound++
			failed++
		case Failed:
			if *row.ErrorCode == "timeout" {
				timeout++
			} else {
				other++
			}
			failed++
		}
	}

	results := manifest.Results
	switch {
	case nextPackOffset != manifest.Pack.SizeBytes:
		return errors.Newf("indexed pack bytes is %d; want %d", nextPackOffset, manifest.Pack.SizeBytes)
	case downloaded != results.DownloadedRecords || failed != results.FailedRecords:
		return errors.New("index download counts do not match chunk manifest")
	case notFound != results.Errors.NotFound || timeout != results.Errors.Timeout || other != results.Errors.Other:
		return errors.New("index error counts do not match chunk manifest")
	}
	return nil
}

func (manifest ChunkManifest) Validate() error {
	if manifest.SchemaVersion != SchemaVersion {
		return errors.Newf("unsupported chunk manifest schema version %d", manifest.SchemaVersion)
	}
	if err := ValidatePartIdentity(manifest.CrawlID, manifest.Selection, manifest.Part); err != nil {
		return errors.Wrap(err, "chunk manifest identity")
	}
	keys, err := KeysForChunk(manifest.CrawlID, manifest.Selection, manifest.Part, manifest.Chunk)
	if err != nil {
		return errors.Wrap(err, "chunk manifest keys")
	}
	if strings.TrimSpace(manifest.Worklist.Key) == "" {
		return errors.New("worklist key is required")
	}
	if err := manifest.Worklist.SHA256.Validate(); err != nil {
		return errors.Wrap(err, "worklist SHA-256")
	}
	if manifest.Worklist.FirstOrdinal < 0 || manifest.Worklist.RecordCount <= 0 {
		return errors.Newf("invalid worklist range first=%d count=%d", manifest.Worklist.FirstOrdinal, manifest.Worklist.RecordCount)
	}
	if err := validateObject(manifest.Pack, keys.Pack, "pack"); err != nil {
		return err
	}
	if err := validateObject(manifest.Index, keys.Index, "index"); err != nil {
		return err
	}
	if err := validateDownloadResults(manifest.Results, manifest.Worklist.RecordCount, manifest.Pack.SizeBytes); err != nil {
		return err
	}
	if err := validateDownloadRun(manifest.Download); err != nil {
		return err
	}
	return nil
}

func (ready ReadyManifest) Validate() error {
	if ready.SchemaVersion != SchemaVersion {
		return errors.Newf("unsupported ready manifest schema version %d", ready.SchemaVersion)
	}
	if err := ValidatePartIdentity(ready.CrawlID, ready.Selection, ready.Part); err != nil {
		return errors.Wrap(err, "ready manifest identity")
	}
	if strings.TrimSpace(ready.Worklist.Key) == "" {
		return errors.New("worklist key is required")
	}
	if ready.Worklist.SizeBytes <= 0 || ready.Worklist.RecordCount <= 0 {
		return errors.Newf("invalid ready worklist size=%d records=%d", ready.Worklist.SizeBytes, ready.Worklist.RecordCount)
	}
	if err := ready.Worklist.SHA256.Validate(); err != nil {
		return errors.Wrap(err, "worklist SHA-256")
	}
	if len(ready.Chunks) == 0 {
		return errors.New("ready manifest requires at least one chunk")
	}

	var records, rawBytes int64
	var nextOrdinal int64
	for i, chunk := range ready.Chunks {
		if chunk.Chunk != i {
			return errors.Newf("chunk at index %d has ID %d", i, chunk.Chunk)
		}
		if chunk.FirstOrdinal != nextOrdinal || chunk.RecordCount <= 0 {
			return errors.Newf("chunk %d has invalid ordinal range first=%d count=%d; want first=%d", chunk.Chunk, chunk.FirstOrdinal, chunk.RecordCount, nextOrdinal)
		}
		if chunk.RawBytes <= 0 {
			return errors.Newf("chunk %d raw bytes must be positive, got %d", chunk.Chunk, chunk.RawBytes)
		}
		keys, err := KeysForChunk(ready.CrawlID, ready.Selection, ready.Part, chunk.Chunk)
		if err != nil {
			return errors.Wrapf(err, "chunk %d key", chunk.Chunk)
		}
		if chunk.ManifestKey != keys.Manifest {
			return errors.Newf("chunk %d manifest key %q does not match %q", chunk.Chunk, chunk.ManifestKey, keys.Manifest)
		}
		if err := chunk.ManifestSHA256.Validate(); err != nil {
			return errors.Wrapf(err, "chunk %d manifest SHA-256", chunk.Chunk)
		}
		nextOrdinal += chunk.RecordCount
		records += chunk.RecordCount
		rawBytes += chunk.RawBytes
	}

	switch {
	case ready.Totals.ChunkCount != len(ready.Chunks):
		return errors.Newf("chunk total is %d; want %d", ready.Totals.ChunkCount, len(ready.Chunks))
	case ready.Totals.RequestedRecords != records:
		return errors.Newf("requested record total is %d; want %d", ready.Totals.RequestedRecords, records)
	case ready.Worklist.RecordCount != records:
		return errors.Newf("worklist record count is %d; want %d", ready.Worklist.RecordCount, records)
	case ready.Totals.DownloadedRecords < 0 || ready.Totals.FailedRecords < 0:
		return errors.New("ready record totals cannot be negative")
	case ready.Totals.DownloadedRecords+ready.Totals.FailedRecords != records:
		return errors.Newf("downloaded plus failed records is %d; want %d", ready.Totals.DownloadedRecords+ready.Totals.FailedRecords, records)
	case ready.Totals.RawBytes != rawBytes:
		return errors.Newf("raw byte total is %d; want %d", ready.Totals.RawBytes, rawBytes)
	case strings.TrimSpace(ready.DownloadRunID) == "":
		return errors.New("download run ID is required")
	case ready.CompletedAt.IsZero():
		return errors.New("ready completion time is required")
	}
	return nil
}

func (ready ReadyManifest) ValidateCommittedChunks(committed []CommittedChunkManifest) error {
	if err := ready.Validate(); err != nil {
		return err
	}
	if len(committed) != len(ready.Chunks) {
		return errors.Newf("committed manifest count is %d; want %d", len(committed), len(ready.Chunks))
	}

	var downloaded, failed int64
	for i, committedChunk := range committed {
		manifest := committedChunk.Manifest
		readyChunk := ready.Chunks[i]
		if err := manifest.Validate(); err != nil {
			return errors.Wrapf(err, "committed chunk %d", i)
		}
		if committedChunk.ManifestSizeBytes <= 0 {
			return errors.Newf("committed chunk %d manifest size must be positive", i)
		}
		if err := committedChunk.ManifestSHA256.Validate(); err != nil {
			return errors.Wrapf(err, "committed chunk %d manifest SHA-256", i)
		}
		if manifest.CrawlID != ready.CrawlID || manifest.Selection != ready.Selection || manifest.Part != ready.Part || manifest.Chunk != readyChunk.Chunk {
			return errors.Newf("committed chunk %d identifies a different raw chunk", i)
		}
		if manifest.Worklist.Key != ready.Worklist.Key || manifest.Worklist.SHA256 != ready.Worklist.SHA256 {
			return errors.Newf("committed chunk %d references a different worklist", i)
		}
		if manifest.Worklist.FirstOrdinal != readyChunk.FirstOrdinal || manifest.Worklist.RecordCount != readyChunk.RecordCount {
			return errors.Newf("committed chunk %d worklist range does not match ready manifest", i)
		}
		if committedChunk.ManifestSHA256 != readyChunk.ManifestSHA256 {
			return errors.Newf("committed chunk %d checksum does not match ready manifest", i)
		}
		rawBytes := manifest.Pack.SizeBytes + manifest.Index.SizeBytes + committedChunk.ManifestSizeBytes
		if readyChunk.RawBytes != rawBytes {
			return errors.Newf("committed chunk %d raw bytes is %d; want %d", i, readyChunk.RawBytes, rawBytes)
		}
		downloaded += manifest.Results.DownloadedRecords
		failed += manifest.Results.FailedRecords
	}
	if ready.Totals.DownloadedRecords != downloaded || ready.Totals.FailedRecords != failed {
		return errors.New("ready record totals do not match committed chunk manifests")
	}
	return nil
}

func validateObject(object ObjectDescriptor, expectedKey, name string) error {
	if object.Key != expectedKey {
		return errors.Newf("%s key %q does not match %q", name, object.Key, expectedKey)
	}
	if object.SizeBytes <= 0 {
		return errors.Newf("%s size must be positive, got %d", name, object.SizeBytes)
	}
	if err := object.SHA256.Validate(); err != nil {
		return errors.Wrapf(err, "%s SHA-256", name)
	}
	return nil
}

func validateDownloadResults(results DownloadResults, expectedRecords, expectedPackBytes int64) error {
	if results.RequestedRecords != expectedRecords {
		return errors.Newf("requested records is %d; want %d", results.RequestedRecords, expectedRecords)
	}
	if results.DownloadedRecords < 0 || results.FailedRecords < 0 || results.DownloadedRecords+results.FailedRecords != results.RequestedRecords {
		return errors.New("downloaded and failed record counts do not cover requested records")
	}
	if results.Errors.NotFound < 0 || results.Errors.Timeout < 0 || results.Errors.Other < 0 {
		return errors.New("download error counts cannot be negative")
	}
	if results.Errors.NotFound+results.Errors.Timeout+results.Errors.Other != results.FailedRecords {
		return errors.New("download error counts do not cover failed records")
	}
	if results.SourceBytes <= 0 || results.PackedBytes <= 0 || results.SourceBytes != results.PackedBytes {
		return errors.Newf("source and packed bytes must be equal and positive, got source=%d packed=%d", results.SourceBytes, results.PackedBytes)
	}
	if results.PackedBytes != expectedPackBytes {
		return errors.Newf("packed bytes is %d; want pack size %d", results.PackedBytes, expectedPackBytes)
	}
	return nil
}

func validateDownloadRun(run DownloadRun) error {
	switch {
	case strings.TrimSpace(run.RunID) == "":
		return errors.New("download run ID is required")
	case strings.TrimSpace(run.WorkerHost) == "":
		return errors.New("download worker host is required")
	case strings.TrimSpace(run.GitCommit) == "":
		return errors.New("download git commit is required")
	case run.StartedAt.IsZero() || run.CompletedAt.IsZero():
		return errors.New("download start and completion times are required")
	case run.CompletedAt.Before(run.StartedAt):
		return errors.New("download completion time precedes start time")
	}
	return nil
}

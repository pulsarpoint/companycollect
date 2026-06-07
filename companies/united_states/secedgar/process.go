package secedgar

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error) {
	if s == nil {
		return countryimport.ProcessResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil SEC EDGAR source"),
		)
	}

	snapshotPath, err := s.resolveProcessSnapshotPath(opts)
	if err != nil {
		return countryimport.ProcessResult{}, err
	}

	startedAt := time.Now().UTC()
	result := countryimport.ProcessResult{
		SourceSlug:   SourceSlug,
		SnapshotPath: snapshotPath,
		StartedAt:    startedAt,
	}
	if err := ctx.Err(); err != nil {
		return result, secContextError(err, snapshotPath)
	}

	payload, err := os.ReadFile(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return result, countryimport.WrapSourceError(
				countryimport.ErrorKindNoSnapshot,
				SourceSlug,
				"",
				snapshotPath,
				0,
				errors.Wrap(err, "read SEC EDGAR snapshot"),
			)
		}
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "read SEC EDGAR snapshot"),
		)
	}
	if err := ctx.Err(); err != nil {
		return result, secContextError(err, snapshotPath)
	}

	records, err := DecodeCompanyTickers(payload)
	if err != nil {
		result.DecodeErrors = 1
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindRemoteDecode,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "decode SEC EDGAR snapshot"),
		)
	}
	result.RecordsSeen = int64(len(records))
	if opts.Limit > 0 && int64(len(records)) > opts.Limit {
		records = records[:opts.Limit]
	}

	chunkSize := resolveInt(opts.ChunkSize, countryimport.DefaultChunkSize)
	for start := 0; start < len(records); start += chunkSize {
		if err := ctx.Err(); err != nil {
			return result, secContextError(err, snapshotPath)
		}
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}
		chunk := records[start:end]
		storeResult, err := s.Store(ctx, chunk)
		if err != nil {
			return result, countryimport.WrapSourceError(
				countryimport.Classify(err),
				SourceSlug,
				"",
				snapshotPath,
				0,
				errors.Wrap(err, "store SEC EDGAR records"),
			)
		}
		result.RecordsProcessed += int64(len(chunk))
		result.RecordsStored += storeResult.RecordsStored
		result.ChunksProcessed++
	}
	if err := ctx.Err(); err != nil {
		return result, secContextError(err, snapshotPath)
	}

	finishedAt := time.Now().UTC()
	result.FinishedAt = finishedAt
	result.Duration = finishedAt.Sub(startedAt)

	metadata := countryimport.ProcessMetadata{
		SourceSlug:       result.SourceSlug,
		SnapshotPath:     result.SnapshotPath,
		StartedAt:        result.StartedAt,
		FinishedAt:       result.FinishedAt,
		DurationMS:       result.Duration.Milliseconds(),
		RecordsSeen:      result.RecordsSeen,
		RecordsProcessed: result.RecordsProcessed,
		RecordsStored:    result.RecordsStored,
		DecodeErrors:     result.DecodeErrors,
		ChunksProcessed:  result.ChunksProcessed,
	}
	if err := s.saveProcessMetadata(ctx, metadata); err != nil {
		return result, err
	}

	return result, nil
}

func secContextError(err error, path string) error {
	return countryimport.WrapSourceError(
		countryimport.Classify(err),
		SourceSlug,
		"",
		path,
		0,
		errors.Wrap(err, "process SEC EDGAR snapshot"),
	)
}

func (s *Source) resolveProcessSnapshotPath(opts countryimport.ProcessOptions) (string, error) {
	if snapshotPath := strings.TrimSpace(opts.SnapshotPath); snapshotPath != "" {
		return snapshotPath, nil
	}
	if s.latestDownload != nil {
		if snapshotPath := strings.TrimSpace(s.latestDownload.SnapshotPath); snapshotPath != "" {
			return snapshotPath, nil
		}
	}

	dataDir := resolveString(opts.DataDir, s.cfg.DataDir, defaultDataDir)
	return latestSnapshotPath(filepath.Join(dataDir, "snapshots"))
}

func latestSnapshotPath(snapshotsDir string) (string, error) {
	entries, err := os.ReadDir(snapshotsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", countryimport.WrapSourceError(
				countryimport.ErrorKindNoSnapshot,
				SourceSlug,
				"",
				snapshotsDir,
				0,
				errors.Wrap(err, "find SEC EDGAR snapshot"),
			)
		}
		return "", countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotsDir,
			0,
			errors.Wrap(err, "read SEC EDGAR snapshots directory"),
		)
	}

	var latestPath string
	var latestModTime time.Time
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return "", countryimport.WrapSourceError(
				countryimport.ErrorKindFileIO,
				SourceSlug,
				"",
				filepath.Join(snapshotsDir, entry.Name()),
				0,
				errors.Wrap(err, "stat SEC EDGAR snapshot"),
			)
		}
		path := filepath.Join(snapshotsDir, entry.Name())
		if latestPath == "" || info.ModTime().After(latestModTime) ||
			(info.ModTime().Equal(latestModTime) && path > latestPath) {
			latestPath = path
			latestModTime = info.ModTime()
		}
	}
	if latestPath == "" {
		return "", countryimport.WrapSourceError(
			countryimport.ErrorKindNoSnapshot,
			SourceSlug,
			"",
			snapshotsDir,
			0,
			errors.New("no SEC EDGAR snapshot found"),
		)
	}

	return latestPath, nil
}

func (s *Source) saveProcessMetadata(ctx context.Context, metadata countryimport.ProcessMetadata) error {
	s.latestProcess = &metadata
	metadataStore := s.metadataStore
	if metadataStore == nil {
		metadataStore = countryimport.NoopMetadataStore{}
	}
	if err := metadataStore.SaveProcess(ctx, metadata); err != nil {
		return countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			metadata.SnapshotPath,
			0,
			errors.Wrap(err, "save SEC EDGAR process metadata"),
		)
	}
	return nil
}

func resolveInt(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

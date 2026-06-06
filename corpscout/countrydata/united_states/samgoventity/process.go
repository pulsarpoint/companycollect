package samgoventity

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

const maxSnapshotLineBytes = 8 * 1024 * 1024

func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error) {
	if s == nil {
		return countryimport.ProcessResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState,
			SourceSlug,
			"",
			"",
			0,
			errors.New("nil SAM.gov entity source"),
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

	file, err := os.Open(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return result, countryimport.WrapSourceError(
				countryimport.ErrorKindNoSnapshot,
				SourceSlug,
				"",
				snapshotPath,
				0,
				errors.Wrap(err, "open SAM snapshot"),
			)
		}
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "open SAM snapshot"),
		)
	}
	defer file.Close()

	chunkSize := resolveInt(opts.ChunkSize, countryimport.DefaultChunkSize)
	records := make([]SamEntityRecord, 0, chunkSize)
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		storeResult, err := s.Store(ctx, records)
		if err != nil {
			return countryimport.WrapSourceError(
				countryimport.ErrorKindState,
				SourceSlug,
				"",
				snapshotPath,
				0,
				errors.Wrap(err, "store SAM records"),
			)
		}
		result.RecordsStored += storeResult.RecordsStored
		result.ChunksProcessed++
		records = records[:0]
		return nil
	}

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSnapshotLineBytes)

	var lineNumber int64
	for {
		if err := ctx.Err(); err != nil {
			return result, processContextError(err, snapshotPath)
		}
		if !scanner.Scan() {
			break
		}
		lineNumber++
		result.RecordsSeen++

		rawLine := append([]byte(nil), scanner.Bytes()...)
		var record SamEntityRecord
		if err := json.Unmarshal(rawLine, &record); err != nil {
			result.DecodeErrors++
			slog.WarnContext(ctx, "decode SAM.gov entity snapshot line",
				"source", SourceSlug,
				"line", lineNumber,
				"error", err,
			)
			continue
		}
		record.RawPayload = rawLine
		payloadHash := sha256.Sum256(rawLine)
		record.PayloadHash = hex.EncodeToString(payloadHash[:])

		records = append(records, record)
		result.RecordsProcessed++
		if len(records) >= chunkSize {
			if err := flush(); err != nil {
				return result, err
			}
		}
		if opts.Limit > 0 && result.RecordsProcessed >= opts.Limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotPath,
			0,
			errors.Wrap(err, "scan SAM snapshot"),
		)
	}
	if err := ctx.Err(); err != nil {
		return result, processContextError(err, snapshotPath)
	}
	if err := flush(); err != nil {
		return result, err
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

func processContextError(err error, snapshotPath string) error {
	return countryimport.WrapSourceError(
		countryimport.Classify(err),
		SourceSlug,
		"",
		snapshotPath,
		0,
		errors.Wrap(err, "process SAM snapshot"),
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
				errors.Wrap(err, "find SAM snapshot"),
			)
		}
		return "", countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO,
			SourceSlug,
			"",
			snapshotsDir,
			0,
			errors.Wrap(err, "read snapshots directory"),
		)
	}

	var latestPath string
	var latestModTime time.Time
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".ndjson") {
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
				errors.Wrap(err, "stat SAM snapshot"),
			)
		}
		if latestPath == "" || info.ModTime().After(latestModTime) {
			latestPath = filepath.Join(snapshotsDir, entry.Name())
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
			errors.New("no SAM snapshot found"),
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
			errors.Wrap(err, "save process metadata"),
		)
	}
	return nil
}

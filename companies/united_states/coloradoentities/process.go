package coloradoentities

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
	countryimport "github.com/pulsarpoint/companycollect/companies/common/countryimport"
)

const maxSnapshotLineBytes = 8 * 1024 * 1024

// Process streams the NDJSON snapshot, decodes each line into a source-native
// record, and flushes records to Store in chunks. Malformed lines are warned
// and skipped; a missing snapshot fails with ErrorKindNoSnapshot.
func (s *Source) Process(ctx context.Context, opts countryimport.ProcessOptions) (countryimport.ProcessResult, error) {
	if s == nil {
		return countryimport.ProcessResult{}, countryimport.WrapSourceError(
			countryimport.ErrorKindState, SourceSlug, "", "", 0,
			errors.New("nil Colorado entities source"),
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
				countryimport.ErrorKindNoSnapshot, SourceSlug, "", snapshotPath, 0,
				errors.Wrap(err, "open Colorado snapshot"),
			)
		}
		return result, countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", snapshotPath, 0,
			errors.Wrap(err, "open Colorado snapshot"),
		)
	}
	defer file.Close()

	chunkSize := resolveInt(opts.ChunkSize, countryimport.DefaultChunkSize)
	records := make([]ColoradoEntityRecord, 0, chunkSize)
	flush := func() error {
		if len(records) == 0 {
			return nil
		}
		storeResult, err := s.Store(ctx, records)
		if err != nil {
			return countryimport.WrapSourceError(
				countryimport.ErrorKindState, SourceSlug, "", snapshotPath, 0,
				errors.Wrap(err, "store Colorado records"),
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
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		lineNumber++
		result.RecordsSeen++

		rawLine := append([]byte(nil), scanner.Bytes()...)
		var record ColoradoEntityRecord
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			result.DecodeErrors++
			slog.WarnContext(ctx, "decode Colorado snapshot line",
				"source", SourceSlug, "line", lineNumber, "error", err)
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
			countryimport.ErrorKindFileIO, SourceSlug, "", snapshotPath, 0,
			errors.Wrap(err, "scan Colorado snapshot"),
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
		countryimport.Classify(err), SourceSlug, "", snapshotPath, 0,
		errors.Wrap(err, "process Colorado snapshot"),
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
				countryimport.ErrorKindNoSnapshot, SourceSlug, "", snapshotsDir, 0,
				errors.Wrap(err, "find Colorado snapshot"),
			)
		}
		return "", countryimport.WrapSourceError(
			countryimport.ErrorKindFileIO, SourceSlug, "", snapshotsDir, 0,
			errors.Wrap(err, "read Colorado snapshots directory"),
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
				countryimport.ErrorKindFileIO, SourceSlug, "", filepath.Join(snapshotsDir, entry.Name()), 0,
				errors.Wrap(err, "stat Colorado snapshot"),
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
			countryimport.ErrorKindNoSnapshot, SourceSlug, "", snapshotsDir, 0,
			errors.New("no Colorado snapshot found"),
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
			countryimport.ErrorKindState, SourceSlug, "", metadata.SnapshotPath, 0,
			errors.Wrap(err, "save Colorado process metadata"),
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

func resolveDuration(override time.Duration, configured time.Duration, fallback time.Duration) time.Duration {
	if override > 0 {
		return override
	}
	if configured > 0 {
		return configured
	}
	return fallback
}

func resolveString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func nilIfEmpty(values map[string]int) map[string]int {
	if len(values) == 0 {
		return nil
	}
	return values
}

package rawstate

import (
	"strings"
	"time"

	"cc-enrich-worker/internal/rawstore"
	"github.com/cockroachdb/errors"
)

func (marker ProcessingMarker) Validate() error {
	if err := validateProcessorMarker(marker.SchemaVersion, marker.CrawlID, marker.Selection, marker.Part, marker.Processor, marker.ProcessingVersion, marker.GitCommit, marker.RunID, marker.WorkerHost); err != nil {
		return err
	}
	if marker.PID <= 0 {
		return errors.Newf("PID must be positive, got %d", marker.PID)
	}
	if err := marker.InputReadySHA256.Validate(); err != nil {
		return errors.Wrap(err, "input ready SHA-256")
	}
	if marker.StartedAt.IsZero() || marker.HeartbeatAt.IsZero() || marker.LeaseExpiresAt.IsZero() {
		return errors.New("processing timestamps are required")
	}
	if marker.HeartbeatAt.Before(marker.StartedAt) {
		return errors.New("heartbeat time precedes start time")
	}
	if !marker.LeaseExpiresAt.After(marker.HeartbeatAt) {
		return errors.New("lease expiry must follow heartbeat time")
	}
	return nil
}

func (marker ProcessingMarker) IsStale(now time.Time) bool {
	return !now.Before(marker.LeaseExpiresAt)
}

func (marker ProcessedMarker) Validate() error {
	if err := validateProcessorMarker(marker.SchemaVersion, marker.CrawlID, marker.Selection, marker.Part, marker.Processor, marker.ProcessingVersion, marker.GitCommit, marker.RunID, marker.WorkerHost); err != nil {
		return err
	}
	if err := marker.InputReadySHA256.Validate(); err != nil {
		return errors.Wrap(err, "input ready SHA-256")
	}
	if marker.StartedAt.IsZero() || marker.CompletedAt.IsZero() || marker.CompletedAt.Before(marker.StartedAt) {
		return errors.New("processed marker has invalid start or completion time")
	}
	if len(marker.InputManifests) == 0 {
		return errors.New("processed marker requires input manifests")
	}
	for i, reference := range marker.InputManifests {
		if reference.Chunk != i {
			return errors.Newf("input manifest at index %d has chunk %d", i, reference.Chunk)
		}
		keys, err := rawstore.KeysForChunk(marker.CrawlID, marker.Selection, marker.Part, reference.Chunk)
		if err != nil {
			return errors.Wrapf(err, "input manifest chunk %d", reference.Chunk)
		}
		if reference.ManifestKey != keys.Manifest {
			return errors.Newf("input manifest chunk %d key %q does not match %q", reference.Chunk, reference.ManifestKey, keys.Manifest)
		}
		if err := reference.ManifestSHA256.Validate(); err != nil {
			return errors.Wrapf(err, "input manifest chunk %d SHA-256", reference.Chunk)
		}
	}
	if err := marker.Counts.Validate(); err != nil {
		return err
	}
	if len(marker.Outputs) == 0 {
		return errors.New("processed marker requires output artifacts")
	}
	names := make(map[string]struct{}, len(marker.Outputs))
	for _, output := range marker.Outputs {
		if strings.TrimSpace(output.Name) == "" || strings.TrimSpace(output.Location) == "" {
			return errors.New("output artifact name and location are required")
		}
		if _, exists := names[output.Name]; exists {
			return errors.Newf("duplicate output artifact %q", output.Name)
		}
		names[output.Name] = struct{}{}
		if output.SizeBytes < 0 || output.RowCount < 0 {
			return errors.Newf("output artifact %q has negative size or row count", output.Name)
		}
		if err := output.SHA256.Validate(); err != nil {
			return errors.Wrapf(err, "output artifact %q SHA-256", output.Name)
		}
	}
	return nil
}

func (counts ProcessedCounts) Validate() error {
	if counts.InputRecords < 0 || counts.DownloadedRecords < 0 || counts.FailedRecords < 0 || counts.ProcessedRecords < 0 || counts.SkippedRecords < 0 {
		return errors.New("processed record counts cannot be negative")
	}
	if counts.DownloadedRecords+counts.FailedRecords != counts.InputRecords {
		return errors.New("downloaded and failed records do not cover input records")
	}
	if counts.ProcessedRecords+counts.SkippedRecords != counts.DownloadedRecords {
		return errors.New("processed and skipped records do not cover downloaded records")
	}
	return nil
}

func (marker LoadedMarker) Validate() error {
	if err := validateProcessorMarker(marker.SchemaVersion, marker.CrawlID, marker.Selection, marker.Part, marker.Processor, marker.ProcessingVersion, marker.GitCommit, marker.RunID, marker.WorkerHost); err != nil {
		return err
	}
	if strings.TrimSpace(marker.SourceRunID) == "" {
		return errors.New("source run ID is required")
	}
	if err := marker.ProcessedSHA256.Validate(); err != nil {
		return errors.Wrap(err, "processed SHA-256")
	}
	if strings.TrimSpace(marker.Destination) == "" {
		return errors.New("load destination is required")
	}
	if marker.CommittedRows < 0 || marker.CommittedObjects < 0 {
		return errors.New("committed rows and objects cannot be negative")
	}
	if marker.StartedAt.IsZero() || marker.CompletedAt.IsZero() || marker.CompletedAt.Before(marker.StartedAt) {
		return errors.New("loaded marker has invalid start or completion time")
	}
	return nil
}

func (marker ReclaimedMarker) Validate() error {
	if marker.SchemaVersion != SchemaVersion {
		return errors.Newf("unsupported reclaimed marker schema version %d", marker.SchemaVersion)
	}
	if err := rawstore.ValidatePartIdentity(marker.CrawlID, marker.Selection, marker.Part); err != nil {
		return errors.Wrap(err, "reclaimed marker identity")
	}
	if err := marker.InputReadySHA256.Validate(); err != nil {
		return errors.Wrap(err, "input ready SHA-256")
	}
	if marker.DeletedObjectCount <= 0 || marker.DeletedBytes <= 0 {
		return errors.Newf("deleted object count and bytes must be positive, got objects=%d bytes=%d", marker.DeletedObjectCount, marker.DeletedBytes)
	}
	if strings.TrimSpace(marker.RunID) == "" || strings.TrimSpace(marker.Operator) == "" || strings.TrimSpace(marker.WorkerHost) == "" || strings.TrimSpace(marker.GitCommit) == "" {
		return errors.New("reclamation run ID, operator, worker host, and git commit are required")
	}
	if marker.StartedAt.IsZero() || marker.CompletedAt.IsZero() || marker.CompletedAt.Before(marker.StartedAt) {
		return errors.New("reclaimed marker has invalid start or completion time")
	}
	return nil
}

func ValidateProcessedAgainstReady(processed ProcessedMarker, ready rawstore.ReadyManifest, readySHA256 rawstore.SHA256) error {
	if err := processed.Validate(); err != nil {
		return errors.Wrap(err, "processed marker")
	}
	if err := ready.Validate(); err != nil {
		return errors.Wrap(err, "ready manifest")
	}
	if err := readySHA256.Validate(); err != nil {
		return errors.Wrap(err, "ready manifest SHA-256")
	}
	if processed.CrawlID != ready.CrawlID || processed.Selection != ready.Selection || processed.Part != ready.Part {
		return errors.New("processed marker and ready manifest identify different parts")
	}
	if processed.InputReadySHA256 != readySHA256 {
		return errors.New("processed marker references a different ready manifest")
	}
	if processed.Counts.InputRecords != ready.Totals.RequestedRecords || processed.Counts.DownloadedRecords != ready.Totals.DownloadedRecords || processed.Counts.FailedRecords != ready.Totals.FailedRecords {
		return errors.New("processed marker input counts do not match ready manifest totals")
	}
	if len(processed.InputManifests) != len(ready.Chunks) {
		return errors.New("processed marker input manifest count does not match ready chunks")
	}
	for i, chunk := range ready.Chunks {
		reference := processed.InputManifests[i]
		if reference.Chunk != chunk.Chunk || reference.ManifestKey != chunk.ManifestKey || reference.ManifestSHA256 != chunk.ManifestSHA256 {
			return errors.Newf("processed marker input manifest %d does not match ready chunk", i)
		}
	}
	return nil
}

func ValidateLoadedAgainstProcessed(loaded LoadedMarker, processed ProcessedMarker, processedSHA256 rawstore.SHA256) error {
	if err := loaded.Validate(); err != nil {
		return errors.Wrap(err, "loaded marker")
	}
	if err := processed.Validate(); err != nil {
		return errors.Wrap(err, "processed marker")
	}
	if err := processedSHA256.Validate(); err != nil {
		return errors.Wrap(err, "processed marker SHA-256")
	}
	if loaded.CrawlID != processed.CrawlID || loaded.Selection != processed.Selection || loaded.Part != processed.Part || loaded.Processor != processed.Processor {
		return errors.New("loaded and processed markers identify different processor inputs")
	}
	if loaded.ProcessingVersion != processed.ProcessingVersion || loaded.SourceRunID != processed.RunID {
		return errors.New("loaded marker references a different processing run")
	}
	if loaded.ProcessedSHA256 != processedSHA256 {
		return errors.New("loaded marker references a different processed marker")
	}
	return nil
}

func validateProcessorMarker(schemaVersion int, crawlID, selection string, part int, processor, processingVersion, gitCommit, runID, workerHost string) error {
	if schemaVersion != SchemaVersion {
		return errors.Newf("unsupported state marker schema version %d", schemaVersion)
	}
	if err := rawstore.ValidatePartIdentity(crawlID, selection, part); err != nil {
		return errors.Wrap(err, "state marker identity")
	}
	if !processorPattern.MatchString(processor) {
		return errors.Newf("invalid processor %q", processor)
	}
	if strings.TrimSpace(processingVersion) == "" || strings.TrimSpace(gitCommit) == "" {
		return errors.New("processing version and git commit are required")
	}
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(workerHost) == "" {
		return errors.New("run ID and worker host are required")
	}
	return nil
}

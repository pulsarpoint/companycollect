package companysources

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runindex"
	"github.com/pulsarpoint/corpscout/scheduler/internal/companysources/runmanifest"
)

func ImportRun(ctx context.Context, registry Registry, req ImportRunRequest) (ImportResult, error) {
	source, err := registry.Get(req.Country, req.Source)
	if err != nil {
		return ImportResult{}, err
	}
	return source.Import(ctx, ImportOptions{
		RunDir:              req.RunDir,
		Files:               req.Files,
		ClickHouseNativeURL: req.ClickHouseNativeURL,
		BatchSize:           req.BatchSize,
		Limit:               req.Limit,
		Truncate:            req.Truncate,
	})
}

func ImportChangedRuns(ctx context.Context, registry Registry, req ImportChangedRunsRequest) (ImportChangedRunsResult, error) {
	index, err := runindex.Load(req.RunIndexPath)
	if err != nil {
		return ImportChangedRunsResult{}, err
	}

	summaries := make([]ImportChangedSourceResult, 0, len(registry.Keys()))
	for _, key := range registry.Keys() {
		country, sourceKey, ok := strings.Cut(key, "/")
		if !ok {
			return ImportChangedRunsResult{}, errors.Errorf("invalid source key %s", key)
		}

		runDir, manifest, err := runmanifest.LatestCompletedRun(req.RunsRoot, country, sourceKey)
		if err != nil {
			return ImportChangedRunsResult{}, err
		}
		manifestHash, err := runmanifest.Hash(runDir)
		if err != nil {
			return ImportChangedRunsResult{}, err
		}
		rawHashes := make([]string, 0, len(manifest.Files))
		for _, file := range manifest.Files {
			rawHashes = append(rawHashes, file.SHA256)
		}

		if req.ChangedOnly && !index.ShouldImport(country, sourceKey, manifest.RunID, manifestHash, rawHashes) {
			summaries = append(summaries, ImportChangedSourceResult{Source: key, RunID: manifest.RunID, Status: "skipped"})
			continue
		}

		result, err := ImportRun(ctx, registry, ImportRunRequest{
			Country:             country,
			Source:              sourceKey,
			RunDir:              runDir,
			ClickHouseNativeURL: req.ClickHouseNativeURL,
			BatchSize:           req.BatchSize,
			Limit:               req.Limit,
			Truncate:            req.Truncate,
		})
		if err != nil {
			return ImportChangedRunsResult{}, err
		}
		index.MarkImported(runindex.Entry{
			Country:       country,
			Source:        sourceKey,
			RunID:         manifest.RunID,
			ManifestHash:  manifestHash,
			RawFileHashes: rawHashes,
			ImportedAt:    time.Now().UTC(),
			Status:        runindex.StatusImported,
		})
		summaries = append(summaries, ImportChangedSourceResult{
			Source:       key,
			RunID:        manifest.RunID,
			Status:       runindex.StatusImported,
			ImportedRows: result.ImportedRows,
		})
	}

	if err := runindex.Save(req.RunIndexPath, index); err != nil {
		return ImportChangedRunsResult{}, err
	}
	return ImportChangedRunsResult{Sources: summaries}, nil
}

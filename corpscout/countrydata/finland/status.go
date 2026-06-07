package finland

import (
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	countryimport "github.com/pulsarpoint/corpscout/countrydata/import"
)

type SourceStatus struct {
	SourceSlug             string    `json:"source_slug"`
	Status                 string    `json:"status"`
	LastExportedAt         time.Time `json:"last_exported_at,omitempty"`
	LastExportManifestPath string    `json:"last_export_manifest_path,omitempty"`
	RecordsSeen            int64     `json:"records_seen"`
	RecordsExported        int64     `json:"records_exported"`
	DecodeErrors           int64     `json:"decode_errors"`
	Warnings               []string  `json:"warnings,omitempty"`
}

func SourceStatusFromLatestManifest(dataDir string, sourceSlug string) (SourceStatus, error) {
	layout := LayoutForDataDir(dataDir)
	exportsDir := layout.SourceExportsDir(sourceSlug)
	manifestPath, err := latestManifestPath(exportsDir)
	if errors.Is(err, os.ErrNotExist) {
		return SourceStatus{SourceSlug: sourceSlug, Status: "missing"}, nil
	}
	if err != nil {
		return SourceStatus{}, err
	}
	manifest, err := countryimport.LoadExportManifest(manifestPath)
	if err != nil {
		return SourceStatus{}, err
	}
	return SourceStatus{
		SourceSlug:             sourceSlug,
		Status:                 "exported",
		LastExportedAt:         manifest.CreatedAt,
		LastExportManifestPath: manifestPath,
		RecordsSeen:            manifest.RecordsSeen,
		RecordsExported:        manifest.RecordsExported,
		DecodeErrors:           manifest.DecodeErrors,
		Warnings:               manifest.Warnings,
	}, nil
}

func latestManifestPath(exportsDir string) (string, error) {
	entries, err := os.ReadDir(exportsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return "", os.ErrNotExist
		}
		return "", errors.Wrap(err, "read exports directory")
	}
	runDirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			runDirs = append(runDirs, entry.Name())
		}
	}
	if len(runDirs) == 0 {
		return "", os.ErrNotExist
	}
	sort.Strings(runDirs)
	return filepath.Join(exportsDir, runDirs[len(runDirs)-1], "manifest.json"), nil
}

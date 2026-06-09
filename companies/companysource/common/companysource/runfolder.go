package companysource

import (
	"path/filepath"
	"strings"
)

func DefaultRunDir(dataRoot string, country string, source string, runID string) string {
	return filepath.Join(dataRoot, country, "sources", source, "runs", runID)
}

func SourceFileName(ext string) string {
	ext = strings.TrimPrefix(strings.TrimSpace(ext), ".")
	if ext == "" {
		ext = "dat"
	}
	return "source." + ext
}

func ManifestPath(runDir string) string {
	return filepath.Join(runDir, "manifest.json")
}

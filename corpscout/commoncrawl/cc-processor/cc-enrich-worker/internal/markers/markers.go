package markers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// Produced records row counts and metadata when a WARC part finishes processing.
type Produced struct {
	Part        uint32         `json:"part"`
	Cmd         string         `json:"cmd"`
	Rows        map[string]int `json:"rows"` // kind -> row count, e.g. "domains": 812
	SourceRunID string         `json:"source_run_id"`
	DurationS   float64        `json:"duration_s"`
	FinishedAt  time.Time      `json:"finished_at"`
}

// ProducedPath returns the path to the .produced marker file for an output directory.
func ProducedPath(outDir string) string {
	return outDir + ".produced"
}

// LoadedPath returns the path to the .loaded marker file for an output directory.
func LoadedPath(outDir string) string {
	return outDir + ".loaded"
}

// WriteProduced writes a Produced struct to a .produced marker file via temp+rename for atomicity.
func WriteProduced(outDir string, p Produced) error {
	path := ProducedPath(outDir)
	parentDir := filepath.Dir(path)

	// Create temp file in the same directory as the marker file
	tmpFile, err := os.CreateTemp(parentDir, ".marker-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()

	// Write JSON to temp file
	encoder := json.NewEncoder(tmpFile)
	if err := encoder.Encode(p); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Atomically rename temp file to final path
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

// ReadProduced reads and parses a .produced marker file.
func ReadProduced(outDir string) (Produced, error) {
	path := ProducedPath(outDir)

	data, err := os.ReadFile(path)
	if err != nil {
		return Produced{}, err
	}

	var p Produced
	if err := json.Unmarshal(data, &p); err != nil {
		return Produced{}, err
	}

	return p, nil
}

// WriteLoaded writes an empty .loaded marker file via temp+rename for atomicity.
// It is idempotent: calling it multiple times succeeds.
func WriteLoaded(outDir string) error {
	path := LoadedPath(outDir)
	parentDir := filepath.Dir(path)

	// Create temp file in the same directory as the marker file
	tmpFile, err := os.CreateTemp(parentDir, ".marker-*")
	if err != nil {
		return err
	}
	tmpPath := tmpFile.Name()

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Atomically rename temp file to final path
	// If the file already exists, rename will overwrite it (idempotent)
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

// Exists checks if a file exists at the given path.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

package markers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestProducedPath(t *testing.T) {
	path := ProducedPath("/tmp/output")
	if path != "/tmp/output.produced" {
		t.Errorf("ProducedPath: got %q, want %q", path, "/tmp/output.produced")
	}
}

func TestLoadedPath(t *testing.T) {
	path := LoadedPath("/tmp/output")
	if path != "/tmp/output.loaded" {
		t.Errorf("LoadedPath: got %q, want %q", path, "/tmp/output.loaded")
	}
}

func TestExistsFalseToTrue(t *testing.T) {
	tmpDir := t.TempDir()
	testPath := filepath.Join(tmpDir, "test.marker")

	// Should not exist initially
	if Exists(testPath) {
		t.Errorf("Exists: expected false for non-existent file")
	}

	// Create the file
	if err := os.WriteFile(testPath, []byte{}, 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Should exist now
	if !Exists(testPath) {
		t.Errorf("Exists: expected true for existing file")
	}
}

func TestWriteAndReadProduced(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "output")

	// Create the output directory
	if err := os.Mkdir(outDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// Create a Produced struct with test data
	finishedAt := time.Date(2026, 7, 13, 12, 30, 45, 0, time.UTC)
	original := Produced{
		Part:        1,
		Cmd:         "enrich",
		Rows:        map[string]int{"domains": 812, "tech": 450},
		SourceRunID: "run-001",
		DurationS:   123.45,
		FinishedAt:  finishedAt,
	}

	// Write the produced marker
	if err := WriteProduced(outDir, original); err != nil {
		t.Fatalf("WriteProduced failed: %v", err)
	}

	// Check that no temp files remain
	entries, err := os.ReadDir(filepath.Dir(outDir))
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) == ".tmp" || filepath.HasPrefix(name, ".marker-") {
			t.Errorf("Found leftover temp file: %s", name)
		}
	}

	// Read the produced marker
	read, err := ReadProduced(outDir)
	if err != nil {
		t.Fatalf("ReadProduced failed: %v", err)
	}

	// Verify round-trip
	if read.Part != original.Part {
		t.Errorf("Part: got %d, want %d", read.Part, original.Part)
	}
	if read.Cmd != original.Cmd {
		t.Errorf("Cmd: got %q, want %q", read.Cmd, original.Cmd)
	}
	if len(read.Rows) != len(original.Rows) {
		t.Errorf("Rows length: got %d, want %d", len(read.Rows), len(original.Rows))
	}
	if read.Rows["domains"] != 812 {
		t.Errorf("Rows[domains]: got %d, want 812", read.Rows["domains"])
	}
	if read.Rows["tech"] != 450 {
		t.Errorf("Rows[tech]: got %d, want 450", read.Rows["tech"])
	}
	if read.SourceRunID != original.SourceRunID {
		t.Errorf("SourceRunID: got %q, want %q", read.SourceRunID, original.SourceRunID)
	}
	if read.DurationS != original.DurationS {
		t.Errorf("DurationS: got %f, want %f", read.DurationS, original.DurationS)
	}
	if !read.FinishedAt.Equal(original.FinishedAt) {
		t.Errorf("FinishedAt: got %v, want %v", read.FinishedAt, original.FinishedAt)
	}
}

func TestWriteProducedNoTempLeftover(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "output")

	// Create the output directory
	if err := os.Mkdir(outDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	produced := Produced{
		Part:        2,
		Cmd:         "test",
		Rows:        map[string]int{"domains": 100},
		SourceRunID: "run-002",
		DurationS:   50.0,
		FinishedAt:  time.Now().UTC(),
	}

	// Write the produced marker
	if err := WriteProduced(outDir, produced); err != nil {
		t.Fatalf("WriteProduced failed: %v", err)
	}

	// List parent directory entries
	parentDir := filepath.Dir(outDir)
	entries, err := os.ReadDir(parentDir)
	if err != nil {
		t.Fatalf("Failed to read parent directory: %v", err)
	}

	// Assert no temp files in parent directory
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) == ".tmp" || filepath.HasPrefix(name, ".marker-") {
			t.Errorf("Found leftover temp file after WriteProduced: %s", name)
		}
	}
}

func TestWriteLoadedIdempotent(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "output")

	// Create the output directory
	if err := os.Mkdir(outDir, 0755); err != nil {
		t.Fatalf("Failed to create output directory: %v", err)
	}

	// First call to WriteLoaded
	if err := WriteLoaded(outDir); err != nil {
		t.Fatalf("WriteLoaded (first call) failed: %v", err)
	}

	// Verify .loaded file exists
	loadedPath := LoadedPath(outDir)
	if !Exists(loadedPath) {
		t.Errorf("WriteLoaded: .loaded file does not exist after first call")
	}

	// Second call to WriteLoaded (should succeed due to idempotency)
	if err := WriteLoaded(outDir); err != nil {
		t.Fatalf("WriteLoaded (second call) failed: %v", err)
	}

	// Verify .loaded file still exists
	if !Exists(loadedPath) {
		t.Errorf("WriteLoaded: .loaded file does not exist after second call")
	}

	// Check that no temp files remain
	parentDir := filepath.Dir(outDir)
	entries, err := os.ReadDir(parentDir)
	if err != nil {
		t.Fatalf("Failed to read parent directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if filepath.Ext(name) == ".tmp" || filepath.HasPrefix(name, ".marker-") {
			t.Errorf("Found leftover temp file after WriteLoaded: %s", name)
		}
	}
}

func TestReadProducedNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	outDir := filepath.Join(tmpDir, "nonexistent")

	_, err := ReadProduced(outDir)
	if err == nil {
		t.Errorf("ReadProduced: expected error for non-existent file")
	}
}

func TestProducedJSONTags(t *testing.T) {
	// Verify that JSON marshaling uses correct tags
	produced := Produced{
		Part:        1,
		Cmd:         "test",
		Rows:        map[string]int{"domains": 100},
		SourceRunID: "run-001",
		DurationS:   10.5,
		FinishedAt:  time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC),
	}

	data, err := json.Marshal(produced)
	if err != nil {
		t.Fatalf("Failed to marshal Produced: %v", err)
	}

	// Unmarshal and verify fields are present with correct JSON keys
	var result map[string]interface{}
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Failed to unmarshal JSON: %v", err)
	}

	expectedKeys := []string{"part", "cmd", "rows", "source_run_id", "duration_s", "finished_at"}
	for _, key := range expectedKeys {
		if _, exists := result[key]; !exists {
			t.Errorf("Missing JSON key: %s", key)
		}
	}
}

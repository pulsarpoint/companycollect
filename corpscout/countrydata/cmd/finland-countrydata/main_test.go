package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseArgsSyncSource(t *testing.T) {
	cfg, err := parseArgs([]string{"sync-source", "--source", "prhytj", "--data-dir", "/data", "--max-pages", "2"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "sync-source" || cfg.source != "prhytj" || cfg.dataDir != "/data" || cfg.maxPages != 2 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsBuildExport(t *testing.T) {
	cfg, err := parseArgs([]string{"build-export", "--data-dir", "/data", "--run-id", "final-run-1"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "build-export" || cfg.runID != "final-run-1" {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsRejectsUnknownSource(t *testing.T) {
	_, err := parseArgs([]string{"sync-source", "--source", "unknown"})
	if err == nil {
		t.Fatal("parse args returned nil error")
	}
}

func TestRunExportSourceReportsPublicSourceKey(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := writeCLITestSnapshot(t, dataDir)

	result, err := run(t.Context(), cliConfig{
		command:      "export-source",
		source:       "prhytj",
		dataDir:      dataDir,
		snapshotPath: snapshotPath,
		runID:        "source-run-1",
	})
	if err != nil {
		t.Fatalf("run export-source: %v", err)
	}

	requireStringResult(t, result, "source", "prhytj")
	requireStringResult(t, result, "status", "ok")
	requireNonEmptyStringResult(t, result, "source_manifest_path")
}

func TestRunStatusSourceReportsPublicSourceKey(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := writeCLITestSnapshot(t, dataDir)
	if _, err := run(t.Context(), cliConfig{
		command:      "export-source",
		source:       "prhytj",
		dataDir:      dataDir,
		snapshotPath: snapshotPath,
		runID:        "source-run-1",
	}); err != nil {
		t.Fatalf("run export-source: %v", err)
	}

	result, err := run(t.Context(), cliConfig{
		command: "status-source",
		source:  "prhytj",
		dataDir: dataDir,
	})
	if err != nil {
		t.Fatalf("run status-source: %v", err)
	}

	requireStringResult(t, result, "source", "prhytj")
	requireStringResult(t, result, "status", "exported")
	requireNonEmptyStringResult(t, result, "source_manifest_path")
}

func TestRunBuildExportReportsFinalManifest(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := writeCLITestSnapshot(t, dataDir)
	if _, err := run(t.Context(), cliConfig{
		command:      "export-source",
		source:       "prhytj",
		dataDir:      dataDir,
		snapshotPath: snapshotPath,
		runID:        "source-run-1",
	}); err != nil {
		t.Fatalf("run export-source: %v", err)
	}

	result, err := run(t.Context(), cliConfig{
		command: "build-export",
		dataDir: dataDir,
		runID:   "final-run-1",
	})
	if err != nil {
		t.Fatalf("run build-export: %v", err)
	}

	requireStringResult(t, result, "status", "ok")
	requireNonEmptyStringResult(t, result, "final_manifest_path")
}

func writeCLITestSnapshot(t *testing.T, dataDir string) string {
	t.Helper()
	snapshotPath := filepath.Join(dataDir, "sources", "prhytj", "snapshots", "sample.ndjson")
	writeCLITestFile(t, snapshotPath, []byte(`{"businessId":{"value":"0100130-4"},"names":[{"name":"Dynava Oy","type":"1"}],"tradeRegisterStatus":"1","status":"2"}`+"\n"))
	return snapshotPath
}

func writeCLITestFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func requireStringResult(t *testing.T, result map[string]any, key string, want string) {
	t.Helper()
	got, ok := result[key].(string)
	if !ok {
		t.Fatalf("result[%q] = %#v, want string %q", key, result[key], want)
	}
	if got != want {
		t.Fatalf("result[%q] = %q, want %q", key, got, want)
	}
}

func requireNonEmptyStringResult(t *testing.T, result map[string]any, key string) {
	t.Helper()
	got, ok := result[key].(string)
	if !ok {
		t.Fatalf("result[%q] = %#v, want non-empty string", key, result[key])
	}
	if got == "" {
		t.Fatalf("result[%q] is empty", key)
	}
}

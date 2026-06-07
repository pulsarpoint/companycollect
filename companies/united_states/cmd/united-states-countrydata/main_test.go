package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestParseArgsSyncSource(t *testing.T) {
	cfg, err := parseArgs([]string{"sync-source", "--source", "secedgar", "--data-dir", "/data", "--run-id", "run-1", "--chunk-size", "25"})
	if err != nil {
		t.Fatalf("parse args: %v", err)
	}
	if cfg.command != "sync-source" || cfg.source != "secedgar" || cfg.dataDir != "/data" || cfg.runID != "run-1" || cfg.chunkSize != 25 {
		t.Fatalf("cfg = %#v", cfg)
	}
}

func TestParseArgsRejectsUnknownSource(t *testing.T) {
	_, err := parseArgs([]string{"sync-source", "--source", "unknown"})
	if err == nil {
		t.Fatal("parse args returned nil error")
	}
}

func TestParseArgsRejectsFutureSourceForRunnableSourceCommand(t *testing.T) {
	_, err := parseArgs([]string{"export-source", "--source", "irseobmf"})
	if err == nil {
		t.Fatal("parse args returned nil error")
	}
}

func TestRunExportSourceReportsPublicSourceKey(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := writeCLITestSnapshot(t, dataDir)

	result, err := run(t.Context(), cliConfig{
		command:      "export-source",
		source:       "secedgar",
		dataDir:      dataDir,
		snapshotPath: snapshotPath,
		runID:        "source-run-1",
	})
	if err != nil {
		t.Fatalf("run export-source: %v", err)
	}

	requireStringResult(t, result, "source", "secedgar")
	requireStringResult(t, result, "status", "ok")
	requireStringResult(t, result, "run_id", "source-run-1")
	requireNonEmptyStringResult(t, result, "source_manifest_path")
}

func TestRunStatusSourceReportsPublicSourceKey(t *testing.T) {
	dataDir := t.TempDir()
	snapshotPath := writeCLITestSnapshot(t, dataDir)
	if _, err := run(t.Context(), cliConfig{
		command:      "export-source",
		source:       "secedgar",
		dataDir:      dataDir,
		snapshotPath: snapshotPath,
		runID:        "source-run-1",
	}); err != nil {
		t.Fatalf("run export-source: %v", err)
	}

	result, err := run(t.Context(), cliConfig{
		command: "status-source",
		source:  "secedgar",
		dataDir: dataDir,
	})
	if err != nil {
		t.Fatalf("run status-source: %v", err)
	}

	requireStringResult(t, result, "source", "secedgar")
	requireStringResult(t, result, "status", "exported")
	requireNonEmptyStringResult(t, result, "source_manifest_path")
}

func TestRunStatusReportsSecEdgar(t *testing.T) {
	result, err := run(t.Context(), cliConfig{
		command: "status",
		dataDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("run status: %v", err)
	}

	requireStringResult(t, result, "status", "ok")
	sources, ok := result["sources"].(map[string]any)
	if !ok {
		t.Fatalf("sources = %#v, want map[string]any", result["sources"])
	}
	if _, ok := sources["secedgar"]; !ok {
		t.Fatalf("sources missing secedgar: %#v", sources)
	}
}

func TestRunSyncSourceDownloadsProcessesAndExportsFromLocalServer(t *testing.T) {
	dataDir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(secFixturePayload())
	}))
	t.Cleanup(server.Close)
	t.Setenv("USA_SEC_EDGAR_DOWNLOAD_URL", server.URL)

	result, err := run(t.Context(), cliConfig{
		command:   "sync-source",
		source:    "secedgar",
		dataDir:   dataDir,
		runID:     "sync-run-1",
		chunkSize: 1,
	})
	if err != nil {
		t.Fatalf("run sync-source: %v", err)
	}

	requireStringResult(t, result, "source", "secedgar")
	requireStringResult(t, result, "status", "ok")
	requireStringResult(t, result, "run_id", "sync-run-1")
	requireInt64Result(t, result, "records_downloaded", 2)
	requireInt64Result(t, result, "records_processed", 2)
	requireInt64Result(t, result, "records_exported", 2)
	requireNonEmptyStringResult(t, result, "snapshot_path")
	requireNonEmptyStringResult(t, result, "source_manifest_path")
}

func TestRunSyncAliasesSyncSource(t *testing.T) {
	dataDir := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(secFixturePayload())
	}))
	t.Cleanup(server.Close)
	t.Setenv("USA_SEC_EDGAR_DOWNLOAD_URL", server.URL)

	result, err := run(t.Context(), cliConfig{
		command: "sync",
		source:  "secedgar",
		dataDir: dataDir,
		runID:   "sync-alias-run-1",
	})
	if err != nil {
		t.Fatalf("run sync: %v", err)
	}

	requireStringResult(t, result, "command", "sync")
	requireStringResult(t, result, "source", "secedgar")
	requireStringResult(t, result, "status", "ok")
}

func TestRunBuildExportNotImplemented(t *testing.T) {
	_, err := run(t.Context(), cliConfig{command: "build-export", dataDir: t.TempDir()})
	if err == nil {
		t.Fatal("run build-export returned nil error")
	}
}

func writeCLITestSnapshot(t *testing.T, dataDir string) string {
	t.Helper()
	snapshotPath := filepath.Join(dataDir, "sources", "secedgar", "snapshots", "company_tickers.json")
	writeCLITestFile(t, snapshotPath, secFixturePayload())
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

func secFixturePayload() []byte {
	return []byte(`{"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."},"1":{"cik_str":789019,"ticker":"MSFT","title":"Microsoft Corp."}}`)
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

func requireInt64Result(t *testing.T, result map[string]any, key string, want int64) {
	t.Helper()
	got, ok := result[key].(int64)
	if !ok {
		t.Fatalf("result[%q] = %#v, want int64 %d", key, result[key], want)
	}
	if got != want {
		t.Fatalf("result[%q] = %d, want %d", key, got, want)
	}
}

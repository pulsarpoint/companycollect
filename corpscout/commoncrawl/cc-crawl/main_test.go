package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunProduceLoadPreservesLocalLoadedMarkerFlow(t *testing.T) {
	directory := t.TempDir()
	workerPath := filepath.Join(directory, "worker.sh")
	argumentsPath := filepath.Join(directory, "arguments.log")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$ARGUMENTS_PATH"
case "$1" in
  tech)
    shift
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--out" ]; then
        shift
        mkdir -p "$1"
        : > "$1/domains.parquet"
        break
      fi
      shift
    done
    echo 'done: 1 domains'
    ;;
  load)
    echo 'loaded 1 rows: domains.parquet -> commoncrawl_domains'
    ;;
esac
`
	if err := os.WriteFile(workerPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARGUMENTS_PATH", argumentsPath)
	dataDirectory := filepath.Join(directory, "CC-MAIN-2026-25", "crawl")
	if err := os.MkdirAll(dataDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	pc := partCtx{
		lg:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		data:      dataDirectory,
		worker:    workerPath,
		crawl:     "CC-MAIN-2026-25",
		selection: "pages25",
		techConc:  "32",
		techChunk: "16384",
	}
	if outcome := runProduceLoad(pc, "tech", 0); outcome != ocDone {
		t.Fatalf("first outcome=%v, want done", outcome)
	}
	marker := filepath.Join(dataDirectory, "out_tech_0.loaded")
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("loaded marker: %v", err)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	produceCommand := strings.Split(strings.TrimSpace(string(arguments)), "\n")[0]
	for _, expected := range []string{
		"tech", "--crawl-id CC-MAIN-2026-25", "--selection pages25", "--part 0",
	} {
		if !strings.Contains(produceCommand, expected) {
			t.Fatalf("produce command %q does not contain %q", produceCommand, expected)
		}
	}
	if strings.Contains(produceCommand, "--worklist") {
		t.Fatalf("produce command still uses a worklist: %q", produceCommand)
	}

	before := string(arguments)
	if outcome := runProduceLoad(pc, "tech", 0); outcome != ocSkipped {
		t.Fatalf("second outcome=%v, want skipped", outcome)
	}
	after, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != before {
		t.Fatal("already-loaded part invoked the worker again")
	}
}

func TestRunProduceLoadDoesNotLoadOrMarkFailedProduce(t *testing.T) {
	directory := t.TempDir()
	workerPath := filepath.Join(directory, "worker.sh")
	argumentsPath := filepath.Join(directory, "arguments.log")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$ARGUMENTS_PATH"
exit 1
`
	if err := os.WriteFile(workerPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARGUMENTS_PATH", argumentsPath)
	dataDirectory := filepath.Join(directory, "CC-MAIN-2026-25", "crawl")
	if err := os.MkdirAll(dataDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	pc := partCtx{
		lg:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		data:      dataDirectory,
		worker:    workerPath,
		crawl:     "CC-MAIN-2026-25",
		selection: "pages25",
		techConc:  "32",
		techChunk: "16384",
	}
	if outcome := runProduceLoad(pc, "tech", 0); outcome != ocFailed {
		t.Fatalf("outcome=%v, want failed", outcome)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSpace(string(arguments)), "\n"); len(lines) != 1 {
		t.Fatalf("worker calls=%d, want produce only: %q", len(lines), arguments)
	}
	if _, err := os.Stat(filepath.Join(dataDirectory, "out_tech_0.loaded")); !os.IsNotExist(err) {
		t.Fatalf("failed produce created loaded marker: %v", err)
	}
}

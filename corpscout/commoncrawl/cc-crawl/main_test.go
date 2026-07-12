package main

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
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
	dataDirectory := filepath.Join(directory, "CC-MAIN-2026-25", "warc", "pages25")
	if err := os.MkdirAll(dataDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	pc := partCtx{
		lg:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		base:               directory,
		data:               dataDirectory,
		worker:             workerPath,
		crawl:              "CC-MAIN-2026-25",
		selection:          "pages25",
		wholeWARCThreshold: "50",
		techConc:           "32",
		techChunk:          "16384",
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
		"tech", "--base " + directory, "--crawl-id CC-MAIN-2026-25", "--selection pages25",
		"--part 0", "--whole-warc-threshold 50",
	} {
		if !strings.Contains(produceCommand, expected) {
			t.Fatalf("produce command %q does not contain %q", produceCommand, expected)
		}
	}
	if strings.Contains(produceCommand, "--worklist") {
		t.Fatalf("produce command still uses a worklist: %q", produceCommand)
	}
	if strings.Contains(produceCommand, "--s3-anonymous") {
		t.Fatalf("produce command enabled anonymous S3 unexpectedly: %q", produceCommand)
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
	dataDirectory := filepath.Join(directory, "CC-MAIN-2026-25", "warc", "pages25")
	if err := os.MkdirAll(dataDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	pc := partCtx{
		lg:                 slog.New(slog.NewTextHandler(io.Discard, nil)),
		base:               directory,
		data:               dataDirectory,
		worker:             workerPath,
		crawl:              "CC-MAIN-2026-25",
		selection:          "pages25",
		wholeWARCThreshold: "50",
		techConc:           "32",
		techChunk:          "16384",
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

func TestWorkerProcessingArgsUseWARCIndexCatalogAndSourceOptions(t *testing.T) {
	pc := partCtx{
		base:               "/data",
		crawl:              "CC-MAIN-2026-25",
		selection:          "pages7",
		wholeWARCThreshold: "50.25",
		s3Anonymous:        true,
		techConc:           "32",
		techChunk:          "16384",
	}
	want := []string{
		"tech",
		"--tech-engine", "fast",
		"--concurrency", "32",
		"--chunk", "16384",
		"--base", "/data",
		"--crawl-id", "CC-MAIN-2026-25",
		"--selection", "pages7",
		"--part", "85",
		"--whole-warc-threshold", "50.25",
		"--s3-anonymous",
		"--out", "/data/CC-MAIN-2026-25/warc/pages7/out_tech_85",
	}
	got := workerProcessingArgs(pc, "tech", 85, "/data/CC-MAIN-2026-25/warc/pages7/out_tech_85")
	if !slices.Equal(got, want) {
		t.Fatalf("worker args:\n got: %q\nwant: %q", got, want)
	}

	pc.s3Anonymous = false
	got = workerProcessingArgs(pc, "tech", 85, "/output")
	if slices.Contains(got, "--s3-anonymous") {
		t.Fatalf("signed-source worker args contain --s3-anonymous: %q", got)
	}
}

func TestValidateWholeWARCThreshold(t *testing.T) {
	for _, value := range []string{"0", "50", "50.25", "100"} {
		if err := validateWholeWARCThreshold(value); err != nil {
			t.Errorf("validateWholeWARCThreshold(%q): %v", value, err)
		}
	}
	for _, value := range []string{"", "-0.01", "100.01", "NaN", "+Inf", "half"} {
		if err := validateWholeWARCThreshold(value); err == nil {
			t.Errorf("validateWholeWARCThreshold(%q) succeeded, want error", value)
		}
	}
}

func TestSelectionForMaxPages(t *testing.T) {
	for value, want := range map[string]string{"1": "pages1", "25": "pages25", " 7 ": "pages7"} {
		got, err := selectionForMaxPages(value)
		if err != nil {
			t.Fatalf("selectionForMaxPages(%q): %v", value, err)
		}
		if got != want {
			t.Errorf("selectionForMaxPages(%q)=%q, want %q", value, got, want)
		}
	}
	for _, value := range []string{"", "0", "-1", "1.5", "65536", "many"} {
		if _, err := selectionForMaxPages(value); err == nil {
			t.Errorf("selectionForMaxPages(%q) succeeded, want error", value)
		}
	}
}

func TestParseS3Anonymous(t *testing.T) {
	for value, want := range map[string]bool{"false": false, "true": true, " TRUE ": true} {
		got, err := parseS3Anonymous(value)
		if err != nil {
			t.Fatalf("parseS3Anonymous(%q): %v", value, err)
		}
		if got != want {
			t.Errorf("parseS3Anonymous(%q)=%t, want %t", value, got, want)
		}
	}
	if _, err := parseS3Anonymous("sometimes"); err == nil {
		t.Fatal("parseS3Anonymous accepted an invalid value")
	}
}

func TestParseRangeRequiresWARCIndexes(t *testing.T) {
	for value, want := range map[string][2]int{
		"0":      {0, 0},
		"85-100": {85, 100},
	} {
		lo, hi, err := parseRange(value)
		if err != nil {
			t.Fatalf("parseRange(%q): %v", value, err)
		}
		if lo != want[0] || hi != want[1] {
			t.Fatalf("parseRange(%q)=%d-%d, want %d-%d", value, lo, hi, want[0], want[1])
		}
	}
	for _, value := range []string{"-1", "1--2", "10-9", "4294967296"} {
		if _, _, err := parseRange(value); err == nil {
			t.Errorf("parseRange(%q) succeeded, want error", value)
		}
	}
}

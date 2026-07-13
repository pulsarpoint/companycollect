package main

import (
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestMainProcessesAllWARCsAndExitsNonzeroAfterFailures(t *testing.T) {
	if os.Getenv("CC_CRAWL_TEST_MAIN_FAILURE") == "1" {
		os.Args = []string{
			"cc-crawl",
			"-base", os.Getenv("CC_CRAWL_TEST_BASE"),
			"-crawl", "CC-MAIN-2026-25",
			"-mode", "tech",
			"-parts", "7-8",
			"-worker", os.Getenv("CC_CRAWL_TEST_WORKER"),
		}
		main()
		os.Exit(42)
	}

	directory := t.TempDir()
	workerPath := filepath.Join(directory, "worker.sh")
	callsPath := filepath.Join(directory, "calls.log")
	script := `#!/bin/sh
printf '%s\n' "$*" >> "$CC_CRAWL_TEST_CALLS"
exit 1
`
	if err := os.WriteFile(workerPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestMainProcessesAllWARCsAndExitsNonzeroAfterFailures$")
	command.Env = append(os.Environ(),
		"CC_CRAWL_TEST_MAIN_FAILURE=1",
		"CC_CRAWL_TEST_BASE="+directory,
		"CC_CRAWL_TEST_WORKER="+workerPath,
		"CC_CRAWL_TEST_CALLS="+callsPath,
		"DOTENV="+filepath.Join(directory, "missing.env"),
	)
	output, err := command.CombinedOutput()
	exitError, exited := err.(*exec.ExitError)
	if !exited || exitError.ExitCode() != 1 {
		t.Fatalf("cc-crawl exit error=%v, want status 1; output:\n%s", err, output)
	}
	if !strings.Contains(string(output), `"msg":"complete"`) ||
		!strings.Contains(string(output), `"failed":2`) {
		t.Fatalf("cc-crawl did not report the final failure summary:\n%s", output)
	}
	calls, err := os.ReadFile(callsPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(strings.Split(strings.TrimSpace(string(calls)), "\n")); got != 2 {
		t.Fatalf("worker calls=%d, want both requested WARCs; calls:\n%s", got, calls)
	}
}

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
		lg:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		base:      directory,
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
		"tech", "--base " + directory, "--crawl-id CC-MAIN-2026-25", "--selection pages25",
		"--part 0",
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
		lg:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		base:      directory,
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

func TestWorkerProcessingArgsUseWARCIndexCatalogAndSourceOptions(t *testing.T) {
	pc := partCtx{
		base:        "/data",
		crawl:       "CC-MAIN-2026-25",
		selection:   "pages7",
		s3Anonymous: true,
		techConc:    "32",
		techChunk:   "16384",
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

func TestWorkerPathBesideVersionedExecutable(t *testing.T) {
	releaseDirectory := filepath.Join(t.TempDir(), "releases", "release-1", "bin")
	if err := os.MkdirAll(releaseDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	worker := filepath.Join(releaseDirectory, "cc-enrich-worker")
	if err := os.WriteFile(worker, []byte("worker"), 0o755); err != nil {
		t.Fatal(err)
	}
	crawl := filepath.Join(releaseDirectory, "cc-crawl")
	if err := os.WriteFile(crawl, []byte("crawl"), 0o755); err != nil {
		t.Fatal(err)
	}
	releaseRoot := filepath.Dir(filepath.Dir(filepath.Dir(releaseDirectory)))
	currentDirectory := filepath.Join(releaseRoot, "current")
	if err := os.Symlink(filepath.Join("releases", "release-1"), currentDirectory); err != nil {
		t.Fatal(err)
	}
	realWorker, err := filepath.EvalSymlinks(worker)
	if err != nil {
		t.Fatal(err)
	}

	got := workerPathBesideExecutable(filepath.Join(currentDirectory, "bin", "cc-crawl"))
	if got != realWorker {
		t.Fatalf("worker path=%q, want pinned release worker %q", got, realWorker)
	}
}

func TestDotenvPathHonorsOverride(t *testing.T) {
	t.Setenv("DOTENV", "/configured/processor.env")
	if got := dotenvPath("/ignored/cc-crawl"); got != "/configured/processor.env" {
		t.Fatalf("dotenv path=%q, want explicit override", got)
	}
}

func TestDotenvPathFindsProcessorEnvironmentFromDevelopmentBinary(t *testing.T) {
	processorDirectory := t.TempDir()
	executable := filepath.Join(processorDirectory, "cc-crawl", "bin", "cc-crawl")
	environment := filepath.Join(processorDirectory, ".env")
	if err := os.WriteFile(environment, []byte("OUT_BASE_DIR=/data\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if got := dotenvPath(executable); got != environment {
		t.Fatalf("dotenv path=%q, want processor environment %q", got, environment)
	}
}

func TestDotenvPathFallsBackToWorkingDirectory(t *testing.T) {
	if got := dotenvPath(filepath.Join(t.TempDir(), "cc-crawl", "bin", "cc-crawl")); got != ".env" {
		t.Fatalf("dotenv path=%q, want working-directory fallback", got)
	}
}

func TestWorkerPathFindsWorkerInDevelopmentCheckout(t *testing.T) {
	processorDirectory := t.TempDir()
	crawl := filepath.Join(processorDirectory, "cc-crawl", "bin", "cc-crawl")
	worker := filepath.Join(
		processorDirectory,
		"cc-enrich-worker", "bin", "cc-enrich-worker",
	)
	for _, path := range []string{crawl, worker} {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(filepath.Base(path)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	realWorker, err := filepath.EvalSymlinks(worker)
	if err != nil {
		t.Fatal(err)
	}

	if got := workerPathBesideExecutable(crawl); got != realWorker {
		t.Fatalf("worker path=%q, want development worker %q", got, realWorker)
	}
}

func TestWorkerPathFallsBackForDevelopmentBuild(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "cc-crawl")
	if err := os.WriteFile(executable, []byte("crawl"), 0o755); err != nil {
		t.Fatal(err)
	}

	if got := workerPathBesideExecutable(executable); got != repositoryWorkerFallback {
		t.Fatalf("worker path=%q, want repository fallback %q", got, repositoryWorkerFallback)
	}
}

func TestDefaultWorkerPathHonorsOverride(t *testing.T) {
	t.Setenv("WORKER", "/custom/cc-enrich-worker")
	if got := defaultWorkerPath(); got != "/custom/cc-enrich-worker" {
		t.Fatalf("worker path=%q, want explicit override", got)
	}
}

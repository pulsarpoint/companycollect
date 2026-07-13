// cc-crawl drives CommonCrawl enrichment over a range of WARC indexes — the Go replacement for
// run_crawl.sh. For each WARC index it runs TWO separate cc-enrich-worker executions: (1) the pass
// that reads the selected pages from the WARC and PRODUCES out_<mode>_<p>/, then (2) the LOADER
// (load --dir) that pushes it to ClickHouse. Every external command and its exit code are logged as
// structured JSON (log/slog) to stdout AND a file under data/logs; the loader runs ONLY if the produce
// step exited 0 and actually wrote domains.parquet, so a bad/partial produce is never loaded.
//
// It only orchestrates (os/exec) — it does not import cc-enrich-worker. Stdlib only.
//
// Resumable: a WARC index whose out_<mode>_<p>.loaded marker exists is skipped.
//
//	cc-crawl -mode industry -parts 0-299 -crawl CC-MAIN-2026-25     (bare N = one WARC index)
//
// -mode embed is the exception: it REUSES the industry page selection, runs a single embed-only exec (no
// load), and writes raw vectors into embedding/warc/<selection>/out_industry_<p>/. It has
// NO .loaded marker and is NOT wiped before the run — instead the worker opens the existing
// embeddings.parquet and skips the part if it's already a valid, non-empty file (the write is atomic, so
// a readable parquet == a complete run). Used to backfill vectors for already-classified WARC units.
//
// All settings are flags; each defaults from the same-named env var. -mode, -parts, -crawl required.
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// defaultWorkerPath pins the worker to the same versioned release as cc-crawl when both binaries
// are deployed together. Development checkouts retain the historical repository-relative fallback.
func defaultWorkerPath() string {
	if configured := os.Getenv("WORKER"); configured != "" {
		return configured
	}
	executable, err := os.Executable()
	if err != nil {
		return "cc-enrich-worker/bin/cc-enrich-worker"
	}
	return workerPathBesideExecutable(executable)
}

func workerPathBesideExecutable(executable string) string {
	realExecutable, err := filepath.EvalSymlinks(executable)
	if err != nil {
		return "cc-enrich-worker/bin/cc-enrich-worker"
	}
	sibling := filepath.Join(filepath.Dir(realExecutable), "cc-enrich-worker")
	info, err := os.Stat(sibling)
	if err == nil && !info.IsDir() && info.Mode()&0o111 != 0 {
		return sibling
	}
	return "cc-enrich-worker/bin/cc-enrich-worker"
}

// Parsed from the worker's own log lines for the per-WARC counts.
var (
	reDomains = regexp.MustCompile(`done:\s+(\d+)\s+domains`) // produce step: "done: 87916 domains, …"
	reEmbeds  = regexp.MustCompile(`(\d+)\s+embeddings`)      // embed: "done: 87916 embeddings" / "skip: 87916 embeddings already present"
	reLoaded  = regexp.MustCompile(`loaded\s+(\d+)\s+rows`)   // load step: "loaded 87916 rows -> table"
)

// runStep execs name+args (optionally in dir), streaming the child's stdout/stderr to the terminal as
// before while capturing them for count parsing. Returns the exit code (0 ok, -1 = failed to start),
// the captured combined output, and the wall time. It does no logging — the caller logs the structured
// before/after events.
func runStep(name string, args []string, dir string) (code int, output string, elapsed time.Duration) {
	cmd := exec.Command(name, args...)
	if dir != "" {
		cmd.Dir = dir
	}
	var sout, serr strings.Builder
	cmd.Stdout = io.MultiWriter(os.Stdout, &sout)
	cmd.Stderr = io.MultiWriter(os.Stderr, &serr)
	start := time.Now()
	err := cmd.Run()
	elapsed = time.Since(start).Round(time.Second)
	switch {
	case cmd.ProcessState != nil:
		code = cmd.ProcessState.ExitCode()
	case err != nil:
		code = -1 // failed to start (e.g. binary not found)
	}
	return code, sout.String() + "\n" + serr.String(), elapsed
}

func main() {
	// Load .env (overriding the ambient shell, like the old run_crawl.sh `set -a; . ./.env; set +a`)
	// so the worker gets the shared S3, ClickHouse, and embedding config. Missing file is fine.
	_ = godotenv.Overload(env("DOTENV", ".env"))

	s3AnonymousDefault, s3AnonymousEnvErr := parseS3Anonymous(env("S3_ANONYMOUS", "false"))
	fs := flag.NewFlagSet("cc-crawl", flag.ExitOnError)
	modeF := fs.String("mode", "", "pass to run: industry | tech | embed  (required; embed = vectors only, no ClickHouse)")
	partsF := fs.String("parts", "", "WARC index range lo-hi, or a single WARC index N  (required)")
	crawlF := fs.String("crawl", env("CRAWL", ""), "CommonCrawl crawl id, e.g. CC-MAIN-2026-25  (required; or env CRAWL)")
	baseF := fs.String("base", env("OUT_BASE_DIR", ""), "output ROOT (required; or env OUT_BASE_DIR) — all output lives under <base>/<crawl>/")
	_ = fs.String("builder-dir", env("BUILDER_DIR", "index-builder"), "deprecated compatibility flag; ignored")
	workerF := fs.String("worker", defaultWorkerPath(), "cc-enrich-worker binary")
	maxPagesF := fs.String("max-pages", env("MAX_PAGES", "25"), "catalog selection pages per domain; derives selection pagesN")
	wholeWARCThresholdF := fs.String("whole-warc-threshold", env("WHOLE_WARC_THRESHOLD", "50"), "selected compressed-byte percentage that triggers a whole-WARC download (0..100)")
	s3AnonymousF := fs.Bool("s3-anonymous", s3AnonymousDefault, "use the anonymous Common Crawl HTTPS endpoint instead of signed S3")
	indConcF := fs.String("ind-conc", env("IND_CONC", "64"), "industry: fetch concurrency")
	embedConcF := fs.String("embed-conc", env("EMBED_CONC", "128"), "industry: embed concurrency")
	// tech-conc counts DOMAINS in flight; the worker fetches each domain's pages through its own
	// 8-wide page pool, so total in-flight fetches = tech-conc x 8 (32 -> 256 sockets).
	techConcF := fs.String("tech-conc", env("TECH_CONC", "32"), "tech: domains in flight (x8 parallel pages each)")
	// Worker chunks are PAGES (~13.1 pages/domain). Keep chunk >> tech-conc x 13 so the domain pool
	// stays full; 16384 ~= 1250 domains.
	techChunkF := fs.String("tech-chunk", env("TECH_CHUNK", "16384"), "tech: pages per fetch+process chunk")
	_ = fs.Parse(os.Args[1:])

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "error: "+format+"\n", a...)
		fs.Usage()
		os.Exit(2)
	}
	if s3AnonymousEnvErr != nil {
		fail("S3_ANONYMOUS: %v", s3AnonymousEnvErr)
	}
	mode := *modeF
	if mode != "industry" && mode != "tech" && mode != "embed" {
		fail("-mode must be industry|tech|embed")
	}
	if *crawlF == "" {
		fail("-crawl is required (no default crawl)")
	}
	lo, hi, err := parseRange(*partsF)
	if err != nil {
		fail("-parts %q: %v", *partsF, err)
	}
	crawl, worker := *crawlF, *workerF
	selection, err := selectionForMaxPages(*maxPagesF)
	if err != nil {
		fail("-max-pages %v", err)
	}
	wholeWARCThreshold := strings.TrimSpace(*wholeWARCThresholdF)
	if err := validateWholeWARCThreshold(wholeWARCThreshold); err != nil {
		fail("-whole-warc-threshold %v", err)
	}
	// Output root MUST come from OUT_BASE_DIR — no silent default, so data can never scatter to an
	// unexpected place. Everything for a crawl lives under <OUT_BASE_DIR>/<crawl>/ (crawl/:
	// per-WARC output + logs; embedding/: the vector tree), so different crawls never collide/overwrite.
	base := *baseF
	if base == "" {
		fail("-base is required (output root; or env OUT_BASE_DIR), e.g. /opt/companycollect/corpscout/commoncrawl/data")
	}
	if abs, err := filepath.Abs(base); err == nil {
		base = abs
	}
	data := filepath.Join(base, crawl, "warc", selection)
	// Resolve to absolute paths so every child writes to the intended location regardless of cwd.
	if abs, err := filepath.Abs(worker); err == nil {
		worker = abs
	}

	if err := os.MkdirAll(filepath.Join(data, "logs"), 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logPath := filepath.Join(data, "logs",
		fmt.Sprintf("crawl_%s_%d-%d_%s.log", mode, lo, hi, time.Now().Format("20060102_150405")))
	logFile, err := os.Create(logPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer logFile.Close()

	// Structured JSON logs to stdout AND the run's log file. The subprocesses' own verbose output
	// still streams to stdout/stderr as text.
	lg := slog.New(slog.NewJSONHandler(io.MultiWriter(os.Stdout, logFile), nil))
	lg.Info("start", "mode", mode, "parts", *partsF, "crawl", crawl, "log", logPath)

	pc := partCtx{
		lg: lg, base: base, data: data, worker: worker, crawl: crawl, selection: selection,
		wholeWARCThreshold: wholeWARCThreshold, s3Anonymous: *s3AnonymousF,
		indConc: *indConcF, embedConc: *embedConcF, techConc: *techConcF, techChunk: *techChunkF,
	}
	var done, skipped, failed int
	for p := lo; p <= hi; p++ {
		var oc outcome
		switch mode {
		case "industry":
			oc = runIndustryPart(pc, p)
		case "tech":
			oc = runTechPart(pc, p)
		case "embed":
			oc = runEmbedPart(pc, p)
		}
		switch oc {
		case ocDone:
			done++
		case ocSkipped:
			skipped++
		case ocFailed:
			failed++
		}
	}
	lg.Info("complete", "mode", mode, "lo", lo, "hi", hi, "done", done, "skipped", skipped, "failed", failed)
	if failed > 0 {
		os.Exit(1)
	}
}

// outcome is what a per-WARC runner reports back to the main loop for tallying.
type outcome int

const (
	ocFailed outcome = iota
	ocSkipped
	ocDone
)

// partCtx is the shared, per-run configuration handed to each WARC-index runner. The name preserves
// the existing operator vocabulary used by -parts, log fields, output directories, and loaded markers.
type partCtx struct {
	lg                           *slog.Logger
	base, data, worker           string
	crawl, selection             string
	wholeWARCThreshold           string
	s3Anonymous                  bool
	indConc, embedConc, techConc string
	techChunk                    string
}

// runIndustryPart / runTechPart are the load-gated modes: produce → load into ClickHouse → mark. A "done"
// WARC unit means produced AND loaded, recorded by the out_<mode>_<p>.loaded marker (the load is a remote side
// effect not visible in local files, so the marker is the only record of it).
func runIndustryPart(pc partCtx, p int) outcome { return runProduceLoad(pc, "industry", p) }
func runTechPart(pc partCtx, p int) outcome     { return runProduceLoad(pc, "tech", p) }

func runProduceLoad(pc partCtx, mode string, p int) outcome {
	plg := pc.lg.With("mode", mode, "part", p)
	outDir := filepath.Join(pc.data, fmt.Sprintf("out_%s_%d", mode, p))
	marker := outDir + ".loaded"
	if _, err := os.Stat(marker); err == nil {
		plg.Info("skip", "reason", "already loaded")
		return ocSkipped
	} else if !os.IsNotExist(err) {
		plg.Error("failed", "step", "check_marker", "err", err.Error())
		return ocFailed
	}
	// Clear any stale/partial output (the worker requires an empty --out); safe — a done WARC unit already
	// returned above on its marker.
	if err := os.RemoveAll(outDir); err != nil {
		plg.Error("failed", "step", "clear_output", "err", err.Error())
		return ocFailed
	}
	pArgs := workerProcessingArgs(pc, mode, p, outDir)
	plg.Info("produce.run", "cmd", pc.worker+" "+strings.Join(pArgs, " "))
	code, pout, pel := runStep(pc.worker, pArgs, "")
	if code != 0 {
		plg.Error("failed", "step", "produce", "exit", code, "elapsed", pel.String(), "loader", "skipped")
		return ocFailed
	}
	if _, err := os.Stat(filepath.Join(outDir, "domains.parquet")); err != nil {
		plg.Error("failed", "step", "produce", "exit", 0, "reason", "domains.parquet missing", "loader", "skipped")
		return ocFailed
	}
	produced := firstInt(reDomains, pout)
	plg.Info("produce.exit", "exit", 0, "produced", produced, "elapsed", pel.String())

	lArgs := []string{"load", "--dir", outDir}
	plg.Info("load.run", "cmd", pc.worker+" "+strings.Join(lArgs, " "))
	lcode, lout, lel := runStep(pc.worker, lArgs, "")
	rows := sumInt(reLoaded, lout)
	if lcode != 0 {
		plg.Error("failed", "step", "load", "exit", lcode, "elapsed", lel.String())
		return ocFailed
	}
	plg.Info("load.exit", "exit", 0, "rows_to_clickhouse", rows, "elapsed", lel.String())

	if err := os.WriteFile(marker, nil, 0o644); err != nil {
		plg.Error("failed", "step", "marker", "err", err.Error())
		return ocFailed
	}
	plg.Info("done", "produced", produced, "rows_to_clickhouse", rows)
	return ocDone
}

// runEmbedPart is the embed-only mode: reuse the same catalog page selection, run ONE embed exec, no load, no
// marker. There is deliberately NO RemoveAll — the worker opens the existing embeddings.parquet and skips
// the WARC unit if it's already complete (the write is atomic, so a readable parquet == a complete run);
// wiping it would defeat that. Vectors land in the sibling data/embedding/ tree.
func runEmbedPart(pc partCtx, p int) outcome {
	plg := pc.lg.With("mode", "embed", "part", p)
	outDir := filepath.Join(pc.base, pc.crawl, "embedding", "warc", pc.selection, fmt.Sprintf("out_industry_%d", p))
	pArgs := workerProcessingArgs(pc, "embed", p, outDir)
	plg.Info("embed.run", "cmd", pc.worker+" "+strings.Join(pArgs, " "))
	code, pout, pel := runStep(pc.worker, pArgs, "")
	if code != 0 {
		plg.Error("failed", "step", "embed", "exit", code, "elapsed", pel.String())
		return ocFailed
	}
	if _, err := os.Stat(filepath.Join(outDir, "embeddings.parquet")); err != nil {
		plg.Error("failed", "step", "embed", "exit", 0, "reason", "embeddings.parquet missing")
		return ocFailed
	}
	if strings.Contains(pout, "already present") { // worker verified an existing complete output
		plg.Info("skip", "reason", "already embedded", "embeddings", firstInt(reEmbeds, pout))
		return ocSkipped
	}
	plg.Info("done", "embeddings", firstInt(reEmbeds, pout), "elapsed", pel.String())
	return ocDone
}

// passArgs builds the per-mode cc-enrich-worker pass flags.
func passArgs(pc partCtx, mode string) []string {
	switch mode {
	case "industry", "embed": // embed runs the same fetch→embed stream, just without classify
		return []string{"--concurrency", pc.indConc, "--embed-concurrency", pc.embedConc}
	case "tech":
		// --chunk counts PAGES; the worker runs one goroutine per DOMAIN within a chunk, so
		// chunk/(pages per domain) caps in-flight fetches — keep it >> tech-conc (see -tech-chunk).
		return []string{"--tech-engine", "fast", "--concurrency", pc.techConc, "--chunk", pc.techChunk}
	}
	return nil
}

func workerProcessingArgs(pc partCtx, mode string, warcIndex int, outDir string) []string {
	args := append([]string{mode}, passArgs(pc, mode)...)
	args = append(args,
		"--base", pc.base,
		"--crawl-id", pc.crawl,
		"--selection", pc.selection,
		"--part", strconv.Itoa(warcIndex),
		"--whole-warc-threshold", pc.wholeWARCThreshold,
	)
	if pc.s3Anonymous {
		args = append(args, "--s3-anonymous")
	}
	return append(args, "--out", outDir)
}

func selectionForMaxPages(value string) (string, error) {
	pagesPerDomain, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || pagesPerDomain < 1 || pagesPerDomain > 65535 {
		return "", fmt.Errorf("must be an integer from 1 to 65535, got %q", value)
	}
	return fmt.Sprintf("pages%d", pagesPerDomain), nil
}

func validateWholeWARCThreshold(value string) error {
	percentage, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(percentage) || math.IsInf(percentage, 0) {
		return fmt.Errorf("must be a number from 0 to 100, got %q", value)
	}
	if percentage < 0 || percentage > 100 {
		return fmt.Errorf("must be between 0 and 100, got %q", value)
	}
	return nil
}

func parseS3Anonymous(value string) (bool, error) {
	anonymous, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("must be true or false, got %q", value)
	}
	return anonymous, nil
}

func parseRange(s string) (int, int, error) {
	lo, hi := s, s
	if i := strings.IndexByte(s, '-'); i >= 0 {
		lo, hi = s[:i], s[i+1:]
	}
	l, err := strconv.Atoi(strings.TrimSpace(lo))
	if err != nil {
		return 0, 0, err
	}
	h, err := strconv.Atoi(strings.TrimSpace(hi))
	if err != nil {
		return 0, 0, err
	}
	if l < 0 || h < 0 || uint64(l) > uint64(^uint32(0)) || uint64(h) > uint64(^uint32(0)) {
		return 0, 0, fmt.Errorf("WARC indexes must be between 0 and 4294967295")
	}
	if h < l {
		return 0, 0, fmt.Errorf("hi < lo")
	}
	return l, h, nil
}

func firstInt(re *regexp.Regexp, s string) int {
	if m := re.FindStringSubmatch(s); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n
	}
	return 0
}

func sumInt(re *regexp.Regexp, s string) int {
	total := 0
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		n, _ := strconv.Atoi(m[1])
		total += n
	}
	return total
}

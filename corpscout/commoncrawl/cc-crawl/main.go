// cc-crawl drives CommonCrawl enrichment over a range of index parts — the Go replacement for
// run_crawl.sh. For each part it runs, in order: build the worklist (index_builder), then TWO separate
// cc-enrich-worker executions — (1) the pass that PRODUCES out_<mode>_<p>/, then (2) the LOADER
// (load --dir) that pushes it to ClickHouse. Every external command and its exit code are logged as
// structured JSON (log/slog) to stdout AND a file under data/logs; the loader runs ONLY if the produce
// step exited 0 and actually wrote domains.parquet, so a bad/partial produce is never loaded.
//
// It only orchestrates (os/exec) — it does not import cc-enrich-worker. Stdlib only.
//
// Resumable: a part whose out_<mode>_<p>.loaded marker exists is skipped.
//
//	cc-crawl -mode industry -parts 0-299 -crawl CC-MAIN-2026-25     (bare N = single part)
//
// -mode embed is the exception: it REUSES the industry worklist, runs a single embed-only exec (no
// load), and writes just the raw vectors into the sibling data/embedding/out_industry_<p>/ tree. It has
// NO .loaded marker and is NOT wiped before the run — instead the worker opens the existing
// embeddings.parquet and skips the part if it's already a valid, non-empty file (the write is atomic, so
// a readable parquet == a complete run). Used to backfill vectors for already-classified segments.
//
// All settings are flags; each defaults from the same-named env var. -mode, -parts, -crawl required.
package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
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

// Parsed from the worker's own log lines for the per-part counts.
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
	// so the worker + index_builder children get the shared config. Missing file is fine.
	_ = godotenv.Overload(env("DOTENV", ".env"))

	fs := flag.NewFlagSet("cc-crawl", flag.ExitOnError)
	modeF := fs.String("mode", "", "pass to run: industry | tech | embed  (required; embed = vectors only, no ClickHouse)")
	partsF := fs.String("parts", "", "part range lo-hi, or a single part N  (required)")
	crawlF := fs.String("crawl", env("CRAWL", ""), "CommonCrawl crawl id, e.g. CC-MAIN-2026-25  (required; or env CRAWL)")
	dataF := fs.String("data", env("DATA", "data/crawl"), "data dir (shards, output, logs)")
	builderF := fs.String("builder-dir", env("BUILDER_DIR", "index-builder"), "index_builder project dir")
	workerF := fs.String("worker", env("WORKER", "cc-enrich-worker/bin/cc-enrich-worker"), "cc-enrich-worker binary")
	maxPagesF := fs.String("max-pages", env("MAX_PAGES", "25"), "tech: max pages per domain (0 = all)")
	indConcF := fs.String("ind-conc", env("IND_CONC", "64"), "industry: fetch concurrency")
	embedConcF := fs.String("embed-conc", env("EMBED_CONC", "128"), "industry: embed concurrency")
	techConcF := fs.String("tech-conc", env("TECH_CONC", "128"), "tech: concurrency")
	_ = fs.Parse(os.Args[1:])

	fail := func(format string, a ...any) {
		fmt.Fprintf(os.Stderr, "error: "+format+"\n", a...)
		fs.Usage()
		os.Exit(2)
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
	crawl, data, builder, worker, maxPages := *crawlF, *dataF, *builderF, *workerF, *maxPagesF
	// Resolve to absolute paths: index_builder runs with cwd=builder, so a relative --out would land
	// under builder/, not data/. Absolute paths make every child write to the right place.
	for _, p := range []*string{&data, &builder, &worker} {
		if abs, err := filepath.Abs(*p); err == nil {
			*p = abs
		}
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
		lg: lg, data: data, builder: builder, worker: worker, crawl: crawl, maxPages: maxPages,
		indConc: *indConcF, embedConc: *embedConcF, techConc: *techConcF,
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
}

// outcome is what a per-part runner reports back to the main loop for tallying.
type outcome int

const (
	ocFailed outcome = iota
	ocSkipped
	ocDone
)

// partCtx is the shared, per-run configuration handed to each per-part runner.
type partCtx struct {
	lg                                     *slog.Logger
	data, builder, worker, crawl, maxPages string
	indConc, embedConc, techConc           string
}

// ensureWorklist returns the path to shard_<wlMode>_<p>.parquet, building it via index_builder if absent
// (atomic tmp + rename). ok=false means the build failed (the caller should fail the part).
func ensureWorklist(pc partCtx, plg *slog.Logger, wlMode string, p int) (shard string, ok bool) {
	shard = filepath.Join(pc.data, fmt.Sprintf("shard_%s_%d.parquet", wlMode, p))
	if _, err := os.Stat(shard); err == nil {
		return shard, true
	}
	tmp := shard + ".tmp"
	args := []string{"run", "python", "-m", "index_builder",
		"--mode", wlMode, "--max-pages", pc.maxPages, "--crawl", pc.crawl, "--part", strconv.Itoa(p), "--out", tmp}
	plg.Info("worklist.run", "cmd", "uv "+strings.Join(args, " "))
	code, _, el := runStep("uv", args, pc.builder)
	plg.Info("worklist.exit", "exit", code, "elapsed", el.String())
	if code != 0 {
		os.Remove(tmp)
		plg.Error("failed", "step", "worklist", "exit", code)
		return "", false
	}
	os.Rename(tmp, shard)
	return shard, true
}

// runIndustryPart / runTechPart are the load-gated modes: produce → load into ClickHouse → mark. A "done"
// part means produced AND loaded, recorded by the out_<mode>_<p>.loaded marker (the load is a remote side
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
	}
	shard, ok := ensureWorklist(pc, plg, mode, p)
	if !ok {
		return ocFailed
	}

	// Clear any stale/partial output (the worker requires an empty --out); safe — a done part already
	// returned above on its marker.
	os.RemoveAll(outDir)
	pArgs := append(append([]string{mode}, passArgs(mode, pc.indConc, pc.embedConc, pc.techConc)...),
		"--worklist", shard, "--crawl-id", pc.crawl, "--out", outDir)
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
		plg.Warn("marker.write", "err", err.Error())
	}
	plg.Info("done", "produced", produced, "rows_to_clickhouse", rows)
	return ocDone
}

// runEmbedPart is the embed-only mode: REUSE the industry worklist, run ONE embed exec, no load, no
// marker. There is deliberately NO RemoveAll — the worker opens the existing embeddings.parquet and skips
// the part if it's already complete (the write is atomic, so a readable parquet == a complete run);
// wiping it would defeat that. Vectors land in the sibling data/embedding/ tree.
func runEmbedPart(pc partCtx, p int) outcome {
	plg := pc.lg.With("mode", "embed", "part", p)
	shard, ok := ensureWorklist(pc, plg, "industry", p) // embed reuses the industry worklist
	if !ok {
		return ocFailed
	}
	outDir := filepath.Join(filepath.Dir(pc.data), "embedding", fmt.Sprintf("out_industry_%d", p))
	pArgs := append(append([]string{"embed"}, passArgs("embed", pc.indConc, pc.embedConc, pc.techConc)...),
		"--worklist", shard, "--crawl-id", pc.crawl, "--out", outDir)
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
func passArgs(mode, indConc, embedConc, techConc string) []string {
	switch mode {
	case "industry", "embed": // embed runs the same fetch→embed stream, just without classify
		return []string{"--concurrency", indConc, "--embed-concurrency", embedConc}
	case "tech":
		return []string{"--tech-engine", "fast", "--concurrency", techConc}
	}
	return nil
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

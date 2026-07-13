package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dustin/go-humanize"
	"github.com/fsnotify/fsnotify"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/classify"
	"cc-enrich-worker/internal/embed"
	"cc-enrich-worker/internal/load"
	mdl "cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
	"cc-enrich-worker/internal/warcinput"
	"cc-enrich-worker/internal/worker"
	"cc-raw/fetch"
)

const warcPreparationTimeout = 30 * time.Minute

// maxFetchErrorRate is the failure contract for every fetch pass: above it the part is systemic
// failure (throttling, auth, dead source), so refuse to write and let cc-crawl retry the part.
// Normal WARC decay is a few %.
const maxFetchErrorRate = 0.5

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func usage() {
	fmt.Fprint(os.Stderr, `cc-enrich-worker — enrich CommonCrawl domains into the corpscout database

Usage:
  cc-enrich-worker <command> [flags]

Commands:
  industry   embed each domain's page + classify NACE industry  (GPU-bound; needs ClickHouse + embed endpoint)
  embed      embed each domain's page, save the raw vector only — no NACE, no ClickHouse  (GPU + embed endpoint)
  tech       fingerprint technologies + extract identifiers/profiles  (CPU-bound; S3 only)
  both       run both in one fetch pass  (discouraged — co-locating throttles the GPU; prefer two processes)
  load       load already-produced Parquet into ClickHouse: --dir/--file one output, or --scan a root of .produced markers (--watch to keep sweeping)
  plan       read-only report: local/remote lane split + threshold sweep for a WARC part range
  status     read-only report: per-command produced/loaded/pending marker counts + oldest pending age

Run "cc-enrich-worker <command> -h" to see that command's flags.

Config is via environment (see ../.env.example): COMMONCRAWL_CATALOG_S3_BASE and CORPSCOUT_S3_*
for the RustFS catalog, COMMONCRAWL_EMBED_*, CLICKHOUSE_*, AWS_* for signed Common Crawl S3 access,
and CC_BASE_URL for anonymous HTTPS access.
`)
}

// opts holds the parsed flags for a run. Mode-specific fields are only registered (and meaningful)
// for the relevant command.
type opts struct {
	crawlID, out, base, selection string
	part                          int
	concurrency, chunk            int
	anonymous                     bool
	wholeWARCThreshold            float64
	techEngine                    string // tech / both
	techMax                       int    // tech / both
	batch, embedConc              int    // industry / both
}

// parse builds a per-command flag set (so tech flags don't show under industry and vice-versa). It
// serves both the single-part run (--part) and the range runner (--parts + --mode). When --parts is
// set it returns isRange=true and a populated runnerOpts; the two selectors are mutually exclusive.
func parse(mode string, args []string) (opts, runnerOpts, bool) {
	fs := flag.NewFlagSet("cc-enrich-worker "+mode, flag.ExitOnError)
	var o opts
	// common to every command
	fs.StringVar(&o.crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 — stamped on every row (required)")
	fs.StringVar(&o.selection, "selection", "pages25", "catalog selection identity")
	fs.IntVar(&o.part, "part", -1, "zero-based WARC index (single-part run; mutually exclusive with --parts)")
	fs.StringVar(&o.out, "out", "", "output DIRECTORY for the Parquet files (default <base>/<crawl-id>/warc/<selection>/out_<mode>_<part>; an explicit dir must be empty)")
	fs.StringVar(&o.base, "base", os.Getenv("OUT_BASE_DIR"), "output ROOT (or env OUT_BASE_DIR); required")
	fs.IntVar(&o.concurrency, "concurrency", 32, "industry/embed: pages in flight; tech/both: DOMAINS in flight, each fetching up to 8 pages in parallel (total fetches = concurrency x 8)")
	fs.IntVar(&o.chunk, "chunk", 1024, "catalog pages per process chunk (tech/both)")
	fs.Float64Var(&o.wholeWARCThreshold, "whole-warc-threshold", 50, "selected compressed-byte percentage that triggers one whole-WARC download (0..100)")
	fs.BoolVar(&o.anonymous, "s3-anonymous", false, "fetch through anonymous HTTPS instead of signed Common Crawl S3")
	if mode == "tech" || mode == "both" {
		fs.StringVar(&o.techEngine, "tech-engine", "fast", "tech matcher: fast (Aho-Corasick gated) | wappalyzer (upstream full scan)")
		fs.IntVar(&o.techMax, "tech-max-bytes", 0, "maximum page bytes scanned for technologies (0 = full page)")
	}
	if mode == "industry" || mode == "both" || mode == "embed" {
		fs.IntVar(&o.batch, "embed-batch", embed.DefaultBatch, "texts per embed request — keep small (big batches overflow the engine's token budget)")
		fs.IntVar(&o.embedConc, "embed-concurrency", 96, "concurrent embed requests in flight (saturate the GPU)")
	}
	// Range runner: process a whole WARC part range through the bounded lane pool.
	var partsFlag, modeFlag string
	var ro runnerOpts
	fs.StringVar(&partsFlag, "parts", "", `WARC part range "A-B" or "N" (range runner; mutually exclusive with --part)`)
	fs.StringVar(&modeFlag, "mode", "", "range runner lane: local|remote (required with --parts)")
	fs.Int64Var(&ro.remoteMaxPages, "remote-max-pages", 0, "range split threshold: parts with <= this many selected pages are remote-eligible (required >=1 for tech/both)")
	fs.IntVar(&ro.warcParallel, "warc-parallel", 4, "range remote lane: parts produced concurrently (>=1)")
	fs.IntVar(&ro.downloadParallel, "download-parallel", 2, "range local lane: whole-WARC downloads in flight (>=1)")
	fs.IntVar(&ro.processParallel, "process-parallel", 2, "range local lane: downloaded WARCs processed in flight (>=1)")
	fs.IntVar(&ro.maxWARCFiles, "max-warc-files", 0, "range local lane: max whole WARC files on disk at once (required >=1 for --mode local)")
	_ = fs.Parse(args)
	if o.crawlID == "" {
		fmt.Fprintln(os.Stderr, "error: --crawl-id is required")
		fs.Usage()
		os.Exit(2)
	}
	if math.IsNaN(o.wholeWARCThreshold) || math.IsInf(o.wholeWARCThreshold, 0) ||
		o.wholeWARCThreshold < 0 || o.wholeWARCThreshold > 100 {
		fmt.Fprintln(os.Stderr, "error: --whole-warc-threshold must be between 0 and 100")
		fs.Usage()
		os.Exit(2)
	}

	if partsFlag != "" {
		if o.part >= 0 {
			fmt.Fprintln(os.Stderr, "error: --part and --parts are mutually exclusive")
			fs.Usage()
			os.Exit(2)
		}
		if modeFlag == "" {
			fmt.Fprintln(os.Stderr, "error: --parts requires --mode (local|remote)")
			fs.Usage()
			os.Exit(2)
		}
		parts, err := parsePartsRange(partsFlag)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid --parts: %v\n", err)
			fs.Usage()
			os.Exit(2)
		}
		ro.parts = parts
		ro.mode = modeFlag
		if err := validateRunnerOpts(mode, ro); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			fs.Usage()
			os.Exit(2)
		}
		return o, ro, true
	}

	if o.part < 0 || uint64(o.part) > uint64(^uint32(0)) {
		fmt.Fprintln(os.Stderr, "error: --part must be a WARC index from 0 to 4294967295")
		fs.Usage()
		os.Exit(2)
	}
	return o, ro, false
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch cmd := os.Args[1]; cmd {
	case "industry", "tech", "both", "embed":
		o, ro, isRange := parse(cmd, os.Args[2:])
		if isRange {
			runRange(cmd, o, ro)
		} else {
			run(cmd, o)
		}
	case "load":
		runLoad(os.Args[2:])
	case "plan":
		runPlanCmd(os.Args[2:])
	case "status":
		runStatusCmd(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

// runLoad implements the `load` command: push already-produced Parquet output into ClickHouse over
// the native driver (no clickhouse-client needed). `--dir` loads every output file in a folder (a
// shard's output dir); `--file` loads one. The kind→table mapping comes from the FIXED filenames.
func runLoad(args []string) {
	fs := flag.NewFlagSet("cc-enrich-worker load", flag.ExitOnError)
	dir := fs.String("dir", "", "a shard output directory; loads every supported output parquet in it")
	file := fs.String("file", "", "a single Parquet output file to load")
	kind := fs.String("kind", "", "override the kind for --file: "+strings.Join(load.Kinds, "|"))
	scan := fs.String("scan", "", "a producer output root; sweep it for .produced dirs lacking .loaded and load each")
	watch := fs.Bool("watch", false, "with --scan, keep sweeping: after each pass wait for a filesystem event under root or a 5-minute tick")
	parallel := fs.Int("parallel", 4, "with --scan, how many output dirs to load concurrently")
	_ = fs.Parse(args)

	// --scan is mutually exclusive with --dir/--file; --dir/--file remain mutually exclusive with each other.
	if *scan != "" {
		if *dir != "" || *file != "" {
			fmt.Fprintln(os.Stderr, "error: --scan cannot be combined with --dir or --file")
			fs.Usage()
			os.Exit(2)
		}
	} else if (*dir == "") == (*file == "") {
		fmt.Fprintln(os.Stderr, "error: pass exactly one of --scan, --dir, or --file")
		fs.Usage()
		os.Exit(2)
	}
	// --watch only governs the --scan sweep loop; without --scan it would be silently ignored.
	if *watch && *scan == "" {
		fmt.Fprintln(os.Stderr, "error: --watch requires --scan")
		fs.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	conn, err := chConnect(ctx)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	defer conn.Close()

	if *scan != "" {
		if *watch {
			watchLoad(ctx, conn, *scan, *parallel)
			return
		}
		res, err := load.Sweep(ctx, conn, *scan, *parallel)
		if err != nil {
			log.Fatalf("scan %s: %v", *scan, err)
		}
		log.Printf("sweep %s: loaded=%d failed=%d pending=%d skipped=%d", *scan, res.Loaded, res.Failed, res.Pending, res.Skipped)
		if res.Failed > 0 {
			os.Exit(1)
		}
		return
	}

	if *dir != "" {
		results, err := load.FromDir(ctx, conn, *dir)
		if err != nil {
			log.Fatalf("load %s: %v", *dir, err)
		}
		for _, r := range results {
			log.Printf("loaded %d rows: %s -> %s", r.Rows, r.Path, r.Table)
		}
		return
	}
	table, n, err := load.FromFile(ctx, conn, *file, *kind)
	if err != nil {
		log.Fatalf("load %s: %v", *file, err)
	}
	log.Printf("loaded %d rows: %s -> %s", n, *file, table)
}

// watchLoad runs Sweep in a loop. The 5-minute ticker is the correctness backstop (a sweep always
// happens); fsnotify Create/Rename events under root only cut latency. If the watcher can't be set
// up it logs ONE warning and degrades to pure ticker polling.
func watchLoad(ctx context.Context, conn driver.Conn, root string, parallel int) {
	trigger := make(chan struct{}, 1)
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("warning: fsnotify unavailable (%v); falling back to 5-minute polling", err)
		watcher = nil
	} else if err := watcher.Add(root); err != nil {
		log.Printf("warning: cannot watch %s (%v); falling back to 5-minute polling", root, err)
		watcher.Close()
		watcher = nil
	}
	if watcher != nil {
		defer watcher.Close()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case ev, ok := <-watcher.Events:
					if !ok {
						return
					}
					if ev.Op&(fsnotify.Create|fsnotify.Rename) != 0 {
						select {
						case trigger <- struct{}{}:
						default:
						}
					}
				case _, ok := <-watcher.Errors:
					if !ok {
						return
					}
				}
			}
		}()
	}

	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		res, err := load.Sweep(ctx, conn, root, parallel)
		if err != nil {
			log.Printf("sweep %s: %v", root, err)
		} else {
			log.Printf("sweep %s: loaded=%d failed=%d pending=%d skipped=%d", root, res.Loaded, res.Failed, res.Pending, res.Skipped)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-trigger:
		}
	}
}

func chConnect(ctx context.Context) (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_HOST", "localhost") + ":" + envOr("CLICKHOUSE_NATIVE_PORT", "9000")},
		Auth: clickhouse.Auth{
			Database: envOr("CLICKHOUSE_DATABASE", "corpscout"),
			Username: envOr("CLICKHOUSE_USER", "default"),
			Password: envOr("CLICKHOUSE_PASSWORD", ""),
		},
	})
}

// detectModel asks the OpenAI-compatible endpoint for its served model id.
func detectModel(baseURL string) (string, error) {
	resp, err := http.Get(baseURL + "/models")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var parsed struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", err
	}
	if len(parsed.Data) == 0 {
		return "", fmt.Errorf("no models served at %s", baseURL)
	}
	return parsed.Data[0].ID, nil
}

// parquetRows returns a Parquet file's row count by reading only its footer. It errors if the file is
// missing or not a valid/complete Parquet (e.g. a write killed mid-flush) — used by embed verify-and-skip.
func parquetRows(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		return 0, err
	}
	return pf.NumRows(), nil
}

func completedEmbedding(outDir string) (string, int64, bool) {
	for _, name := range []string{"embeddings.parquet", "embeddings_fp16.parquet"} {
		path := filepath.Join(outDir, name)
		rows, err := parquetRows(path)
		if err == nil {
			if rows > 0 {
				return path, rows, true
			}
			if _, markerErr := os.Stat(path + ".empty"); markerErr == nil {
				return path, 0, true
			}
			continue
		}
		// Converted fp16 files from the older toolchain may have a logical type parquet-go cannot
		// decode. Their conversion step verified the file before removing fp32, so retain that fallback.
		if name == "embeddings_fp16.parquet" {
			info, statErr := os.Stat(path)
			if statErr == nil && info.Size() > 0 {
				return path, 0, true
			}
		}
	}
	return "", 0, false
}

// partDeps are the per-process resources shared by every part in a run: the parsed opts, the
// embedder + NACE reference (industry/embed/both), and the object getter. Built ONCE and passed to
// producePart. The range lanes call producePart concurrently over a SINGLE partDeps, so every field
// must be safe for concurrent reads: opts/mode are immutable value copies; emb (embed client),
// ref/protos (read-only reference data), and objects (S3/HTTP getter) are all concurrent-safe.
// Anything per-part (the opened input, output dir, temp dir, accumulators) stays local to producePart.
type partDeps struct {
	mode string
	o    opts

	emb    classify.Embedder
	ref    *mdl.Reference
	protos *mdl.Prototypes

	objects fetch.ObjectGetter
	source  string
}

// partResult reports what a single part produced. Rows maps parquet kind names (the loader's table
// kinds in load.Kinds) to row counts for the .produced marker + loader verify; Domains/Embeds drive
// the done log and the range summary.
type partResult struct {
	Rows            map[string]int
	Domains, Embeds int
}

// preparedPart is a WARC input opened for one part, carried from openInput to processInput so the
// local lane (Task 7) can download in one pool and process in another.
//
// Cleanup contract: on the normal path processInput owns the input's close + temp-dir removal (its
// deferred cleanupInput). But a preparedPart that is DROPPED between the two stages — never reaching
// processInput, e.g. a breaker trip or context cancel in the local lane — is NOT cleaned up by
// processInput; whoever drops it MUST close input and remove warcTempDirectory itself (see
// cleanupPrepared in runrange.go). Because a dropped part never reaches processInput, there is no
// double close/remove.
type preparedPart struct {
	input             *warcinput.Input
	outDir            string
	warcTempDirectory string
}

// fetchConcurrencyFor sizes the shared S3/HTTP transport connection budget. The per-part base is the
// domain pool (o.concurrency), multiplied by the per-domain page pool (worker.PageConcurrency) for
// tech/both since those fetch PageConcurrency pages per in-flight domain. partsParallel scales that
// by how many parts share the ONE transport concurrently: 1 for a single --part run; the range
// runner passes its lane's parts-parallelism (spec §2: X * concurrency * PageConcurrency) so that,
// e.g., --warc-parallel 4 does not squeeze 4 parts through one part's connection budget.
func fetchConcurrencyFor(mode string, concurrency, partsParallel int) int {
	if partsParallel < 1 {
		partsParallel = 1
	}
	c := concurrency
	if mode == "tech" || mode == "both" {
		c *= worker.PageConcurrency
	}
	return c * partsParallel
}

// buildPartDeps constructs the once-per-process resources. Unlike the old inline setup it does not
// gate on plan emptiness — the plan is loaded per part inside producePart, the range runner only
// dispatches non-empty parts, and a non-empty single --part run builds exactly the same resources.
//
// partsParallel is how many parts will share the single transport concurrently (1 for a single
// --part run; the range lane's parts-parallelism otherwise) — see fetchConcurrencyFor.
func buildPartDeps(ctx context.Context, mode string, o opts, partsParallel int) (partDeps, error) {
	d := partDeps{mode: mode, o: o}

	if mode == "tech" || mode == "both" { // tech matcher only needed when we fingerprint
		switch o.techEngine {
		case "fast":
			fm, err := tech.NewFastMatcher()
			if err != nil {
				return partDeps{}, fmt.Errorf("build fast tech matcher: %w", err)
			}
			tech.SetFastMatcher(fm)
			log.Printf("tech engine: fast (Aho-Corasick gated)")
		case "wappalyzer":
			log.Printf("tech engine: wappalyzer (upstream full scan)")
		default:
			return partDeps{}, fmt.Errorf("--tech-engine must be fast|wappalyzer, got %q", o.techEngine)
		}
	}

	// The embedder is needed whenever we embed pages (industry/both/embed). The NACE reference +
	// ClickHouse are needed only when we ALSO classify (industry/both) — embed-only skips them.
	if mode != "tech" {
		baseURL := envOr("COMMONCRAWL_EMBED_BASE_URL", "http://localhost:8000/v1")
		model := envOr("COMMONCRAWL_EMBED_MODEL", "")
		if model == "" {
			var derr error
			if model, derr = detectModel(baseURL); derr != nil {
				return partDeps{}, fmt.Errorf("detect embed model: %w", derr)
			}
		}
		log.Printf("embed endpoint=%s model=%s", baseURL, model)
		d.emb = embed.NewEmbedClient(baseURL, model, o.batch, o.embedConc, envInt("COMMONCRAWL_EMBED_MAX_CHARS", 0))

		if mode == "embed" {
			log.Printf("embed-only mode: no NACE reference, no ClickHouse, no load")
		} else {
			conn, err := chConnect(ctx)
			if err != nil {
				return partDeps{}, fmt.Errorf("clickhouse connect: %w", err)
			}
			ref, protos, err := classify.LoadReference(ctx, conn)
			if err != nil {
				conn.Close()
				return partDeps{}, fmt.Errorf("load reference: %w", err)
			}
			meta, metaErr := classify.LoadReferenceMeta(ctx, conn)
			conn.Close()
			if metaErr != nil {
				return partDeps{}, fmt.Errorf("load reference meta: %w", metaErr)
			}
			if len(ref.M) == 0 {
				return partDeps{}, fmt.Errorf("empty NACE reference — rebuild corpscout.nace_category_embeddings")
			}
			log.Printf("reference: nace=%d page_types=%d dim=%d model=%s", len(ref.M), len(protos.P), len(ref.M[0]), meta.Model)

			// Fail fast unless the reference covers every NACE category and was built with the same
			// model + dim this worker will embed pages with (the matching-vector-space invariant).
			if err := classify.VerifyReference(meta, model, d.emb); err != nil {
				return partDeps{}, fmt.Errorf("reference check failed: %w", err)
			}
			log.Printf("reference check OK: %d/%d categories, model=%s dim=%d", meta.Count, meta.Expected, meta.Model, meta.Dim)
			d.ref, d.protos = ref, protos
		}
	}

	// Transport sizing = true in-flight range reads. Whole-WARC mode makes one streaming GET and
	// then uses concurrent local ReadAt calls through the same RangeGetter boundary.
	fetchConcurrency := fetchConcurrencyFor(mode, o.concurrency, partsParallel)
	if o.anonymous {
		d.objects = fetch.NewHTTPGetter(envOr("CC_BASE_URL", ""), fetchConcurrency)
		d.source = "anonymous_https"
	} else {
		s3Getter, err := fetch.NewS3Getter(ctx, envOr("AWS_REGION", "us-east-1"), fetchConcurrency)
		if err != nil {
			return partDeps{}, fmt.Errorf("Common Crawl S3 init: %w", err)
		}
		d.objects = s3Getter
		d.source = "signed_s3"
	}
	return d, nil
}

// openInput loads one part's plan, prepares the WARC input, and logs "WARC input ready". forced ==
// "" keeps the legacy threshold decision (single --part); a range lane passes warcinput.ModeRange or
// warcinput.ModeWholeFile to force the strategy. On failure it cleans up any stale/partial temp dir.
func openInput(ctx context.Context, d partDeps, part uint32, forced warcinput.Mode, outDir string) (preparedPart, error) {
	o := d.o
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return preparedPart{}, fmt.Errorf("create output dir %s: %w", outDir, err)
	}
	catalogS3Base := strings.TrimSpace(os.Getenv("COMMONCRAWL_CATALOG_S3_BASE"))

	primaryPagesOnly := d.mode == "industry" || d.mode == "embed"
	catalogCachePath := filepath.Join(o.base, o.crawlID, "warc-index", o.selection, "catalog.duckdb")
	catalogStarted := time.Now()
	log.Printf(
		"catalog: checking RustFS commit and local cache crawl=%s selection=%s path=%s",
		o.crawlID,
		o.selection,
		catalogCachePath,
	)
	// With no RustFS base configured we read the already-synced LOCAL catalog directly (same path the
	// S3 sync writes to). This mirrors plancmd's "sync only if a base is set" contract and lets the
	// range runner operate on a catalog that was synced once up front rather than per part.
	var plan warcinput.Plan
	var err error
	if catalogS3Base == "" {
		plan, err = warcinput.LoadPlan(o.base, o.crawlID, o.selection, part, primaryPagesOnly)
	} else {
		plan, err = warcinput.LoadS3Plan(
			ctx,
			catalog.S3Config{
				BaseURI:   catalogS3Base,
				Endpoint:  os.Getenv("CORPSCOUT_S3_ENDPOINT"),
				Region:    envOr("CORPSCOUT_S3_REGION", "us-east-1"),
				AccessKey: os.Getenv("CORPSCOUT_S3_ACCESS_KEY"),
				SecretKey: os.Getenv("CORPSCOUT_S3_SECRET_KEY"),
			},
			o.base,
			o.crawlID,
			o.selection,
			part,
			primaryPagesOnly,
		)
	}
	if err != nil {
		return preparedPart{}, fmt.Errorf("load WARC catalog: %w", err)
	}
	log.Printf(
		"catalog: ready warc_index=%d selected_pages=%d elapsed=%s",
		plan.WARCIndex,
		len(plan.Items),
		time.Since(catalogStarted).Round(time.Millisecond),
	)

	var s3Before fetch.S3Stats
	if s3Getter, ok := d.objects.(*fetch.S3Getter); ok {
		s3Before = s3Getter.Stats()
	}
	prepareStarted := time.Now()
	warcTempDirectory := filepath.Join(outDir, ".warc-input")
	if err := os.RemoveAll(warcTempDirectory); err != nil {
		return preparedPart{}, fmt.Errorf("remove stale WARC input directory: %w", err)
	}
	prepareContext, cancelPrepare := context.WithTimeout(ctx, warcPreparationTimeout)
	var input *warcinput.Input
	if forced == "" {
		input, err = plan.Open(prepareContext, d.objects, "commoncrawl", o.wholeWARCThreshold, warcTempDirectory)
	} else {
		input, err = plan.OpenAs(prepareContext, d.objects, "commoncrawl", forced, warcTempDirectory)
	}
	cancelPrepare()
	if err != nil {
		if cleanupErr := os.RemoveAll(warcTempDirectory); cleanupErr != nil {
			log.Printf("remove failed WARC input directory: %v", cleanupErr)
		}
		return preparedPart{}, fmt.Errorf("prepare WARC input: %w", err)
	}
	prepareElapsed := time.Since(prepareStarted)
	coveragePercent := 0.0
	if input.ObjectBytes > 0 {
		coveragePercent = 100 * float64(input.SelectedBytes) / float64(input.ObjectBytes)
	}
	inputLog := []any{
		"crawl", o.crawlID,
		"selection", o.selection,
		"warc_index", input.WARCIndex,
		"warc_filename", input.WARCFilename,
		"mode", input.Mode,
		"source", d.source,
		"selected_pages", len(input.Items),
		"selected_bytes", input.SelectedBytes,
		"selected_size", humanize.Bytes(uint64(input.SelectedBytes)),
		"object_bytes", input.ObjectBytes,
		"object_size", humanize.Bytes(uint64(input.ObjectBytes)),
		"utilization_percent", coveragePercent,
		"whole_warc_threshold_percent", o.wholeWARCThreshold,
		"prepare_seconds", prepareElapsed.Seconds(),
	}
	if s3Getter, ok := d.objects.(*fetch.S3Getter); ok {
		stats := s3Getter.Stats().Delta(s3Before)
		sdkRetryAttempts := stats.HTTPAttempts - stats.HeadObjectCalls - stats.GetObjectCalls
		if sdkRetryAttempts < 0 {
			sdkRetryAttempts = 0
		}
		requestsPerSecond := 0.0
		if prepareElapsed > 0 {
			requestsPerSecond = float64(stats.HTTPAttempts) / prepareElapsed.Seconds()
		}
		inputLog = append(inputLog,
			"head_object_calls", stats.HeadObjectCalls,
			"head_object_ms", float64(stats.HeadObjectTime.Microseconds())/1000,
			"get_object_calls", stats.GetObjectCalls,
			"http_attempts", stats.HTTPAttempts,
			"requests_per_second", requestsPerSecond,
			"sdk_retry_attempts", sdkRetryAttempts,
			"http_429", stats.HTTP429s,
			"http_503", stats.HTTP503s,
			"body_bytes", stats.BodyBytes,
			"body_size", humanize.Bytes(uint64(stats.BodyBytes)),
			"body_read_errors", stats.BodyReadErrors,
			"body_read_retries", stats.BodyReadRetries,
		)
	}
	if input.Mode == warcinput.ModeWholeFile && prepareElapsed > 0 {
		inputLog = append(inputLog, "download_mib_per_second",
			float64(input.ObjectBytes)/(1024*1024)/prepareElapsed.Seconds())
	}
	slog.InfoContext(ctx, "WARC input ready", inputLog...)

	return preparedPart{input: input, outDir: outDir, warcTempDirectory: warcTempDirectory}, nil
}

// producePart processes one WARC part end to end: open the input (forced or threshold-decided),
// process + write its outputs, clean up, and return the row counts. Every failure returns an error
// (the deferred cleanup still runs); the caller decides whether to fatal (single --part) or record
// and continue (range lanes).
func producePart(ctx context.Context, d partDeps, part uint32, forced warcinput.Mode, outDir string) (partResult, error) {
	prepared, err := openInput(ctx, d, part, forced, outDir)
	if err != nil {
		return partResult{}, err
	}
	return processInput(ctx, d, prepared)
}

// processInput runs the post-open remainder of producePart: process the opened WARC, write outputs,
// clean up, log "done", and report row counts. Split from openInput so the local lane (Task 7) can
// download and process in separate pools.
func processInput(ctx context.Context, d partDeps, prepared preparedPart) (partResult, error) {
	o := d.o
	mode := d.mode
	input := prepared.input
	outDir := prepared.outDir
	warcTempDirectory := prepared.warcTempDirectory

	cleanupInput := func() error {
		closeErr := input.Close()
		removeErr := os.RemoveAll(warcTempDirectory)
		if closeErr != nil && removeErr != nil {
			return fmt.Errorf("close WARC input: %v; remove input directory: %w", closeErr, removeErr)
		}
		if closeErr != nil {
			return closeErr
		}
		if removeErr != nil {
			return fmt.Errorf("remove WARC input directory: %w", removeErr)
		}
		return nil
	}
	defer func() {
		if err := cleanupInput(); err != nil {
			log.Printf("clean up WARC input: %v", err)
		}
	}()

	getter := input.Getter
	items := input.Items
	if mode == "tech" || mode == "both" {
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].RootDomain != items[j].RootDomain {
				return items[i].RootDomain < items[j].RootDomain
			}
			if items[i].Primary != items[j].Primary {
				return items[i].Primary
			}
			return items[i].Offset < items[j].Offset
		})
	}

	cfg := worker.ShardConfig{
		CrawlID: o.crawlID, SourceRunID: fmt.Sprintf("go-%d", time.Now().Unix()),
		ResolvedAt: time.Now().UTC(), Concurrency: o.concurrency, TechMaxBytes: o.techMax,
		EmbedConcurrency: o.embedConc, EmbedBatch: o.batch, Mode: mode,
		EmbedOnly: mode == "embed",
	}
	start := time.Now()
	// Accumulators for the industry/embed path, which builds ONE ShardResult for the whole shard. The
	// tech/both path does NOT accumulate — it streams each chunk straight to parquet (see the else branch),
	// so a huge shard never buffers a whole part's rows in RAM.
	var domains []output.DomainRow
	var industries []output.IndustryRow
	var pageSignals []output.PageSignalRow
	var embeddings []output.EmbeddingRow

	rows := map[string]int{}          // parquet kind -> row count for the .produced marker
	streamed := false                 // tech/both writes its own files (streaming) and skips the shared block
	finalDomains, finalEmbeds := 0, 0 // for the done log (the accumulator slices are empty on the streamed path)

	// Industry: one continuous fetch→embed stream over the whole shard, so the GPU is fed
	// non-stop (no per-chunk "embed all then wait" barrier). tech/both keep the chunk pipeline.
	if mode == "industry" || mode == "embed" {
		res, err := worker.ProcessIndustryStream(ctx, items, getter, d.emb, d.ref, d.protos, cfg)
		if err != nil {
			return partResult{}, fmt.Errorf("industry stream: %w", err)
		}
		if res.Pages > 0 && float64(res.Errs)/float64(res.Pages) > maxFetchErrorRate {
			return partResult{}, fmt.Errorf("refusing to write: fetch error rate %.0f%% (%d/%d pages) exceeds %.0f%% — WARC index will be retried",
				100*float64(res.Errs)/float64(res.Pages), res.Errs, res.Pages, 100*maxFetchErrorRate)
		}
		domains = res.Domains
		industries = res.Industries
		pageSignals = res.PageSignals
		embeddings = res.Embeddings
		unit, n := "domains", len(domains)
		if mode == "embed" {
			unit, n = "embeddings", len(embeddings)
		}
		log.Printf("progress: %d/%d pages, %d %s (%.1f pages/s)",
			len(items), len(items), n, unit, float64(len(items))/time.Since(start).Seconds())
	} else {
		// Streaming write: the chunked tech/both path writes each chunk's rows as ONE parquet row group
		// immediately (worker.ShardStreamer) instead of buffering the whole shard, so peak memory is
		// O(chunk) not O(part). A 250k-domain (~2.75M-page) shard would otherwise buffer ~80GB+ of rows.
		runEmbedChunk := mode == "both" // "both" also emits vectors -> the separate data/embedding tree
		embedDir := ""
		if runEmbedChunk {
			embedDir = filepath.Join(o.base, o.crawlID, "embedding", "warc", o.selection, filepath.Base(outDir))
		}
		streamer, serr := worker.NewShardStreamer(outDir, embedDir, runEmbedChunk, true /*runTech*/)
		if serr != nil {
			return partResult{}, fmt.Errorf("open shard writers: %w", serr)
		}
		// Items were sorted by root domain above. Snap every chunk to a domain boundary so a domain's pages
		// cannot aggregate twice into conflicting DomainRows. The next chunk is read while the current one
		// finalizes and writes; the reader may be Common Crawl ranges or a downloaded local WARC.
		chunkEnd := func(i int) int {
			e := i + o.chunk
			if e >= len(items) {
				return len(items)
			}
			for e < len(items) && items[e].RootDomain == items[e-1].RootDomain {
				e++ // extend to the next domain change
			}
			return e
		}
		var fetched worker.FetchedChunk
		var totalPages, totalErrs int64
		curLo, curHi := 0, 0
		if len(items) > 0 {
			curHi = chunkEnd(0)
			fetched = worker.FetchChunk(ctx, items[0:curHi], getter, cfg)
		}
		for curLo < curHi {
			cur := fetched
			totalPages += cur.Pages
			totalErrs += cur.Errs
			nextLo, nextHi := curHi, 0
			var nextCh chan worker.FetchedChunk
			if nextLo < len(items) { // prefetch the next chunk while this one finalizes + writes
				nextHi = chunkEnd(nextLo)
				nextCh = make(chan worker.FetchedChunk, 1)
				go func() { nextCh <- worker.FetchChunk(ctx, items[nextLo:nextHi], getter, cfg) }()
			}
			res, ferr := worker.Finalize(ctx, cur, d.emb, d.ref, d.protos, cfg)
			if ferr != nil {
				streamer.Abort()
				return partResult{}, fmt.Errorf("process chunk [%d:%d]: %w", curLo, curHi, ferr)
			}
			if werr := streamer.Write(res); werr != nil {
				streamer.Abort()
				return partResult{}, fmt.Errorf("write chunk [%d:%d]: %w", curLo, curHi, werr)
			}
			el := time.Since(start).Seconds()
			log.Printf("progress: %d/%d pages, %d domains written (%.1f pages/s)",
				curHi, len(items), streamer.DomainCount(), float64(curHi)/el)
			if nextCh != nil {
				fetched = <-nextCh
			}
			curLo, curHi = nextLo, nextHi
		}
		// Failure contract: abort (delete partials) rather than commit a systemically-failed part. cc-crawl
		// gates on exit code, so a non-nil error here retries the part next run.
		if totalPages > 0 && float64(totalErrs)/float64(totalPages) > maxFetchErrorRate {
			streamer.Abort()
			return partResult{}, fmt.Errorf("refusing to write: fetch error rate %.0f%% (%d/%d pages) exceeds %.0f%% — WARC index will be retried",
				100*float64(totalErrs)/float64(totalPages), totalErrs, totalPages, 100*maxFetchErrorRate)
		}
		if len(items) > 0 && streamer.DomainCount() == 0 {
			streamer.Abort()
			return partResult{}, fmt.Errorf("refusing to write: 0 domains from %d catalog pages (systemic parse failure?) — WARC index will be retried", len(items))
		}
		_, cerr := streamer.Commit()
		if cerr != nil {
			return partResult{}, fmt.Errorf("commit shard writers: %w", cerr)
		}
		finalDomains = streamer.DomainCount()
		finalEmbeds = streamer.EmbeddingCount()
		rows["domains"] = finalDomains
		streamed = true
	}

	if !streamed {
		// Failure contract (industry/embed): refuse to write when non-empty catalog input produced nothing.
		produced := len(domains)
		if mode == "embed" {
			produced = len(embeddings)
		}
		if len(items) > 0 && produced == 0 {
			return partResult{}, fmt.Errorf("refusing to write: 0 outputs from %d catalog pages (systemic fetch failure?) — WARC index will be retried", len(items))
		}
		write := func(name string, fn func(string) error) error {
			p := filepath.Join(outDir, name)
			if err := fn(p); err != nil {
				return fmt.Errorf("write %s: %w", name, err)
			}
			return nil
		}
		if mode == "industry" { // domains master + classification; embed writes only the vectors below
			if err := write("domains.parquet", func(p string) error { return output.WriteDomains(p, domains) }); err != nil {
				return partResult{}, err
			}
			if err := write("industries.parquet", func(p string) error { return output.WriteIndustries(p, industries) }); err != nil {
				return partResult{}, err
			}
			if err := write("page_signals.parquet", func(p string) error { return output.WritePageSignals(p, pageSignals) }); err != nil {
				return partResult{}, err
			}
			rows["domains"] = len(domains)
			rows["industries"] = len(industries)
			rows["page_signals"] = len(pageSignals)
		}
		// Embeddings are the expensive GPU artifact — kept large, never loaded into ClickHouse. For embed
		// mode --out IS the embed dir; for industry, write to the SEPARATE sibling tree
		// <crawl>/warc/<selection>/<stem> -> <crawl>/embedding/warc/<selection>/<stem>.
		if len(embeddings) > 0 || mode == "embed" {
			embedDir := outDir
			if mode != "embed" {
				embedDir = filepath.Join(o.base, o.crawlID, "embedding", "warc", o.selection, filepath.Base(outDir))
			}
			if err := os.MkdirAll(embedDir, 0o755); err != nil {
				return partResult{}, fmt.Errorf("create embedding dir %s: %w", embedDir, err)
			}
			p := filepath.Join(embedDir, "embeddings.parquet")
			if err := output.WriteEmbeddings(p, embeddings); err != nil {
				return partResult{}, fmt.Errorf("write embeddings: %w", err)
			}
			emptyMarker := p + ".empty"
			if len(embeddings) == 0 {
				if err := os.WriteFile(emptyMarker, nil, 0o644); err != nil {
					return partResult{}, fmt.Errorf("write empty embedding marker: %w", err)
				}
			} else if err := os.Remove(emptyMarker); err != nil && !os.IsNotExist(err) {
				return partResult{}, fmt.Errorf("remove stale empty embedding marker: %w", err)
			}
			log.Printf("saved %d embeddings -> %s", len(embeddings), p)
		}
		finalDomains = len(domains)
		finalEmbeds = len(embeddings)
	}

	if err := cleanupInput(); err != nil {
		return partResult{}, fmt.Errorf("clean up WARC input: %w", err)
	}

	dur := time.Since(start).Seconds()
	if mode == "embed" {
		erate := 0.0
		if dur > 0 {
			erate = float64(finalEmbeds) / dur
		}
		log.Printf("done: %d embeddings in %.1fs (%.1f embeddings/s)", finalEmbeds, dur, erate)
	} else {
		rate := 0.0
		if dur > 0 {
			rate = float64(finalDomains) / dur
		}
		log.Printf("done: %d domains in %.1fs (%.1f domains/s) -> %s", finalDomains, dur, rate, outDir)
	}

	return partResult{Rows: rows, Domains: finalDomains, Embeds: finalEmbeds}, nil
}

func run(mode string, o opts) {
	ctx := context.Background()
	if o.base == "" {
		log.Fatal("no --base / OUT_BASE_DIR — the output root is required")
	}
	base, err := filepath.Abs(o.base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", o.base, err)
	}
	o.base = base
	catalogS3Base := strings.TrimSpace(os.Getenv("COMMONCRAWL_CATALOG_S3_BASE"))
	if catalogS3Base == "" {
		log.Fatal("COMMONCRAWL_CATALOG_S3_BASE is required (s3://bucket/prefix)")
	}

	// Resolve the output DIRECTORY up front (fail fast, before any fetch). FIXED filenames inside so
	// `load --dir` knows which file → which table. cc-crawl always passes --out; for a standalone run the
	// default is derived from OUT_BASE_DIR (REQUIRED — no silent fallback that could scatter data) as
	// <OUT_BASE_DIR>/<crawl>/warc/<selection>/out_<mode>_<part>/, unique per selection and WARC. The
	// embedding tree is a sibling under <OUT_BASE_DIR>/<crawl>/embedding/ (derived from outDir below).
	outDir := o.out
	if outDir == "" {
		outDir = filepath.Join(o.base, o.crawlID, "warc", o.selection, fmt.Sprintf("out_%s_%d", mode, o.part))
	}
	// embed VERIFY-AND-SKIP: a part is DONE if its embed folder already holds a complete vector file under
	// EITHER name — embeddings.parquet (fp32, fresh from this pass) or embeddings_fp16.parquet (converted
	// offline once the fp32 was pruned). For fp32 the write is atomic (one WriteFile at the end), so
	// parquetRows>0 proves completeness; parquetRows reads only the footer row count, so it usually works for
	// fp16 too, and if parquet-go can't read the HALF_FLOAT footer we accept a non-empty file (the converted
	// file was verified before its fp32 was pruned). If done, skip the whole part (no re-fetch/re-embed);
	// else fall through and (over)write embeddings.parquet. embed deliberately reuses the dir (no empty rule).
	if mode == "embed" {
		if embPath, rows, complete := completedEmbedding(outDir); complete {
			log.Printf("skip: embeddings already present (rows=%d) -> %s", rows, embPath)
			return
		}
	} else if entries, _ := os.ReadDir(outDir); len(entries) > 0 {
		log.Fatalf("output dir %s is not empty — point --out at a fresh/empty directory", outDir)
	}

	d, err := buildPartDeps(ctx, mode, o, 1) // single-part run: one part shares the transport
	if err != nil {
		log.Fatalf("%v", err)
	}
	if _, err := producePart(ctx, d, uint32(o.part), "", outDir); err != nil {
		log.Fatalf("%v", err)
	}

	// Loading remains a separate `load --dir` step so cc-crawl preserves its existing
	// produce -> verify -> load -> local .loaded marker state machine.
}

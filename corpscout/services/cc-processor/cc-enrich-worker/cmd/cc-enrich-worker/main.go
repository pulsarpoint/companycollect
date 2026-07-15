package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dustin/go-humanize"
	"github.com/fsnotify/fsnotify"

	"cc-enrich-worker/internal/classify"
	"cc-enrich-worker/internal/embed"
	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/load"
	mdl "cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
	"cc-enrich-worker/internal/warcinput"
	"cc-enrich-worker/internal/work"
	"cc-enrich-worker/internal/worker"
)

const warcPreparationTimeout = 30 * time.Minute

// maxFetchErrorRate is the failure contract for every fetch pass: above it the part is systemic
// failure (throttling, auth, dead source), so refuse to write and let the range runner retry the part
// on its next pass. Normal WARC decay is a few %.
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
  load       load already-produced Parquet into ClickHouse: --scan a producer root for .produced markers (--watch to keep sweeping)
  sync-db    pull the WARC catalog from S3/RustFS and cache it locally (idempotent; run before first range run on a new host)
  status     read-only report: per-command produced/loaded/pending marker counts + oldest pending age

Run "cc-enrich-worker <command> -h" to see that command's flags.

Config is via environment (see ../.env.example): COMMONCRAWL_CATALOG_S3_BASE and CORPSCOUT_S3_*
for the RustFS catalog (read only by sync-db — produce runs use the local cache it writes),
COMMONCRAWL_EMBED_*, CLICKHOUSE_*, and AWS_* for signed Common Crawl S3 access.
`)
}

// opts holds the parsed flags for a run. Mode-specific fields are only registered (and meaningful)
// for the relevant command.
type opts struct {
	crawlID, out, base, selection string
	part                          int
	concurrency, chunk            int
	techEngine                    string // tech / both
	techMax                       int    // tech / both
	batch, embedConc              int    // industry / both
}

// parse builds a per-command flag set (so tech flags don't show under industry and vice-versa). It
// serves both the single-part run (--part) and the range runner (--parts). When --parts is set it
// returns isRange=true and a populated runnerOpts; the two selectors are mutually exclusive.
func parse(mode string, args []string) (opts, runnerOpts, bool) {
	fs := flag.NewFlagSet("cc-enrich-worker "+mode, flag.ExitOnError)
	var o opts
	// common to every command
	fs.StringVar(&o.crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 — stamped on every row (required)")
	fs.StringVar(&o.selection, "selection", "pages25", "catalog selection identity")
	fs.IntVar(&o.part, "part", -1, "zero-based WARC index (single-part run; mutually exclusive with --parts)")
	fs.StringVar(&o.out, "out", "", "output DIRECTORY for the Parquet files (default <base>/<crawl-id>/warc/<selection>/out_<mode>_<part>; an explicit dir must be empty)")
	fs.StringVar(&o.base, "base", "", "output ROOT (required, explicit — no environment fallback)")
	fs.IntVar(&o.concurrency, "concurrency", 32, "industry/embed: pages in flight; tech/both: DOMAINS in flight, each fetching up to 8 pages in parallel (total fetches = concurrency x 8)")
	fs.IntVar(&o.chunk, "chunk", 1024, "catalog pages per process chunk (tech/both)")
	if mode == "tech" || mode == "both" {
		fs.StringVar(&o.techEngine, "tech-engine", "fast", "tech matcher: fast (Aho-Corasick gated) | wappalyzer (upstream full scan)")
		fs.IntVar(&o.techMax, "tech-max-bytes", 0, "maximum page bytes scanned for technologies (0 = full page)")
	}
	if mode == "industry" || mode == "both" || mode == "embed" {
		fs.IntVar(&o.batch, "embed-batch", embed.DefaultBatch, "texts per embed request — keep small (big batches overflow the engine's token budget)")
		fs.IntVar(&o.embedConc, "embed-concurrency", 96, "concurrent embed requests in flight (saturate the GPU)")
	}
	// Range runner: process a whole WARC part range through the bounded pool of range readers.
	var partsFlag string
	var ro runnerOpts
	fs.StringVar(&partsFlag, "parts", "", `WARC part range "A-B" or "N" (range runner; mutually exclusive with --part)`)
	fs.IntVar(&ro.warcParallel, "warc-parallel", 4, "range runner: parts produced concurrently (>=1)")
	_ = fs.Parse(args)
	if o.crawlID == "" {
		fmt.Fprintln(os.Stderr, "error: --crawl-id is required")
		fs.Usage()
		os.Exit(2)
	}

	if partsFlag != "" {
		if o.part >= 0 {
			fmt.Fprintln(os.Stderr, "error: --part and --parts are mutually exclusive")
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
		if err := validateRunnerOpts(ro); err != nil {
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
	case "sync-db":
		runSyncDBCmd(os.Args[2:])
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

// runLoad implements the `load` command: sweep a producer output ROOT for `.produced` dirs lacking
// `.loaded` and push each into ClickHouse over the native driver (no clickhouse-client needed). This
// is the marker-driven counterpart to the range runner — the runner produces + writes `.produced`;
// `load --scan` loads + writes `.loaded`. `--watch` keeps sweeping. The kind→table mapping comes from
// each output's FIXED parquet filenames.
func runLoad(args []string) {
	fs := flag.NewFlagSet("cc-enrich-worker load", flag.ExitOnError)
	scan := fs.String("scan", "", "a producer output root; sweep it for .produced dirs lacking .loaded and load each (required)")
	watch := fs.Bool("watch", false, "keep sweeping: after each pass wait for a filesystem event under root or a 5-minute tick")
	parallel := fs.Int("parallel", 4, "how many output dirs to load concurrently")
	deleteLoaded := fs.Bool("delete-loaded", false, "after a dir loads+verifies (and .loaded is written), remove its output DIRECTORY to reclaim disk; also prunes pre-existing .produced+.loaded leftovers. Never deletes markers or embed output. Default off")
	_ = fs.Parse(args)

	if *scan == "" {
		fmt.Fprintln(os.Stderr, "error: --scan <root> is required")
		fs.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	conn, err := chConnect(ctx)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	defer conn.Close()

	if *watch {
		watchLoad(ctx, conn, *scan, *parallel, *deleteLoaded)
		return
	}
	res, err := load.Sweep(ctx, conn, *scan, *parallel, *deleteLoaded)
	if err != nil {
		log.Fatalf("scan %s: %v", *scan, err)
	}
	log.Printf("sweep %s: loaded=%d failed=%d pending=%d skipped=%d pruned=%d", *scan, res.Loaded, res.Failed, res.Pending, res.Skipped, res.Pruned)
	if res.Failed > 0 {
		os.Exit(1)
	}
}

// watchLoad runs Sweep in a loop. The 5-minute ticker is the correctness backstop (a sweep always
// happens); fsnotify Create/Rename events under root only cut latency. If the watcher can't be set
// up it logs ONE warning and degrades to pure ticker polling.
func watchLoad(ctx context.Context, conn driver.Conn, root string, parallel int, deleteLoaded bool) {
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
		res, err := load.Sweep(ctx, conn, root, parallel, deleteLoaded)
		if err != nil {
			log.Printf("sweep %s: %v", root, err)
		} else {
			log.Printf("sweep %s: loaded=%d failed=%d pending=%d skipped=%d pruned=%d", root, res.Loaded, res.Failed, res.Pending, res.Skipped, res.Pruned)
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

	// tech is the page fingerprint engine (--tech-engine). Set only for modes that fingerprint
	// (tech/both) — the industry/embed paths never touch it. Immutable after construction.
	tech tech.Detector

	objects fetch.ObjectGetter

	// work answers "what remains for this run": catalog plans, part status, output layout.
	// Immutable after Open, safe for the range lanes' concurrent producePart calls.
	work *work.Run

	// runStats is the range runner's process-wide sink. Non-nil ONLY on the --parts path: it puts
	// every part's ShardConfig into range mode (aggregate + suppress per-chunk logs) and gates the
	// per-part "progress:" lines off. nil for a single --part run, which keeps all logging unchanged.
	runStats *worker.RunStats
}

// partResult reports what a single part produced. Rows maps parquet kind names (the loader's table
// kinds in load.Kinds) to row counts for the .produced marker + loader verify; Domains/Embeds drive
// the done log and the range summary.
type partResult struct {
	Rows            map[string]int
	Domains, Embeds int
}

// preparedPart is a WARC input opened for one part, carried from openInput to processInput. The
// split keeps plan-loading/HEAD separate from the process+write remainder. processInput owns the
// input's close (its deferred cleanupInput).
type preparedPart struct {
	input  *warcinput.Input
	outDir string
}

// fetchConcurrencyFor sizes the shared S3/HTTP transport connection budget. The per-part base is the
// domain pool (o.concurrency), multiplied by the per-domain page pool (worker.PageConcurrency) for
// tech/both since those fetch PageConcurrency pages per in-flight domain. partsParallel scales that
// by how many parts share the ONE transport concurrently: 1 for a single --part run; warcParallel
// for the range runner so that, e.g., --warc-parallel 4 does not squeeze 4 parts through one part's
// connection budget.
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

	if mode == "tech" || mode == "both" { // tech engine only needed when we fingerprint
		switch o.techEngine {
		case "fast":
			fm, err := tech.NewFastMatcher()
			if err != nil {
				return partDeps{}, fmt.Errorf("build fast tech matcher: %w", err)
			}
			d.tech = fm
			log.Printf("tech engine: fast (Aho-Corasick gated)")
		case "wappalyzer":
			wz, err := tech.NewWappalyzer()
			if err != nil {
				return partDeps{}, fmt.Errorf("build wappalyzer tech engine: %w", err)
			}
			d.tech = wz
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

	// Transport sizing = true in-flight range reads across the parts sharing this transport.
	fetchConcurrency := fetchConcurrencyFor(mode, o.concurrency, partsParallel)
	s3Getter, err := fetch.NewS3Getter(ctx, envOr("AWS_REGION", "us-east-1"), fetchConcurrency)
	if err != nil {
		return partDeps{}, fmt.Errorf("Common Crawl S3 init: %w", err)
	}
	d.objects = s3Getter
	return d, nil
}

// openInput loads one part's plan, prepares the WARC input as network range reads, and logs "WARC
// input ready".
func openInput(ctx context.Context, d partDeps, part uint32, outDir string) (preparedPart, error) {
	o := d.o
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return preparedPart{}, fmt.Errorf("create output dir %s: %w", outDir, err)
	}
	catalogStarted := time.Now()
	log.Printf(
		"catalog: opening local catalog crawl=%s selection=%s path=%s",
		o.crawlID,
		o.selection,
		d.work.CatalogPath(),
	)
	// The synced LOCAL catalog is the only source — produce runs never sync from RustFS. `sync-db`
	// is the explicit step that downloads and validates it; work.Open preflighted its presence
	// so a missing catalog fails before any part starts.
	plan, err := d.work.Plan(part)
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
	prepareContext, cancelPrepare := context.WithTimeout(ctx, warcPreparationTimeout)
	var input *warcinput.Input
	input, err = plan.Open(prepareContext, d.objects, "commoncrawl")
	cancelPrepare()
	if err != nil {
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
		"selected_pages", len(input.Items),
		"selected_bytes", input.SelectedBytes,
		"selected_size", humanize.Bytes(uint64(input.SelectedBytes)),
		"object_bytes", input.ObjectBytes,
		"object_size", humanize.Bytes(uint64(input.ObjectBytes)),
		"utilization_percent", coveragePercent,
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
	slog.InfoContext(ctx, "WARC input ready", inputLog...)

	return preparedPart{input: input, outDir: outDir}, nil
}

// producePart processes one WARC part end to end: open the input (range reads), process + write its
// outputs, clean up, and return the row counts. Every failure returns an error (the deferred cleanup
// still runs); the caller decides whether to fatal (single --part) or record and continue (range runner).
func producePart(ctx context.Context, d partDeps, part uint32, outDir string) (partResult, error) {
	prepared, err := openInput(ctx, d, part, outDir)
	if err != nil {
		return partResult{}, err
	}
	return processInput(ctx, d, prepared)
}

// processInput runs the post-open remainder of producePart: process the opened WARC, write outputs,
// clean up, log "done", and report row counts. Split from openInput so plan-loading/HEAD stays
// separate from the process+write remainder.
func processInput(ctx context.Context, d partDeps, prepared preparedPart) (partResult, error) {
	o := d.o
	mode := d.mode
	input := prepared.input
	outDir := prepared.outDir

	cleanupInput := func() error {
		return input.Close()
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

	// rangeMode: a --parts run wires deps.runStats, which both aggregates counters and (via
	// ShardConfig.RunStats) silences the per-chunk logs. It also gates the per-part "progress:" lines
	// off so the runner's cumulative line is the sole progress output. A single --part run leaves
	// runStats nil, so everything below prints exactly as before.
	rangeMode := d.runStats != nil
	cfg := worker.ShardConfig{
		CrawlID: o.crawlID, SourceRunID: fmt.Sprintf("go-%d", time.Now().Unix()),
		ResolvedAt: time.Now().UTC(), Concurrency: o.concurrency, TechMaxBytes: o.techMax,
		EmbedConcurrency: o.embedConc, EmbedBatch: o.batch, Mode: mode,
		EmbedOnly: mode == "embed",
		Tech:      d.tech,
		RunStats:  d.runStats,
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
		if !rangeMode {
			unit, n := "domains", len(domains)
			if mode == "embed" {
				unit, n = "embeddings", len(embeddings)
			}
			log.Printf("progress: %d/%d pages, %d %s (%.1f pages/s)",
				len(items), len(items), n, unit, float64(len(items))/time.Since(start).Seconds())
		}
	} else {
		// Streaming write: the chunked tech/both path writes each chunk's rows as ONE parquet row group
		// immediately (worker.ShardStreamer) instead of buffering the whole shard, so peak memory is
		// O(chunk) not O(part). A 250k-domain (~2.75M-page) shard would otherwise buffer ~80GB+ of rows.
		runEmbedChunk := mode == "both" // "both" also emits vectors -> the separate data/embedding tree
		embedDir := ""
		if runEmbedChunk {
			embedDir = d.work.EmbedDirFor(outDir)
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
			if !rangeMode {
				el := time.Since(start).Seconds()
				log.Printf("progress: %d/%d pages, %d domains written (%.1f pages/s)",
					curHi, len(items), streamer.DomainCount(), float64(curHi)/el)
			}
			if nextCh != nil {
				fetched = <-nextCh
			}
			curLo, curHi = nextLo, nextHi
		}
		// Failure contract: abort (delete partials) rather than commit a systemically-failed part. The
		// range runner gates on the error (no .produced marker), so a non-nil error here retries the part
		// on its next pass.
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
				embedDir = d.work.EmbedDirFor(outDir)
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
		log.Fatal("--base is required (output root)")
	}
	base, err := filepath.Abs(o.base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", o.base, err)
	}
	o.base = base
	w, err := work.Open(o.base, o.crawlID, o.selection, mode)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// Resolve the output DIRECTORY up front (fail fast, before any fetch). FIXED filenames inside so the
	// loader knows which file → which table. A single --part run is for ad-hoc/debug produce; it does NOT
	// write a .produced marker, so its output is for inspection, not the load pipeline. The default is
	// derived from --base (REQUIRED, explicit — no silent fallback that could scatter data) as
	// <base>/<crawl>/warc/<selection>/out_<mode>_<part>/, unique per selection and WARC. The
	// embedding tree is a sibling under <base>/<crawl>/embedding/ (derived from outDir below).
	outDir := o.out
	if outDir == "" {
		outDir = w.OutDir(uint32(o.part))
	}
	// embed VERIFY-AND-SKIP: a part is DONE if its embed folder already holds a complete vector file under
	// EITHER name — embeddings.parquet (fp32, fresh from this pass) or embeddings_fp16.parquet (converted
	// offline once the fp32 was pruned). For fp32 the write is atomic (one WriteFile at the end), so
	// a positive footer row count proves completeness; the probe reads only the footer, so it usually works for
	// fp16 too, and if parquet-go can't read the HALF_FLOAT footer we accept a non-empty file (the converted
	// file was verified before its fp32 was pruned). If done, skip the whole part (no re-fetch/re-embed);
	// else fall through and (over)write embeddings.parquet. embed deliberately reuses the dir (no empty rule).
	if mode == "embed" {
		if embPath, rows, complete := work.CompletedEmbedding(outDir); complete {
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
	d.work = w
	if _, err := producePart(ctx, d, uint32(o.part), outDir); err != nil {
		log.Fatalf("%v", err)
	}

	// A single --part run writes no .produced marker, so `load --scan` will not pick it up: this path is
	// for ad-hoc/debug produce + inspection. Use `--parts A-B` + `load --scan` for the load pipeline.
}

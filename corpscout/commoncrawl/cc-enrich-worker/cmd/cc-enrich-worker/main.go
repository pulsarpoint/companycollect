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
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dustin/go-humanize"
	"github.com/parquet-go/parquet-go"

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
  load       load an already-produced Parquet output into ClickHouse (native driver — no clickhouse-client needed)

Run "cc-enrich-worker <command> -h" to see that command's flags.

Config is via environment (see ../.env.example): COMMONCRAWL_EMBED_*, CLICKHOUSE_*,
AWS_* for signed Common Crawl S3 access, and CC_BASE_URL for anonymous HTTPS access.
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

// parse builds a per-command flag set (so tech flags don't show under industry and vice-versa).
func parse(mode string, args []string) opts {
	fs := flag.NewFlagSet("cc-enrich-worker "+mode, flag.ExitOnError)
	var o opts
	// common to every command
	fs.StringVar(&o.crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 — stamped on every row (required)")
	fs.StringVar(&o.selection, "selection", "pages25", "catalog selection identity")
	fs.IntVar(&o.part, "part", -1, "zero-based WARC index (required)")
	fs.StringVar(&o.out, "out", "", "output DIRECTORY for the Parquet files (default <base>/<crawl-id>/warc/<selection>/out_<mode>_<part>; an explicit dir must be empty)")
	fs.StringVar(&o.base, "base", os.Getenv("OUT_BASE_DIR"), "catalog and output ROOT (or env OUT_BASE_DIR); required")
	fs.IntVar(&o.concurrency, "concurrency", 32, "industry/embed: pages in flight; tech/both: DOMAINS in flight, each fetching up to 8 pages in parallel (total fetches = concurrency x 8)")
	fs.IntVar(&o.chunk, "chunk", 1024, "catalog pages per process chunk (tech/both)")
	fs.Float64Var(&o.wholeWARCThreshold, "whole-warc-threshold", 50, "selected compressed-byte percentage that triggers one whole-WARC download (0..100)")
	fs.BoolVar(&o.anonymous, "s3-anonymous", false, "fetch through anonymous HTTPS instead of signed Common Crawl S3")
	if mode == "tech" || mode == "both" {
		fs.StringVar(&o.techEngine, "tech-engine", "fast", "tech matcher: fast (Aho-Corasick gated) | wappalyzer (upstream full scan)")
		fs.IntVar(&o.techMax, "tech-max-bytes", 131072, "cap body bytes fed to Wappalyzer (0 = full body; full-body regex ~1.2s/page)")
	}
	if mode == "industry" || mode == "both" || mode == "embed" {
		fs.IntVar(&o.batch, "embed-batch", embed.DefaultBatch, "texts per embed request — keep small (big batches overflow the engine's token budget)")
		fs.IntVar(&o.embedConc, "embed-concurrency", 96, "concurrent embed requests in flight (saturate the GPU)")
	}
	_ = fs.Parse(args)
	if o.crawlID == "" {
		fmt.Fprintln(os.Stderr, "error: --crawl-id is required")
		fs.Usage()
		os.Exit(2)
	}
	if o.part < 0 || uint64(o.part) > uint64(^uint32(0)) {
		fmt.Fprintln(os.Stderr, "error: --part must be a WARC index from 0 to 4294967295")
		fs.Usage()
		os.Exit(2)
	}
	if math.IsNaN(o.wholeWARCThreshold) || math.IsInf(o.wholeWARCThreshold, 0) ||
		o.wholeWARCThreshold < 0 || o.wholeWARCThreshold > 100 {
		fmt.Fprintln(os.Stderr, "error: --whole-warc-threshold must be between 0 and 100")
		fs.Usage()
		os.Exit(2)
	}
	return o
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch cmd := os.Args[1]; cmd {
	case "industry", "tech", "both", "embed":
		run(cmd, parse(cmd, os.Args[2:]))
	case "load":
		runLoad(os.Args[2:])
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
	dir := fs.String("dir", "", "a shard output directory; loads every {domains,industries,page_signals,metadata,contacts,tech,identifiers}.parquet in it")
	file := fs.String("file", "", "a single Parquet output file to load")
	kind := fs.String("kind", "", "override the kind for --file: domains|industries|page_signals|metadata|contacts|tech|identifiers")
	_ = fs.Parse(args)
	if (*dir == "") == (*file == "") {
		fmt.Fprintln(os.Stderr, "error: pass exactly one of --dir or --file")
		fs.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	conn, err := chConnect(ctx)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	defer conn.Close()

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

func run(mode string, o opts) {
	ctx := context.Background()
	if o.base == "" {
		log.Fatal("no --base / OUT_BASE_DIR — the WARC catalog root is required")
	}
	base, err := filepath.Abs(o.base)
	if err != nil {
		log.Fatalf("resolve base directory %s: %v", o.base, err)
	}
	o.base = base

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
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("create output dir %s: %v", outDir, err)
	}

	primaryPagesOnly := mode == "industry" || mode == "embed"
	plan, err := warcinput.LoadPlan(o.base, o.crawlID, o.selection, uint32(o.part), primaryPagesOnly)
	if err != nil {
		log.Fatalf("load WARC catalog: %v", err)
	}

	if len(plan.Items) > 0 && (mode == "tech" || mode == "both") { // tech matcher only needed when we fingerprint
		switch o.techEngine {
		case "fast":
			fm, err := tech.NewFastMatcher()
			if err != nil {
				log.Fatalf("build fast tech matcher: %v", err)
			}
			tech.SetFastMatcher(fm)
			log.Printf("tech engine: fast (Aho-Corasick gated)")
		case "wappalyzer":
			log.Printf("tech engine: wappalyzer (upstream full scan)")
		default:
			log.Fatalf("--tech-engine must be fast|wappalyzer, got %q", o.techEngine)
		}
	}

	// The embedder is needed whenever we embed pages (industry/both/embed). The NACE reference +
	// ClickHouse are needed only when we ALSO classify (industry/both) — embed-only skips them.
	var ref *mdl.Reference
	var protos *mdl.Prototypes
	var emb classify.Embedder
	if len(plan.Items) > 0 && mode != "tech" {
		baseURL := envOr("COMMONCRAWL_EMBED_BASE_URL", "http://localhost:8000/v1")
		model := envOr("COMMONCRAWL_EMBED_MODEL", "")
		if model == "" {
			var derr error
			if model, derr = detectModel(baseURL); derr != nil {
				log.Fatalf("detect embed model: %v", derr)
			}
		}
		log.Printf("embed endpoint=%s model=%s", baseURL, model)
		emb = embed.NewEmbedClient(baseURL, model, o.batch, o.embedConc, envInt("COMMONCRAWL_EMBED_MAX_CHARS", 0))

		if mode == "embed" {
			log.Printf("embed-only mode: no NACE reference, no ClickHouse, no load")
		} else {
			conn, err := chConnect(ctx)
			if err != nil {
				log.Fatalf("clickhouse connect: %v", err)
			}
			ref, protos, err = classify.LoadReference(ctx, conn)
			if err != nil {
				conn.Close()
				log.Fatalf("load reference: %v", err)
			}
			meta, metaErr := classify.LoadReferenceMeta(ctx, conn)
			conn.Close()
			if metaErr != nil {
				log.Fatalf("load reference meta: %v", metaErr)
			}
			if len(ref.M) == 0 {
				log.Fatal("empty NACE reference — rebuild corpscout.nace_category_embeddings")
			}
			log.Printf("reference: nace=%d page_types=%d dim=%d model=%s", len(ref.M), len(protos.P), len(ref.M[0]), meta.Model)

			// Fail fast unless the reference covers every NACE category and was built with the same
			// model + dim this worker will embed pages with (the matching-vector-space invariant).
			if err := classify.VerifyReference(meta, model, emb); err != nil {
				log.Fatalf("reference check failed: %v", err)
			}
			log.Printf("reference check OK: %d/%d categories, model=%s dim=%d", meta.Count, meta.Expected, meta.Model, meta.Dim)
		}
	}

	// Transport sizing = true in-flight range reads. Whole-WARC mode makes one streaming GET and
	// then uses concurrent local ReadAt calls through the same RangeGetter boundary.
	var objects fetch.ObjectGetter
	source := "none"
	if !plan.Empty() {
		fetchConcurrency := o.concurrency
		if mode == "tech" || mode == "both" {
			fetchConcurrency *= worker.PageConcurrency
		}
		if o.anonymous {
			objects = fetch.NewHTTPGetter(envOr("CC_BASE_URL", ""), fetchConcurrency)
			source = "anonymous_https"
		} else {
			objects, err = fetch.NewS3Getter(ctx, envOr("AWS_REGION", "us-east-1"), fetchConcurrency)
			if err != nil {
				log.Fatalf("Common Crawl S3 init: %v", err)
			}
			source = "signed_s3"
		}
	}

	var s3Before fetch.S3Stats
	if s3Getter, ok := objects.(*fetch.S3Getter); ok {
		s3Before = s3Getter.Stats()
	}
	prepareStarted := time.Now()
	warcTempDirectory := filepath.Join(outDir, ".warc-input")
	if err := os.RemoveAll(warcTempDirectory); err != nil {
		log.Fatalf("remove stale WARC input directory: %v", err)
	}
	prepareContext, cancelPrepare := context.WithTimeout(ctx, warcPreparationTimeout)
	input, err := plan.Open(
		prepareContext,
		objects,
		"commoncrawl",
		o.wholeWARCThreshold,
		warcTempDirectory,
	)
	cancelPrepare()
	if err != nil {
		if cleanupErr := os.RemoveAll(warcTempDirectory); cleanupErr != nil {
			log.Printf("remove failed WARC input directory: %v", cleanupErr)
		}
		log.Fatalf("prepare WARC input: %v", err)
	}
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
	fatal := func(format string, arguments ...any) {
		if err := cleanupInput(); err != nil {
			log.Printf("clean up WARC input after failure: %v", err)
		}
		log.Fatalf(format, arguments...)
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
		"source", source,
		"selected_pages", len(input.Items),
		"selected_bytes", input.SelectedBytes,
		"selected_size", humanize.Bytes(uint64(input.SelectedBytes)),
		"object_bytes", input.ObjectBytes,
		"object_size", humanize.Bytes(uint64(input.ObjectBytes)),
		"utilization_percent", coveragePercent,
		"whole_warc_threshold_percent", o.wholeWARCThreshold,
		"prepare_seconds", prepareElapsed.Seconds(),
	}
	if s3Getter, ok := objects.(*fetch.S3Getter); ok {
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

	streamed := false                 // tech/both writes its own files (streaming) and skips the shared block
	finalDomains, finalEmbeds := 0, 0 // for the done log (the accumulator slices are empty on the streamed path)

	// Industry: one continuous fetch→embed stream over the whole shard, so the GPU is fed
	// non-stop (no per-chunk "embed all then wait" barrier). tech/both keep the chunk pipeline.
	if mode == "industry" || mode == "embed" {
		res, err := worker.ProcessIndustryStream(ctx, items, getter, emb, ref, protos, cfg)
		if err != nil {
			fatal("industry stream: %v", err)
		}
		if res.Pages > 0 && float64(res.Errs)/float64(res.Pages) > maxFetchErrorRate {
			fatal("refusing to write: fetch error rate %.0f%% (%d/%d pages) exceeds %.0f%% — WARC index will be retried",
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
			fatal("open shard writers: %v", serr)
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
			res, ferr := worker.Finalize(ctx, cur, emb, ref, protos, cfg)
			if ferr != nil {
				streamer.Abort()
				fatal("process chunk [%d:%d]: %v", curLo, curHi, ferr)
			}
			if werr := streamer.Write(res); werr != nil {
				streamer.Abort()
				fatal("write chunk [%d:%d]: %v", curLo, curHi, werr)
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
		// gates on exit code, so a Fatalf here retries the part next run.
		if totalPages > 0 && float64(totalErrs)/float64(totalPages) > maxFetchErrorRate {
			streamer.Abort()
			fatal("refusing to write: fetch error rate %.0f%% (%d/%d pages) exceeds %.0f%% — WARC index will be retried",
				100*float64(totalErrs)/float64(totalPages), totalErrs, totalPages, 100*maxFetchErrorRate)
		}
		if len(items) > 0 && streamer.DomainCount() == 0 {
			streamer.Abort()
			fatal("refusing to write: 0 domains from %d catalog pages (systemic parse failure?) — WARC index will be retried", len(items))
		}
		_, cerr := streamer.Commit()
		if cerr != nil {
			fatal("commit shard writers: %v", cerr)
		}
		finalDomains = streamer.DomainCount()
		finalEmbeds = streamer.EmbeddingCount()
		streamed = true
	}

	if !streamed {
		// Failure contract (industry/embed): refuse to write when non-empty catalog input produced nothing.
		produced := len(domains)
		if mode == "embed" {
			produced = len(embeddings)
		}
		if len(items) > 0 && produced == 0 {
			fatal("refusing to write: 0 outputs from %d catalog pages (systemic fetch failure?) — WARC index will be retried", len(items))
		}
		write := func(name string, fn func(string) error) {
			p := filepath.Join(outDir, name)
			if err := fn(p); err != nil {
				fatal("write %s: %v", name, err)
			}
		}
		if mode == "industry" { // domains master + classification; embed writes only the vectors below
			write("domains.parquet", func(p string) error { return output.WriteDomains(p, domains) })
			write("industries.parquet", func(p string) error { return output.WriteIndustries(p, industries) })
			write("page_signals.parquet", func(p string) error { return output.WritePageSignals(p, pageSignals) })
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
				fatal("create embedding dir %s: %v", embedDir, err)
			}
			p := filepath.Join(embedDir, "embeddings.parquet")
			if err := output.WriteEmbeddings(p, embeddings); err != nil {
				fatal("write embeddings: %v", err)
			}
			emptyMarker := p + ".empty"
			if len(embeddings) == 0 {
				if err := os.WriteFile(emptyMarker, nil, 0o644); err != nil {
					fatal("write empty embedding marker: %v", err)
				}
			} else if err := os.Remove(emptyMarker); err != nil && !os.IsNotExist(err) {
				fatal("remove stale empty embedding marker: %v", err)
			}
			log.Printf("saved %d embeddings -> %s", len(embeddings), p)
		}
		finalDomains = len(domains)
		finalEmbeds = len(embeddings)
	}

	if err := cleanupInput(); err != nil {
		fatal("clean up WARC input: %v", err)
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

	// Loading remains a separate `load --dir` step so cc-crawl preserves its existing
	// produce -> verify -> load -> local .loaded marker state machine.
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/classify"
	"cc-enrich-worker/internal/embed"
	"cc-enrich-worker/internal/fetch"
	"cc-enrich-worker/internal/load"
	mdl "cc-enrich-worker/internal/model"
	"cc-enrich-worker/internal/output"
	"cc-enrich-worker/internal/tech"
	"cc-enrich-worker/internal/worker"
)

// WorklistRow is one row of the materialized worklist parquet (index-builder columns).
// content_languages is present in the file but unused by the worker.
type WorklistRow struct {
	RootDomain   string `parquet:"root_domain"`
	URL          string `parquet:"url"`
	WarcFilename string `parquet:"warc_filename"`
	Offset       int64  `parquet:"warc_record_offset"`
	Length       int64  `parquet:"warc_record_length"`
}

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
AWS_* (region defaults to us-east-1 — the CommonCrawl bucket), CC_BASE_URL.
`)
}

// opts holds the parsed flags for a run. Mode-specific fields are only registered (and meaningful)
// for the relevant command.
type opts struct {
	worklist, crawlID, out, base string
	concurrency, chunk     int
	anonymous              bool
	s3Bucket, s3Prefix     string
	techEngine             string // tech / both
	techMax                int    // tech / both
	batch, embedConc       int    // industry / both
}

// parse builds a per-command flag set (so tech flags don't show under industry and vice-versa).
func parse(mode string, args []string) opts {
	fs := flag.NewFlagSet("cc-enrich-worker "+mode, flag.ExitOnError)
	var o opts
	// common to every command
	fs.StringVar(&o.worklist, "worklist", "", "worklist parquet shard (required)")
	fs.StringVar(&o.crawlID, "crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 — stamped on every row (required)")
	fs.StringVar(&o.out, "out", "", "output DIRECTORY for the Parquet files (default <base>/<crawl-id>/crawl/<worklist-name>/, unique per shard; an explicit dir must be empty)")
	fs.StringVar(&o.base, "base", os.Getenv("OUT_BASE_DIR"), "output ROOT for the default --out (or env OUT_BASE_DIR); required when --out is not given")
	fs.IntVar(&o.concurrency, "concurrency", 32, "industry/embed: pages in flight; tech/both: DOMAINS in flight, each fetching up to 8 pages in parallel (total fetches = concurrency x 8)")
	fs.IntVar(&o.chunk, "chunk", 1024, "worklist rows (pages) per fetch+process chunk (tech/both)")
	fs.BoolVar(&o.anonymous, "s3-anonymous", false, "fetch via the HTTPS CDN data.commoncrawl.org (off-AWS; rate-limited — signed S3 preferred)")
	fs.StringVar(&o.s3Bucket, "s3-bucket", "", "if set, upload outputs to this bucket (in-AWS path)")
	fs.StringVar(&o.s3Prefix, "s3-prefix", "", "key prefix for uploaded outputs")
	if mode == "tech" || mode == "both" {
		fs.StringVar(&o.techEngine, "tech-engine", "fast", "tech matcher: fast (Aho-Corasick gated) | wappalyzer (upstream full scan)")
		fs.IntVar(&o.techMax, "tech-max-bytes", 131072, "cap body bytes fed to Wappalyzer (0 = full body; full-body regex ~1.2s/page)")
	}
	if mode == "industry" || mode == "both" || mode == "embed" {
		fs.IntVar(&o.batch, "embed-batch", embed.DefaultBatch, "texts per embed request — keep small (big batches overflow the engine's token budget)")
		fs.IntVar(&o.embedConc, "embed-concurrency", 96, "concurrent embed requests in flight (saturate the GPU)")
	}
	_ = fs.Parse(args)
	if o.worklist == "" || o.crawlID == "" {
		fmt.Fprintln(os.Stderr, "error: --worklist and --crawl-id are required")
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

// readWorklist loads the shard and marks the first row per domain as the primary page (the
// worklist is ordered shallowest-first, so rn=1 leads each domain group).
func readWorklist(path string) ([]mdl.WorklistItem, error) {
	rows, err := parquet.ReadFile[WorklistRow](path)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	items := make([]mdl.WorklistItem, 0, len(rows))
	for _, r := range rows {
		primary := !seen[r.RootDomain]
		seen[r.RootDomain] = true
		items = append(items, mdl.WorklistItem{
			RootDomain: r.RootDomain, URL: r.URL, WarcFilename: r.WarcFilename,
			Offset: r.Offset, Length: r.Length, Primary: primary,
		})
	}
	return items, nil
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

// uniqueDomains counts distinct root domains in a worklist (industry/embed embed one page per domain).
func uniqueDomains(items []mdl.WorklistItem) int {
	seen := make(map[string]struct{}, len(items))
	for _, it := range items {
		seen[it.RootDomain] = struct{}{}
	}
	return len(seen)
}

func run(mode string, o opts) {
	ctx := context.Background()

	// Resolve the output DIRECTORY up front (fail fast, before any fetch). FIXED filenames inside so
	// `load --dir` knows which file → which table. cc-crawl always passes --out; for a standalone run the
	// default is derived from OUT_BASE_DIR (REQUIRED — no silent fallback that could scatter data) as
	// <OUT_BASE_DIR>/<crawl>/crawl/<worklist-stem>/, unique per shard so parallel runs never collide. The
	// embedding tree is a sibling under <OUT_BASE_DIR>/<crawl>/embedding/ (derived from outDir below).
	outDir := o.out
	if outDir == "" {
		if o.base == "" {
			log.Fatal("no --out and no -base / OUT_BASE_DIR — refusing to guess an output directory")
		}
		stem := strings.TrimSuffix(filepath.Base(o.worklist), filepath.Ext(o.worklist))
		outDir = filepath.Join(o.base, o.crawlID, "crawl", stem)
	}
	// embed VERIFY-AND-SKIP: a part is DONE if its embed folder already holds a complete vector file under
	// EITHER name — embeddings.parquet (fp32, fresh from this pass) or embeddings_fp16.parquet (converted
	// offline once the fp32 was pruned). For fp32 the write is atomic (one WriteFile at the end), so
	// parquetRows>0 proves completeness; parquetRows reads only the footer row count, so it usually works for
	// fp16 too, and if parquet-go can't read the HALF_FLOAT footer we accept a non-empty file (the converted
	// file was verified before its fp32 was pruned). If done, skip the whole part (no re-fetch/re-embed);
	// else fall through and (over)write embeddings.parquet. embed deliberately reuses the dir (no empty rule).
	if mode == "embed" {
		for _, name := range []string{"embeddings.parquet", "embeddings_fp16.parquet"} {
			embPath := filepath.Join(outDir, name)
			rows, verr := parquetRows(embPath)
			if verr == nil {
				if rows <= 0 {
					continue // readable but empty -> not done under this name
				}
			} else if st, serr := os.Stat(embPath); serr != nil || st.Size() == 0 {
				continue // missing, or unreadable-and-empty -> not done under this name
			}
			dom := 0
			if items, werr := readWorklist(o.worklist); werr == nil {
				dom = uniqueDomains(items)
			}
			log.Printf("skip: embeddings already present (rows=%d, %d worklist domains) -> %s", rows, dom, embPath)
			return
		}
	} else if entries, _ := os.ReadDir(outDir); len(entries) > 0 {
		log.Fatalf("output dir %s is not empty — point --out at a fresh/empty directory", outDir)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("create output dir %s: %v", outDir, err)
	}

	if mode == "tech" || mode == "both" { // tech matcher only needed when we fingerprint
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
	if mode != "tech" {
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

	// Transport sizing = true in-flight fetches: industry/embed run one page per concurrency slot;
	// tech/both fan each domain slot into worker.PageConcurrency parallel page fetches.
	fetchConc := o.concurrency
	if mode == "tech" || mode == "both" {
		fetchConc *= worker.PageConcurrency
	}
	var getter fetch.RangeGetter
	if o.anonymous {
		getter = fetch.NewHTTPGetter(envOr("CC_BASE_URL", ""), fetchConc)
		log.Printf("fetch: anonymous HTTPS CDN (data.commoncrawl.org)")
	} else {
		g, err := fetch.NewS3Getter(ctx, envOr("AWS_REGION", "us-east-1"), fetchConc)
		if err != nil {
			log.Fatalf("s3 init: %v", err)
		}
		getter = g
	}

	items, err := readWorklist(o.worklist)
	if err != nil {
		log.Fatalf("read worklist: %v", err)
	}
	log.Printf("worklist: %d pages", len(items))

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

	var written []string              // committed output paths (both branches populate this)
	streamed := false                 // tech/both writes its own files (streaming) and skips the shared block
	finalDomains, finalEmbeds := 0, 0 // for the done log (the accumulator slices are empty on the streamed path)

	// Industry: one continuous fetch→embed stream over the whole shard, so the GPU is fed
	// non-stop (no per-chunk "embed all then wait" barrier). tech/both keep the chunk pipeline.
	if mode == "industry" || mode == "embed" {
		res, err := worker.ProcessIndustryStream(ctx, items, getter, emb, ref, protos, cfg)
		if err != nil {
			log.Fatalf("industry stream: %v", err)
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
			embedDir = filepath.Join(filepath.Dir(filepath.Dir(outDir)), "embedding", filepath.Base(outDir))
		}
		streamer, serr := worker.NewShardStreamer(outDir, embedDir, runEmbedChunk, true /*runTech*/)
		if serr != nil {
			log.Fatalf("open shard writers: %v", serr)
		}
		// Domain-aligned chunks: each snapped to a domain boundary so a domain's pages never split across a
		// chunk (the worklist is ORDER BY root_domain, rn) — otherwise a straddling domain aggregates twice
		// into two DomainRows with conflicting primary URLs (review C3). The loop advances by the ACTUAL
		// snapped boundary, prefetching the next chunk over the network while the current one finalizes+writes.
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
				log.Fatalf("process chunk [%d:%d]: %v", curLo, curHi, ferr)
			}
			if werr := streamer.Write(res); werr != nil {
				streamer.Abort()
				log.Fatalf("write chunk [%d:%d]: %v", curLo, curHi, werr)
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
		const maxFetchErrorRate = 0.5 // >50% errors = systemic; normal WARC decay is a few %
		if totalPages > 0 && float64(totalErrs)/float64(totalPages) > maxFetchErrorRate {
			streamer.Abort()
			log.Fatalf("refusing to write: fetch error rate %.0f%% (%d/%d pages) exceeds %.0f%% — part will be retried",
				100*float64(totalErrs)/float64(totalPages), totalErrs, totalPages, 100*maxFetchErrorRate)
		}
		if len(items) > 0 && streamer.DomainCount() == 0 {
			streamer.Abort()
			log.Fatalf("refusing to write: 0 domains from %d worklist pages (systemic fetch failure?) — part will be retried", len(items))
		}
		committed, cerr := streamer.Commit()
		if cerr != nil {
			log.Fatalf("commit shard writers: %v", cerr)
		}
		written = committed
		finalDomains = streamer.DomainCount()
		finalEmbeds = streamer.EmbeddingCount()
		streamed = true
	}

	if !streamed {
		// Failure contract (industry/embed): refuse to write when a non-empty worklist produced nothing.
		produced := len(domains)
		if mode == "embed" {
			produced = len(embeddings)
		}
		if len(items) > 0 && produced == 0 {
			log.Fatalf("refusing to write: 0 outputs from %d worklist pages (systemic fetch failure?) — part will be retried", len(items))
		}
		write := func(name string, fn func(string) error) {
			p := filepath.Join(outDir, name)
			if err := fn(p); err != nil {
				log.Fatalf("write %s: %v", name, err)
			}
			written = append(written, p)
		}
		if mode == "industry" { // domains master + classification; embed writes only the vectors below
			write("domains.parquet", func(p string) error { return output.WriteDomains(p, domains) })
			write("industries.parquet", func(p string) error { return output.WriteIndustries(p, industries) })
			write("page_signals.parquet", func(p string) error { return output.WritePageSignals(p, pageSignals) })
		}
		// Embeddings are the expensive GPU artifact — kept large, never loaded into ClickHouse. For embed
		// mode --out IS the embed dir; for industry, write to the SEPARATE sibling tree
		// data/crawl/<stem> -> data/embedding/<stem>.
		if len(embeddings) > 0 {
			embedDir := outDir
			if mode != "embed" {
				embedDir = filepath.Join(filepath.Dir(filepath.Dir(outDir)), "embedding", filepath.Base(outDir))
			}
			if err := os.MkdirAll(embedDir, 0o755); err != nil {
				log.Fatalf("create embedding dir %s: %v", embedDir, err)
			}
			p := filepath.Join(embedDir, "embeddings.parquet")
			if err := output.WriteEmbeddings(p, embeddings); err != nil {
				log.Fatalf("write embeddings: %v", err)
			}
			written = append(written, p)
			log.Printf("saved %d embeddings -> %s", len(embeddings), p)
		}
		finalDomains = len(domains)
		finalEmbeds = len(embeddings)
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

	// Loading is a SEPARATE step (the `load` subcommand / `load --dir`) so produce and load can be
	// run and checked independently — a bad produce never gets auto-loaded. See docs/cc-crawl-design.md.

	if o.s3Bucket != "" {
		g, ok := getter.(*fetch.S3Getter)
		if !ok {
			log.Printf("skip S3 upload: anonymous HTTP fetch mode has no S3 client")
		} else {
			for _, p := range written {
				key := o.s3Prefix + p
				if err := output.UploadToS3(ctx, g.Client, o.s3Bucket, key, p); err != nil {
					log.Fatalf("upload %s: %v", p, err)
				}
				log.Printf("uploaded s3://%s/%s", o.s3Bucket, key)
			}
		}
	}
}

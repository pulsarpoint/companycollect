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

func usage() {
	fmt.Fprint(os.Stderr, `cc-enrich-worker — enrich CommonCrawl domains into the corpscout database

Usage:
  cc-enrich-worker <command> [flags]

Commands:
  industry   embed each domain's page + classify NACE industry  (GPU-bound; needs ClickHouse + embed endpoint)
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
	worklist, crawlID, out string
	load                   bool
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
	fs.StringVar(&o.out, "out", "", "output DIRECTORY for the Parquet files (default ../data/crawl/<worklist-name>/, unique per shard; an explicit dir must be empty)")
	fs.BoolVar(&o.load, "load", false, "after writing Parquet, INSERT it into ClickHouse over the native driver (no clickhouse-client needed)")
	fs.IntVar(&o.concurrency, "concurrency", 32, "fetch/parse concurrency (push to ~128 to hide off-AWS fetch RTT)")
	fs.IntVar(&o.chunk, "chunk", 1024, "domains per fetch+process chunk")
	fs.BoolVar(&o.anonymous, "s3-anonymous", false, "fetch via the HTTPS CDN data.commoncrawl.org (off-AWS; rate-limited — signed S3 preferred)")
	fs.StringVar(&o.s3Bucket, "s3-bucket", "", "if set, upload outputs to this bucket (in-AWS path)")
	fs.StringVar(&o.s3Prefix, "s3-prefix", "", "key prefix for uploaded outputs")
	if mode == "tech" || mode == "both" {
		fs.StringVar(&o.techEngine, "tech-engine", "fast", "tech matcher: fast (Aho-Corasick gated) | wappalyzer (upstream full scan)")
		fs.IntVar(&o.techMax, "tech-max-bytes", 131072, "cap body bytes fed to Wappalyzer (0 = full body; full-body regex ~1.2s/page)")
	}
	if mode == "industry" || mode == "both" {
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
	case "industry", "tech", "both":
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
	dir := fs.String("dir", "", "a shard output directory; loads every {domains,tech,identifiers,profiles}.parquet in it")
	file := fs.String("file", "", "a single Parquet output file to load")
	kind := fs.String("kind", "", "override the kind for --file: domains|tech|identifiers|profiles")
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

func run(mode string, o opts) {
	ctx := context.Background()

	// Resolve the output DIRECTORY up front (fail fast, before any fetch). FIXED filenames inside so
	// `load --dir` knows which file → which table. The default is unique per shard
	// (../data/crawl/<worklist-stem>/) so parallel shard runs never collide; an explicit --out must
	// be empty so a manual run can't clobber another's output.
	outDir := o.out
	if outDir == "" {
		stem := strings.TrimSuffix(filepath.Base(o.worklist), filepath.Ext(o.worklist))
		outDir = filepath.Join("../data/crawl", stem)
	} else if entries, _ := os.ReadDir(outDir); len(entries) > 0 {
		log.Fatalf("output dir %s is not empty — point --out at a fresh/empty directory", outDir)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		log.Fatalf("create output dir %s: %v", outDir, err)
	}

	if mode != "industry" { // tech matcher only needed when we fingerprint
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

	// ClickHouse reference + embedder are only needed when we classify (industry/both).
	var ref *mdl.Reference
	var protos *mdl.Prototypes
	var emb classify.Embedder
	if mode != "tech" {
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

		baseURL := envOr("COMMONCRAWL_EMBED_BASE_URL", "http://localhost:8000/v1")
		model := envOr("COMMONCRAWL_EMBED_MODEL", "")
		if model == "" {
			if model, err = detectModel(baseURL); err != nil {
				log.Fatalf("detect embed model: %v", err)
			}
		}
		log.Printf("embed endpoint=%s model=%s", baseURL, model)
		emb = embed.NewEmbedClient(baseURL, model, o.batch, o.embedConc)

		// Fail fast unless the reference covers every NACE category and was built with the same
		// model + dim this worker will embed pages with (the matching-vector-space invariant).
		if err := classify.VerifyReference(meta, model, emb); err != nil {
			log.Fatalf("reference check failed: %v", err)
		}
		log.Printf("reference check OK: %d/%d categories, model=%s dim=%d", meta.Count, meta.Expected, meta.Model, meta.Dim)
	}

	var getter fetch.RangeGetter
	if o.anonymous {
		getter = fetch.NewHTTPGetter(envOr("CC_BASE_URL", ""), o.concurrency)
		log.Printf("fetch: anonymous HTTPS CDN (data.commoncrawl.org)")
	} else {
		g, err := fetch.NewS3Getter(ctx, envOr("AWS_REGION", "us-east-1"))
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
	}
	start := time.Now()
	var domains []output.DomainRow
	var techRows []output.TechRow
	var idents []output.IdentifierRow
	var profiles []output.ProfileRow

	// Industry: one continuous fetch→embed stream over the whole shard, so the GPU is fed
	// non-stop (no per-chunk "embed all then wait" barrier). tech/both keep the chunk pipeline.
	if mode == "industry" {
		res, err := worker.ProcessIndustryStream(ctx, items, getter, emb, ref, protos, cfg)
		if err != nil {
			log.Fatalf("industry stream: %v", err)
		}
		domains = res.Domains
		log.Printf("progress: %d/%d pages, %d domains (%.1f pages/s)",
			len(items), len(items), len(domains), float64(len(items))/time.Since(start).Seconds())
	} else {
		// Pipeline fetch and finalize: while chunk N embeds on the GPU (Finalize), chunk N+1 downloads
		// on the network (FetchChunk) — so the GPU isn't idle during the fetch phase.
		chunkEnd := func(i int) int {
			if e := i + o.chunk; e < len(items) {
				return e
			}
			return len(items)
		}
		var fetched worker.FetchedChunk
		if len(items) > 0 {
			fetched = worker.FetchChunk(ctx, items[0:chunkEnd(0)], getter, cfg)
		}
		for i := 0; i < len(items); i += o.chunk {
			end := chunkEnd(i)
			cur := fetched
			var nextCh chan worker.FetchedChunk
			if end < len(items) { // prefetch the next chunk while this one finalizes
				nextCh = make(chan worker.FetchedChunk, 1)
				nlo, nhi := end, chunkEnd(end)
				go func() { nextCh <- worker.FetchChunk(ctx, items[nlo:nhi], getter, cfg) }()
			}
			res, err := worker.Finalize(ctx, cur, emb, ref, protos, cfg)
			if err != nil {
				log.Fatalf("process chunk at %d: %v", i, err)
			}
			domains = append(domains, res.Domains...)
			techRows = append(techRows, res.Tech...)
			idents = append(idents, res.Identifiers...)
			profiles = append(profiles, res.Profiles...)
			el := time.Since(start).Seconds()
			log.Printf("progress: %d/%d pages, %d domains, %d tech, %d ids, %d profiles (%.1f pages/s)",
				end, len(items), len(domains), len(techRows), len(idents), len(profiles), float64(end)/el)
			if nextCh != nil {
				fetched = <-nextCh
			}
		}
	}

	var written []string
	write := func(name string, fn func(string) error) {
		p := filepath.Join(outDir, name)
		if err := fn(p); err != nil {
			log.Fatalf("write %s: %v", name, err)
		}
		written = append(written, p)
	}
	if mode != "tech" {
		write("domains.parquet", func(p string) error { return output.WriteDomains(p, domains) })
	}
	if mode != "industry" {
		write("tech.parquet", func(p string) error { return output.WriteTech(p, techRows) })
		write("identifiers.parquet", func(p string) error { return output.WriteIdentifiers(p, idents) })
		write("profiles.parquet", func(p string) error { return output.WriteProfiles(p, profiles) })
	}
	dur := time.Since(start).Seconds()
	rate := 0.0
	if dur > 0 {
		rate = float64(len(domains)) / dur
	}
	log.Printf("done: %d domains, %d tech rows in %.1fs (%.1f domains/s) -> %s", len(domains), len(techRows), dur, rate, outDir)

	if o.load {
		conn, err := chConnect(ctx)
		if err != nil {
			log.Fatalf("clickhouse connect (load): %v", err)
		}
		defer conn.Close()
		loadInto := func(kind string, do func() (int, error)) {
			n, err := do()
			if err != nil {
				log.Fatalf("load %s: %v", kind, err)
			}
			log.Printf("loaded %d rows -> %s", n, load.Tables[kind])
		}
		if mode != "tech" {
			loadInto("domains", func() (int, error) { return load.Insert(ctx, conn, load.Tables["domains"], domains) })
		}
		if mode != "industry" {
			loadInto("tech", func() (int, error) { return load.Insert(ctx, conn, load.Tables["tech"], techRows) })
			loadInto("identifiers", func() (int, error) { return load.Insert(ctx, conn, load.Tables["identifiers"], idents) })
			loadInto("profiles", func() (int, error) { return load.Insert(ctx, conn, load.Tables["profiles"], profiles) })
		}
	}

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

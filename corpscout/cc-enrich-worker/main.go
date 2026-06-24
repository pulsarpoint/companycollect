package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/parquet-go/parquet-go"
)

// WorklistRow is one row of the materialized worklist parquet (index_enrich/worklist.py
// columns). content_languages is present in the file but unused by the worker.
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

// readWorklist loads the shard and marks the first row per domain as the primary
// page (the worklist is ordered shallowest-first, so rn=1 leads each domain group).
func readWorklist(path string) ([]WorklistItem, error) {
	rows, err := parquet.ReadFile[WorklistRow](path)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	items := make([]WorklistItem, 0, len(rows))
	for _, r := range rows {
		primary := !seen[r.RootDomain]
		seen[r.RootDomain] = true
		items = append(items, WorklistItem{
			RootDomain: r.RootDomain, URL: r.URL, WarcFilename: r.WarcFilename,
			Offset: r.Offset, Length: r.Length, Primary: primary,
		})
	}
	return items, nil
}

func main() {
	worklist := flag.String("worklist", "", "worklist parquet shard (required)")
	out := flag.String("out", "out", "output prefix: writes <out>-domains.parquet, <out>-tech.parquet")
	crawlID := flag.String("crawl-id", "", "crawl id, e.g. CC-MAIN-2026-25 (required)")
	concurrency := flag.Int("concurrency", 32, "fetch/parse/tech concurrency")
	techMax := flag.Int("tech-max-bytes", techMaxBytes, "cap body bytes fed to Wappalyzer (0=full body; full-body regex is ~1.2s/page)")
	skipTech := flag.Bool("skip-tech", false, "skip Wappalyzer entirely (light industry-only pass; run tech separately)")
	anonymous := flag.Bool("s3-anonymous", false, "fetch from S3 with unsigned requests (off-AWS; CommonCrawl is open data)")
	batch := flag.Int("embed-batch", embedBatch, "embed batch size")
	embedConc := flag.Int("embed-concurrency", 96, "concurrent embed requests in flight (saturate the GPU)")
	chunkSize := flag.Int("chunk", 1024, "domains per fetch+embed chunk (pipelines fetch/embed; lower = earlier GPU traffic)")
	region := flag.String("region", envOr("AWS_REGION", "us-east-1"), "S3 region")
	s3Bucket := flag.String("s3-bucket", "", "if set, upload outputs to this bucket")
	s3Prefix := flag.String("s3-prefix", "", "key prefix for uploaded outputs")
	flag.Parse()

	if *worklist == "" || *crawlID == "" {
		log.Fatal("--worklist and --crawl-id are required")
	}
	ctx := context.Background()

	conn, err := chConnect(ctx)
	if err != nil {
		log.Fatalf("clickhouse connect: %v", err)
	}
	ref, protos, err := LoadReference(ctx, conn)
	conn.Close()
	if err != nil {
		log.Fatalf("load reference: %v", err)
	}
	if len(ref.M) == 0 {
		log.Fatal("empty NACE reference — rebuild corpscout.nace_category_embeddings")
	}
	log.Printf("reference: nace=%d page_types=%d dim=%d", len(ref.M), len(protos.P), len(ref.M[0]))

	baseURL := envOr("COMMONCRAWL_EMBED_BASE_URL", "http://localhost:8000/v1")
	model := envOr("COMMONCRAWL_EMBED_MODEL", "")
	if model == "" {
		if model, err = detectModel(baseURL); err != nil {
			log.Fatalf("detect embed model: %v", err)
		}
	}
	log.Printf("embed endpoint=%s model=%s", baseURL, model)
	emb := NewEmbedClient(baseURL, model, *batch, *embedConc)

	getter, err := NewS3Getter(ctx, *region, *anonymous)
	if err != nil {
		log.Fatalf("s3 init: %v", err)
	}

	items, err := readWorklist(*worklist)
	if err != nil {
		log.Fatalf("read worklist: %v", err)
	}
	log.Printf("worklist: %d pages", len(items))

	cfg := ShardConfig{
		CrawlID: *crawlID, SourceRunID: fmt.Sprintf("go-%d", time.Now().Unix()),
		ResolvedAt: time.Now().UTC(), Concurrency: *concurrency, TechMaxBytes: *techMax,
		SkipTech: *skipTech,
	}
	start := time.Now()
	var domains []DomainRow
	var tech []TechRow
	for i := 0; i < len(items); i += *chunkSize {
		end := i + *chunkSize
		if end > len(items) {
			end = len(items)
		}
		d, tk, err := ProcessShard(ctx, items[i:end], getter, emb, ref, protos, cfg)
		if err != nil {
			log.Fatalf("process chunk at %d: %v", i, err)
		}
		domains = append(domains, d...)
		tech = append(tech, tk...)
		el := time.Since(start).Seconds()
		log.Printf("progress: %d/%d domains, %d tech (%.1f domains/s)", len(domains), len(items), len(tech), float64(len(domains))/el)
	}

	domPath, techPath := *out+"-domains.parquet", *out+"-tech.parquet"
	if err := WriteDomains(domPath, domains); err != nil {
		log.Fatalf("write domains: %v", err)
	}
	if err := WriteTech(techPath, tech); err != nil {
		log.Fatalf("write tech: %v", err)
	}
	dur := time.Since(start).Seconds()
	rate := 0.0
	if dur > 0 {
		rate = float64(len(domains)) / dur
	}
	log.Printf("done: %d domains, %d tech rows in %.1fs (%.1f domains/s)", len(domains), len(tech), dur, rate)

	if *s3Bucket != "" {
		s3c := getter.client
		for _, p := range []string{domPath, techPath} {
			key := *s3Prefix + p
			if err := UploadToS3(ctx, s3c, *s3Bucket, key, p); err != nil {
				log.Fatalf("upload %s: %v", p, err)
			}
			log.Printf("uploaded s3://%s/%s", *s3Bucket, key)
		}
	}
}

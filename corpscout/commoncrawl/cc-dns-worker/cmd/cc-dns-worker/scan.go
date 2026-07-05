package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/input"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"
)

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan batch id (default today, UTC)")
	runID := fs.String("run-id", "", "source run id (defaults to scan-id)")
	dbPath := fs.String("db", "scan.db", "SQLite stage path")
	query := fs.String("query", input.DefaultQuery, "ClickHouse query returning root_domain")
	limit := fs.Int("limit", 0, "cap number of domains (0 = all)")
	resolvers := fs.String("resolvers", strings.Join(resolve.DefaultResolvers, ","), "comma-separated recursive resolvers for NS discovery (use 127.0.0.1:53 for a local unbound)")
	discoveryQPS := fs.Float64("discovery-qps", 50, "max queries/sec per recursive resolver (bump high for local unbound)")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per authoritative NS IP")
	inflight := fs.Int("per-server-inflight", 3, "max concurrent queries per NS IP")
	workers := fs.Int("workers", 4000, "max domains resolved concurrently")
	batchN := fs.Int("commit-batch", 200, "domains per SQLite commit")
	seedChunk := fs.Int("seed-chunk", 5000, "domains per SQLite seed transaction")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	_ = fs.Parse(args)
	if *runID == "" {
		*runID = *scanID
	}
	ctx := context.Background()

	// 1) Seed the durable queue from ClickHouse (idempotent; resumes if scan.db already has rows).
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	conn, err := chConn()
	if err != nil {
		return err
	}
	domains, err := input.FromClickHouse(ctx, conn, *query, *limit)
	conn.Close()
	if err != nil {
		return err
	}
	added := 0
	for i := 0; i < len(domains); i += *seedChunk {
		end := i + *seedChunk
		if end > len(domains) {
			end = len(domains)
		}
		n, err := st.Seed(ctx, *scanID, domains[i:end])
		if err != nil {
			return err
		}
		added += n
	}
	pending, err := st.Pending(ctx, *scanID)
	if err != nil {
		return err
	}
	log.Printf("scan_id=%s: %d domains from CH (%d new); %d pending to resolve", *scanID, len(domains), added, len(pending))

	// 2) Two schedulers: discovery (recursive resolvers) and authoritative (per-NS-IP politeness).
	discSched := scheduler.New(scheduler.Config{PerServerQPS: *discoveryQPS, Burst: int(*discoveryQPS), MaxInFlight: *inflight})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: int(*qps), MaxInFlight: *inflight})
	disc := resolve.NewDiscoverer(resolve.NewExchanger(discSched, *timeout), strings.Split(*resolvers, ","))
	rec := resolve.NewResolver(resolve.NewExchanger(authSched, *timeout))
	cfg := records.DefaultConfig()

	// 3) Resolver pool -> results channel -> single writer goroutine (batched SQLite commits).
	results := make(chan model.DomainResult, *batchN*2)
	var writerWG sync.WaitGroup
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		batch := make([]model.DomainResult, 0, *batchN)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if err := st.CommitBatch(ctx, batch); err != nil {
				log.Printf("commit batch: %v", err)
			}
			batch = batch[:0]
		}
		for r := range results {
			batch = append(batch, r)
			if len(batch) >= *batchN {
				flush()
			}
		}
		flush()
	}()

	sem := make(chan struct{}, *workers)
	var wg sync.WaitGroup
	for _, d := range pending {
		sem <- struct{}{}
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			defer func() { <-sem }()
			now := time.Now().UTC()
			del, derr := disc.DiscoverNS(ctx, domain)
			if derr != nil || len(del.NSIPs) == 0 {
				msg := "no authoritative NS IPs"
				if derr != nil {
					msg = derr.Error()
				}
				results <- model.DomainResult{
					ScanID: *scanID, RootDomain: domain, ETLD: del.ETLD,
					Nameservers: del.NS, DSPresent: len(del.DS) > 0,
					Status: "error", Error: msg, SourceRunID: *runID, ResolvedAt: now,
				}
				return
			}
			results <- rec.Resolve(ctx, domain, *scanID, *runID, del, cfg, now)
		}(d)
	}
	wg.Wait()
	close(results)
	writerWG.Wait()
	log.Printf("scan_id=%s: done (%d domains resolved this run)", *scanID, len(pending))
	return nil
}

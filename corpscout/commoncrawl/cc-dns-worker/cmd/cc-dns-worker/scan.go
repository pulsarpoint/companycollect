package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/input"
	"cc-dns-worker/internal/metrics"
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
	resolvers := fs.String("resolvers", "", "REQUIRED: comma-separated recursive resolvers for NS discovery — point at a local resolver, e.g. 127.0.0.1:53 (unbound / PowerDNS Recursor)")
	discoveryQPS := fs.Float64("discovery-qps", 50, "max queries/sec per recursive resolver (bump high for a local resolver)")
	discoveryInflight := fs.Int("discovery-inflight", 500, "max concurrent in-flight queries per recursive resolver — keep high for a local resolver (the authoritative --per-server-inflight stays low)")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per authoritative NS IP")
	inflight := fs.Int("per-server-inflight", 3, "max concurrent queries per authoritative NS IP (Tier-2 politeness; discovery uses --discovery-inflight)")
	hyperscalerQPS := fs.Float64("hyperscaler-qps", 200, "elevated per-server QPS for big anycast DNS providers (Cloudflare/Google/AWS Route53), which absorb far more than --per-server-qps; 0 disables")
	workers := fs.Int("workers", 4000, "max domains resolved concurrently")
	batchN := fs.Int("commit-batch", 200, "domains per SQLite commit")
	seedChunk := fs.Int("seed-chunk", 5000, "domains per SQLite seed transaction")
	dispatchBatch := fs.Int("dispatch-batch", 20000, "domains fetched from the queue and resolved per barrier iteration (bounds memory)")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	breakerThreshold := fs.Int("breaker-threshold", 5, "consecutive transport failures before a server IP's circuit opens (0 disables)")
	breakerCooldown := fs.Duration("breaker-cooldown", 30*time.Second, "how long a server IP's circuit stays open before a half-open probe")
	statsInterval := fs.Duration("stats-interval", 5*time.Second, "how often to print live throughput/traffic stats to stdout (0 = off)")
	_ = fs.Parse(args)
	if *runID == "" {
		*runID = *scanID
	}
	if *seedChunk <= 0 {
		*seedChunk = 5000
	}
	if *workers <= 0 {
		*workers = 1
	}
	if *dispatchBatch <= 0 {
		*dispatchBatch = 20000
	}
	if *batchN <= 0 {
		*batchN = 200
	}
	if *discoveryInflight <= 0 {
		*discoveryInflight = 500
	}
	resolverList := cleanResolvers(strings.Split(*resolvers, ","))
	if len(resolverList) == 0 {
		return fmt.Errorf("--resolvers is required: give a recursive resolver address, e.g. a local unbound/PowerDNS Recursor at 127.0.0.1:53")
	}
	ctx := context.Background()

	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()

	// 1) Stream the queue seed from ClickHouse in batches — never materialize the whole domain list.
	// On a restart where the seed already finished, skip the re-stream entirely and resume straight
	// from the SQLite queue (the marker is only set after a full seed, so an interrupted seed re-runs).
	seeded, err := st.SeedComplete(ctx, *scanID)
	if err != nil {
		return err
	}
	if seeded {
		log.Printf("scan_id=%s: seed already complete — skipping ClickHouse re-stream, resuming from queue", *scanID)
	} else {
		conn, err := chConn()
		if err != nil {
			return err
		}
		added, total := 0, 0
		err = input.StreamClickHouse(ctx, conn, *query, *limit, *seedChunk, func(batch []string) error {
			n, serr := st.Seed(ctx, *scanID, batch)
			if serr != nil {
				return serr
			}
			added += n
			total += len(batch)
			return nil
		})
		conn.Close()
		if err != nil {
			return err
		}
		if err := st.MarkSeedComplete(ctx, *scanID); err != nil {
			return err
		}
		log.Printf("scan_id=%s: seeded %d domains from CH (%d new)", *scanID, total, added)
	}

	// Live metrics: DNS queries sent (traffic) + domains resolved, counted in the hot paths below.
	stats := &metrics.Stats{}

	// 2) Two schedulers + resolver. The exchangers count every query they send into stats.
	discSched := scheduler.New(scheduler.Config{PerServerQPS: *discoveryQPS, Burst: max(1, int(*discoveryQPS)), MaxInFlight: *discoveryInflight, BreakerThreshold: *breakerThreshold, BreakerCooldown: *breakerCooldown})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: *qps, Burst: max(1, int(*qps)), MaxInFlight: *inflight, HyperscalerQPS: *hyperscalerQPS, HyperscalerInFlight: max(*inflight, 40), BreakerThreshold: *breakerThreshold, BreakerCooldown: *breakerCooldown})
	disc := resolve.NewDiscoverer(resolve.NewExchangerWithStats(discSched, *timeout, stats), resolverList)
	rec := resolve.NewResolver(resolve.NewExchangerWithStats(authSched, *timeout, stats))
	cfg := records.DefaultConfig()

	// Print live stats to stdout every --stats-interval while the dispatch loop runs.
	runStart := time.Now()
	stopReporter := make(chan struct{})
	var reporterWG sync.WaitGroup
	if *statsInterval > 0 {
		reporterWG.Add(1)
		go func() {
			defer reporterWG.Done()
			ticker := time.NewTicker(*statsInterval)
			defer ticker.Stop()
			prev := metrics.Snapshot{At: runStart}
			for {
				select {
				case <-stopReporter:
					return
				case now := <-ticker.C:
					cur := stats.Snapshot(now)
					fmt.Println(metrics.Line(prev, cur, runStart))
					prev = cur
				}
			}
		}()
	}

	// 3) Dispatch the pending queue in ordered batches. Each batch is fully resolved AND committed
	// before the next is fetched (commit barrier), so peak memory is one dispatch-batch and in-flight
	// domains are never re-fetched. The cursor advances by root_domain to keep this ~O(n).
	cursor := ""
	resolved := 0
	for {
		batch, err := st.PendingBatch(ctx, *scanID, cursor, *dispatchBatch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			break
		}
		committed, err := resolveBatch(ctx, st, disc, rec, cfg, batch, *scanID, *runID, *workers, *batchN, stats)
		if err != nil {
			return err
		}
		// Defensive: a non-empty batch should always commit at least one domain (every worker sends
		// exactly one result, and real CommitBatch errors already return via the err != nil path
		// above). This only guards the theoretical zero-progress case so the cursor loop can't spin.
		if committed == 0 {
			return fmt.Errorf("no progress: batch of %d domains committed 0", len(batch))
		}
		cursor = batch[len(batch)-1]
		resolved += committed
		log.Printf("scan_id=%s: resolved %d domains (cursor=%q)", *scanID, resolved, cursor)
	}
	close(stopReporter)
	reporterWG.Wait()
	if *statsInterval > 0 {
		fmt.Println(metrics.Line(metrics.Snapshot{At: runStart}, stats.Snapshot(time.Now()), runStart))
	}
	log.Printf("scan_id=%s: done (%d domains resolved this run)", *scanID, resolved)
	return nil
}

// resolveBatch resolves one dispatch-batch of domains concurrently (bounded by workers), collects
// their DomainResults, and commits them to SQLite in commit-batch chunks. It returns the number of
// domains committed. Peak memory is the batch's results, so the caller's cursor loop stays bounded.
func resolveBatch(ctx context.Context, st *store.Store, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, batch []string, scanID, runID string, workers, commitBatch int, stats *metrics.Stats) (int, error) {
	results := make(chan model.DomainResult, commitBatch*2)
	collected := make([]model.DomainResult, 0, len(batch))
	var collectWG sync.WaitGroup
	collectWG.Add(1)
	go func() {
		defer collectWG.Done()
		for r := range results {
			collected = append(collected, r)
			if stats != nil {
				stats.Domains.Add(1)
				if r.Status == "error" {
					stats.DomainErrors.Add(1)
				}
			}
		}
	}()

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, d := range batch {
		sem <- struct{}{}
		wg.Add(1)
		go func(domain string) {
			defer wg.Done()
			defer func() { <-sem }()
			results <- resolveDomain(ctx, disc, rec, cfg, domain, scanID, runID)
		}(d)
	}
	wg.Wait()
	close(results)
	collectWG.Wait()

	committed := 0
	for i := 0; i < len(collected); i += commitBatch {
		end := min(i+commitBatch, len(collected))
		if err := st.CommitBatch(ctx, collected[i:end]); err != nil {
			return committed, fmt.Errorf("commit batch: %w", err)
		}
		committed += end - i
	}
	return committed, nil
}

// resolveDomain discovers a domain's authoritative NS then resolves its Tier-2 records into a
// DomainResult; on discovery failure or no NS IPs it returns a status="error" result.
func resolveDomain(ctx context.Context, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, domain, scanID, runID string) model.DomainResult {
	now := time.Now().UTC()
	del, derr := disc.DiscoverNS(ctx, domain)
	if derr != nil || len(del.NSIPs) == 0 {
		msg := "no authoritative NS IPs"
		if derr != nil {
			msg = derr.Error()
		}
		return model.DomainResult{
			ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
			Nameservers: del.NS, DSPresent: len(del.DS) > 0,
			Status: "error", Error: msg, SourceRunID: runID, ResolvedAt: now,
		}
	}
	return rec.Resolve(ctx, domain, scanID, runID, del, cfg, now)
}

// cleanResolvers drops empty/whitespace-only tokens (e.g. from a trailing comma or blank
// --resolvers flag) so a malformed flag can't silently produce a zero-length resolver list that then
// fails every domain's discovery one at a time.
func cleanResolvers(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if t := strings.TrimSpace(r); t != "" {
			out = append(out, t)
		}
	}
	return out
}

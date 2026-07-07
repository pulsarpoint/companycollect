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

// scanConfig holds every knob a scan cycle needs. runScan fills it from flags; the orchestrator
// (run.go) builds it per cycle with a timestamped scan-id/db.
type scanConfig struct {
	scanID, runID     string
	query             string
	limit             int
	resolvers         []string
	discoveryQPS      float64
	discoveryInflight int
	qps               float64
	inflight          int
	hyperscalerQPS    float64
	workers           int
	commitBatch       int
	seedChunk         int
	dispatchBatch     int
	timeout           time.Duration
	breakerThreshold  int
	breakerCooldown   time.Duration
	statsInterval     time.Duration
}

// scanFlags defines the shared scan tunables on fs (used by both `scan` and `run`) and returns a
// closure that builds a scanConfig from them once fs is parsed.
func scanFlags(fs *flag.FlagSet) func() (scanConfig, error) {
	query := fs.String("query", input.DefaultQuery, "ClickHouse query returning root_domain")
	limit := fs.Int("limit", 0, "cap number of domains (0 = all)")
	resolvers := fs.String("resolvers", "", "REQUIRED: comma-separated recursive resolvers for NS discovery — point at a local resolver, e.g. 127.0.0.1:53 (unbound / PowerDNS Recursor)")
	discoveryQPS := fs.Float64("discovery-qps", 50, "max queries/sec per recursive resolver (bump high for a local resolver)")
	discoveryInflight := fs.Int("discovery-inflight", 500, "max concurrent in-flight queries per recursive resolver")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per authoritative NS IP")
	inflight := fs.Int("per-server-inflight", 3, "max concurrent queries per authoritative NS IP (Tier-2 politeness)")
	hyperscalerQPS := fs.Float64("hyperscaler-qps", 200, "elevated per-server QPS for big anycast DNS providers (Cloudflare/Google/AWS/UltraDNS); 0 disables")
	workers := fs.Int("workers", 4000, "max domains resolved concurrently")
	batchN := fs.Int("commit-batch", 200, "domains per SQLite commit")
	seedChunk := fs.Int("seed-chunk", 5000, "domains per SQLite seed transaction")
	dispatchBatch := fs.Int("dispatch-batch", 20000, "domains the feeder pulls from the queue per fetch (streaming)")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	breakerThreshold := fs.Int("breaker-threshold", 5, "consecutive transport failures before a server IP's circuit opens (0 disables)")
	breakerCooldown := fs.Duration("breaker-cooldown", 30*time.Second, "how long a server IP's circuit stays open before a half-open probe")
	statsInterval := fs.Duration("stats-interval", 5*time.Second, "how often to print live throughput/traffic stats (0 = off)")
	return func() (scanConfig, error) {
		resolverList := cleanResolvers(strings.Split(*resolvers, ","))
		if len(resolverList) == 0 {
			return scanConfig{}, fmt.Errorf("--resolvers is required: give a recursive resolver address, e.g. a local unbound/PowerDNS Recursor at 127.0.0.1:53")
		}
		return scanConfig{
			query: *query, limit: *limit, resolvers: resolverList,
			discoveryQPS: *discoveryQPS, discoveryInflight: *discoveryInflight,
			qps: *qps, inflight: *inflight, hyperscalerQPS: *hyperscalerQPS,
			workers: *workers, commitBatch: *batchN, seedChunk: *seedChunk, dispatchBatch: *dispatchBatch,
			timeout: *timeout, breakerThreshold: *breakerThreshold, breakerCooldown: *breakerCooldown,
			statsInterval: *statsInterval,
		}, nil
	}
}

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan batch id (default today, UTC)")
	runID := fs.String("run-id", "", "source run id (defaults to scan-id)")
	dbPath := fs.String("db", "scan.db", "SQLite stage path")
	build := scanFlags(fs)
	_ = fs.Parse(args)
	cfg, err := build()
	if err != nil {
		return err
	}
	cfg.scanID = *scanID
	cfg.runID = *runID
	if cfg.runID == "" {
		cfg.runID = cfg.scanID
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	return scanCycle(context.Background(), st, cfg)
}

// scanCycle seeds (if needed) and resolves the pending queue for cfg.scanID into st. It does NOT open
// or close st, so the orchestrator can share the same store with a concurrent incremental loader.
func scanCycle(ctx context.Context, st *store.Store, cfg scanConfig) error {
	if cfg.seedChunk <= 0 {
		cfg.seedChunk = 5000
	}
	if cfg.workers <= 0 {
		cfg.workers = 1
	}
	if cfg.dispatchBatch <= 0 {
		cfg.dispatchBatch = 20000
	}
	if cfg.commitBatch <= 0 {
		cfg.commitBatch = 200
	}
	if cfg.discoveryInflight <= 0 {
		cfg.discoveryInflight = 500
	}

	// 1) Seed the queue from ClickHouse, unless a prior seed for this scan-id already finished.
	seeded, err := st.SeedComplete(ctx, cfg.scanID)
	if err != nil {
		return err
	}
	if seeded {
		log.Printf("scan_id=%s: seed already complete — skipping ClickHouse re-stream, resuming from queue", cfg.scanID)
	} else {
		conn, err := chConn()
		if err != nil {
			return err
		}
		added, total := 0, 0
		err = input.StreamClickHouse(ctx, conn, cfg.query, cfg.limit, cfg.seedChunk, func(batch []string) error {
			n, serr := st.Seed(ctx, cfg.scanID, batch)
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
		if err := st.MarkSeedComplete(ctx, cfg.scanID); err != nil {
			return err
		}
		log.Printf("scan_id=%s: seeded %d domains from CH (%d new)", cfg.scanID, total, added)
	}

	stats := &metrics.Stats{}

	// 2) Two schedulers + resolver. No breaker on discovery (single shared local resolver — a breaker
	// there fast-fails every domain); it stays on authSched for the many authoritative servers.
	discSched := scheduler.New(scheduler.Config{PerServerQPS: cfg.discoveryQPS, Burst: max(1, int(cfg.discoveryQPS)), MaxInFlight: cfg.discoveryInflight, BreakerThreshold: 0, BreakerCooldown: cfg.breakerCooldown})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: cfg.qps, Burst: max(1, int(cfg.qps)), MaxInFlight: cfg.inflight, HyperscalerQPS: cfg.hyperscalerQPS, HyperscalerInFlight: max(cfg.inflight, 40), BreakerThreshold: cfg.breakerThreshold, BreakerCooldown: cfg.breakerCooldown})
	disc := resolve.NewDiscoverer(resolve.NewExchangerWithStats(discSched, cfg.timeout, stats), cfg.resolvers)
	rec := resolve.NewResolver(resolve.NewExchangerWithStats(authSched, cfg.timeout, stats))
	rcfg := records.DefaultConfig()

	runStart := time.Now()
	stopReporter := make(chan struct{})
	var reporterWG sync.WaitGroup
	if cfg.statsInterval > 0 {
		reporterWG.Add(1)
		go func() {
			defer reporterWG.Done()
			ticker := time.NewTicker(cfg.statsInterval)
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

	// 3) Streaming dispatch — feeder → workers → single committer, no commit barrier.
	work := make(chan string, cfg.workers)
	results := make(chan model.DomainResult, cfg.commitBatch*2)
	var feedErr error
	go func() {
		defer close(work)
		cursor := ""
		for {
			batch, err := st.PendingBatch(ctx, cfg.scanID, cursor, cfg.dispatchBatch)
			if err != nil {
				feedErr = err
				return
			}
			if len(batch) == 0 {
				return
			}
			for _, d := range batch {
				select {
				case work <- d:
				case <-ctx.Done():
					return
				}
			}
			cursor = batch[len(batch)-1]
		}
	}()
	var workerWG sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			for d := range work {
				results <- resolveDomain(ctx, disc, rec, rcfg, d, cfg.scanID, cfg.runID)
			}
		}()
	}
	go func() { workerWG.Wait(); close(results) }()

	resolved, lastLog := 0, 0
	buf := make([]model.DomainResult, 0, cfg.commitBatch)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := st.CommitBatch(ctx, buf); err != nil {
			return fmt.Errorf("commit batch: %w", err)
		}
		resolved += len(buf)
		buf = buf[:0]
		if resolved-lastLog >= 20000 {
			log.Printf("scan_id=%s: resolved %d domains", cfg.scanID, resolved)
			lastLog = resolved
		}
		return nil
	}
	for r := range results {
		stats.Domains.Add(1)
		if r.Status == "error" {
			stats.DomainErrors.Add(1)
		}
		buf = append(buf, r)
		if len(buf) >= cfg.commitBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if feedErr != nil {
		return feedErr
	}
	close(stopReporter)
	reporterWG.Wait()
	if cfg.statsInterval > 0 {
		fmt.Println(metrics.Line(metrics.Snapshot{At: runStart}, stats.Snapshot(time.Now()), runStart))
	}
	log.Printf("scan_id=%s: done (%d domains resolved this run)", cfg.scanID, resolved)
	return nil
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

// cleanResolvers drops empty/whitespace-only tokens so a malformed --resolvers flag can't silently
// produce a zero-length resolver list.
func cleanResolvers(raw []string) []string {
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		if t := strings.TrimSpace(r); t != "" {
			out = append(out, t)
		}
	}
	return out
}

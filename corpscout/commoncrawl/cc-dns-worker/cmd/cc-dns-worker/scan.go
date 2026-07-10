package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cc-dns-worker/internal/hostsource"
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
	maxDomains        int
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
	axfr              bool
	axfrWorkers       int
	axfrQPS           float64
	axfrInflight      int
	axfrMaxRecords    int
	axfrMaxBytes      int
	axfrTimeout       time.Duration
	hostEnrich        bool
	hostCap           int
	hostConcurrency   int
}

// scanFlags defines the shared scan tunables on fs (used by both `scan` and `run`) and returns a
// closure that builds a scanConfig from them once fs is parsed.
func scanFlags(fs *flag.FlagSet) func() (scanConfig, error) {
	query := fs.String("query", input.DefaultQuery, "ClickHouse query returning root_domain")
	maxDomains := fs.Int("max-domains", 0, "cap number of domains to scan this run (0 = all)")
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
	axfr := fs.Bool("axfr", false, "enable the post-scan AXFR phase (a separate worker pool probes resolved domains' non-hyperscaler NS for open zones) — master switch, default off")
	axfrWorkers := fs.Int("axfr-workers", 50, "AXFR phase: max concurrent domain probers (its own pool, never touches resolution)")
	axfrQPS := fs.Float64("axfr-qps", 5, "max AXFR transfers/sec per NS IP")
	axfrInflight := fs.Int("axfr-inflight", 50, "max total concurrent AXFR transfers across all domains")
	axfrMaxRecords := fs.Int("axfr-max-records", 50000, "stop draining a zone past this many records")
	axfrMaxBytes := fs.Int("axfr-max-bytes", 67108864, "stop draining a zone past this running byte sum")
	axfrTimeout := fs.Duration("axfr-timeout", 20*time.Second, "whole-transfer timeout per AXFR")
	hostEnrich := fs.Bool("host-enrich", false, "enable CT + registry hostname enrichment (seed-time) — master switch, default off")
	hostCap := fs.Int("host-cap", 100, "max discovered hosts per domain unioned into the scan")
	hostConcurrency := fs.Int("host-concurrency", 4, "seed-time host-load: hash shards enriched concurrently (16 partition-aligned shards total; keep modest to spare the shared ctlogs node)")
	return func() (scanConfig, error) {
		resolverList := cleanResolvers(strings.Split(*resolvers, ","))
		if len(resolverList) == 0 {
			return scanConfig{}, fmt.Errorf("--resolvers is required: give a recursive resolver address, e.g. a local unbound/PowerDNS Recursor at 127.0.0.1:53")
		}
		return scanConfig{
			query: *query, maxDomains: *maxDomains, resolvers: resolverList,
			discoveryQPS: *discoveryQPS, discoveryInflight: *discoveryInflight,
			qps: *qps, inflight: *inflight, hyperscalerQPS: *hyperscalerQPS,
			workers: *workers, commitBatch: *batchN, seedChunk: *seedChunk, dispatchBatch: *dispatchBatch,
			timeout: *timeout, breakerThreshold: *breakerThreshold, breakerCooldown: *breakerCooldown,
			statsInterval: *statsInterval,
			axfr:          *axfr, axfrWorkers: *axfrWorkers, axfrQPS: *axfrQPS, axfrInflight: *axfrInflight,
			axfrMaxRecords: *axfrMaxRecords, axfrMaxBytes: *axfrMaxBytes, axfrTimeout: *axfrTimeout,
			hostEnrich: *hostEnrich, hostCap: *hostCap, hostConcurrency: *hostConcurrency,
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

// applyScanDefaults fills zero/negative tunables with safe defaults so each phase run is self-contained.
func applyScanDefaults(cfg scanConfig) scanConfig {
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
	return cfg
}

// scanCycle seeds then resolves — the single-shot path used by the standalone `scan` subcommand. The
// orchestrator (run.go) instead runs seedCycle and scanResolve as separate crash-safe phases so status
// reflects the long seeding/host-load window distinctly from actual resolution. Does NOT open/close st.
func scanCycle(ctx context.Context, st *store.Store, cfg scanConfig) error {
	if err := seedCycle(ctx, st, cfg); err != nil {
		return err
	}
	return scanResolve(ctx, st, cfg)
}

// seedCycle runs the SEEDING phase: stream domains from ClickHouse into the SQLite queue (unless a
// prior seed for this scan-id finished) and, when --host-enrich is set, the CT+registry host-load
// (the long part — enrichment for tens of millions of domains). Resumable + idempotent; populates the
// queue that scanResolve consumes. No records are produced yet, so no incremental CH load runs here.
func seedCycle(ctx context.Context, st *store.Store, cfg scanConfig) error {
	cfg = applyScanDefaults(cfg)

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
		err = input.StreamClickHouse(ctx, conn, cfg.query, cfg.maxDomains, cfg.seedChunk, func(batch []string) error {
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

	// 1b) Host-load phase: union CT + registry discovered hostnames into scan_hostnames (resumable,
	// skipped on resume or when disabled). Runs before dispatch so the plan can consume it.
	if cfg.hostEnrich {
		if err := hostLoadPhase(ctx, st, cfg); err != nil {
			return err
		}
	}
	return nil
}

// scanResolve runs the SCANNING phase: resolve the pending queue into the SQLite stage. Assumes the
// seeding phase (seedCycle) already populated the queue (and scan_hostnames when --host-enrich). Does
// NOT open/close st, so the orchestrator can share the store with a concurrent incremental loader.
func scanResolve(ctx context.Context, st *store.Store, cfg scanConfig) error {
	cfg = applyScanDefaults(cfg)

	stats := &metrics.Stats{}

	// 2) Two schedulers + resolver. No breaker on discovery (single shared local resolver — a breaker
	// there fast-fails every domain); it stays on authSched for the many authoritative servers.
	discSched := scheduler.New(scheduler.Config{PerServerQPS: cfg.discoveryQPS, Burst: max(1, int(cfg.discoveryQPS)), MaxInFlight: cfg.discoveryInflight, BreakerThreshold: 0, BreakerCooldown: cfg.breakerCooldown})
	authSched := scheduler.New(scheduler.Config{PerServerQPS: cfg.qps, Burst: max(1, int(cfg.qps)), MaxInFlight: cfg.inflight, HyperscalerQPS: cfg.hyperscalerQPS, HyperscalerInFlight: max(cfg.inflight, 40), BreakerThreshold: cfg.breakerThreshold, BreakerCooldown: cfg.breakerCooldown})
	disc := resolve.NewDiscoverer(resolve.NewExchangerWithStats(discSched, cfg.timeout, stats), cfg.resolvers)
	rec := resolve.NewResolverWithStats(resolve.NewExchangerWithStats(authSched, cfg.timeout, stats), stats)
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
	type domainWork struct {
		domain string
		hosts  []model.HostLabel
	}
	work := make(chan domainWork, cfg.workers)
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
			hostsByDomain, herr := st.HostnamesForBatch(ctx, cfg.scanID, batch)
			if herr != nil {
				feedErr = herr
				return
			}
			for _, d := range batch {
				select {
				case work <- domainWork{domain: d, hosts: hostsByDomain[d]}:
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
			for w := range work {
				results <- resolveDomain(ctx, disc, rec, rcfg, w.domain, cfg.scanID, cfg.runID, w.hosts)
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

// hostLoadPhase populates scan_hostnames for cfg.scanID from CT (ctlogs.hostnames) and the registry
// (commoncrawl_domain_hostnames), capped at cfg.hostCap per domain. Rather than thousands of per-batch
// IN() lookups, it splits the domain space into hostsource.CTPartitions hash shards aligned to the
// ctlogs physical partitioning — each shard's query prunes to one partition and semi-joins our seed
// set server-side — and runs up to cfg.hostConcurrency shards at once. Resumable per shard via
// host_load_shards; idempotent (INSERT OR IGNORE). host_load_state is marked complete once every shard
// finished, so a restart re-runs only the shards not yet done and the scanning phase never starts on a
// partial enrichment.
func hostLoadPhase(ctx context.Context, st *store.Store, cfg scanConfig) error {
	if cfg.hostCap <= 0 {
		cfg.hostCap = 100
	}
	if cfg.hostConcurrency <= 0 {
		cfg.hostConcurrency = 4
	}
	done, err := st.HostLoadComplete(ctx, cfg.scanID)
	if err != nil {
		return err
	}
	if done {
		log.Printf("scan_id=%s: host-load already complete — skipping", cfg.scanID)
		return nil
	}
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()

	// Cancel in-flight shard queries as soon as any shard fails, so we don't keep hammering CH.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	numShards := hostsource.CTPartitions
	sem := make(chan struct{}, cfg.hostConcurrency)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	setErr := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
			cancel()
		}
		mu.Unlock()
	}
	var totalRows, doneShards int64
	start := time.Now()

	for shard := 0; shard < numShards; shard++ {
		complete, err := st.HostLoadShardComplete(ctx, cfg.scanID, shard)
		if err != nil {
			return err
		}
		if complete {
			atomic.AddInt64(&doneShards, 1)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(shard int) {
			defer wg.Done()
			defer func() { <-sem }()
			mu.Lock()
			stop := firstErr != nil
			mu.Unlock()
			if stop {
				return
			}
			sStart := time.Now()
			ct, err := hostsource.CTShard(ctx, conn, shard, numShards, cfg.hostCap)
			if err != nil {
				setErr(fmt.Errorf("CT shard %d: %w", shard, err))
				return
			}
			reg, err := hostsource.RegistryShard(ctx, conn, shard, numShards, cfg.hostCap)
			if err != nil {
				setErr(fmt.Errorf("registry shard %d: %w", shard, err))
				return
			}
			n, err := st.InsertHostnamesMap(ctx, cfg.scanID, hostsource.Merge(ct, reg, cfg.hostCap))
			if err != nil {
				setErr(fmt.Errorf("insert shard %d: %w", shard, err))
				return
			}
			if err := st.MarkHostLoadShardComplete(ctx, cfg.scanID, shard); err != nil {
				setErr(fmt.Errorf("mark shard %d: %w", shard, err))
				return
			}
			atomic.AddInt64(&totalRows, int64(n))
			d := atomic.AddInt64(&doneShards, 1)
			log.Printf("scan_id=%s: host-load shard %d done (%d hostnames, %s) — %d/%d shards complete",
				cfg.scanID, shard, n, time.Since(sStart).Round(time.Second), d, numShards)
		}(shard)
	}
	wg.Wait()
	if firstErr != nil {
		return firstErr
	}
	if err := st.MarkHostLoadComplete(ctx, cfg.scanID); err != nil {
		return err
	}
	log.Printf("scan_id=%s: host-load complete (%d hostnames across %d shards, %s)",
		cfg.scanID, atomic.LoadInt64(&totalRows), numShards, time.Since(start).Round(time.Second))
	return nil
}

// resolveDomain discovers a domain's authoritative NS then resolves its Tier-2 records into a
// DomainResult; on discovery failure or no NS IPs it returns a status="error" result. AXFR is NOT done
// here — it runs as a separate post-scan phase (axfr.go) so it can never throttle resolution.
func resolveDomain(ctx context.Context, disc *resolve.Discoverer, rec *resolve.Resolver, cfg records.Config, domain, scanID, runID string, extra []model.HostLabel) model.DomainResult {
	now := time.Now().UTC()
	del, derr := disc.DiscoverNS(ctx, domain)
	if derr != nil || len(del.NSIPs) == 0 {
		msg := "no authoritative NS IPs"
		if derr != nil {
			msg = derr.Error()
		}
		return model.DomainResult{
			ScanID: scanID, RootDomain: domain, ETLD: del.ETLD,
			Nameservers: del.NS, NSIPs: del.NSIPs, DSPresent: len(del.DS) > 0,
			Status: "error", Error: msg, SourceRunID: runID, ResolvedAt: now,
		}
	}
	return rec.Resolve(ctx, domain, scanID, runID, del, cfg, now, extra)
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

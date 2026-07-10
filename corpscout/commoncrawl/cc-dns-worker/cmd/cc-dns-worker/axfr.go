package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cc-dns-worker/internal/load"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"
)

// axfrDispatchBatch bounds how many pending domains the AXFR feeder pulls from the SQLite queue per
// fetch (mirrors scanResolve's dispatchBatch, but sized for the AXFR phase's much lighter per-domain
// payload — no record-plan work, just a handful of NS endpoints).
const axfrDispatchBatch = 5000

// axfrProber is the subset of *resolve.AXFRProber the pipeline calls, narrowed to an interface so tests
// can inject a fake (no real network) to exercise the feeder/worker/committer shutdown paths.
type axfrProber interface {
	ProbeServer(ctx context.Context, zone, nsHost, nsIP string) resolve.AXFROutcome
}

// axfrPendingFunc streams one page of pending domains — the store's PendingAXFRDomains in production,
// a fixed fake in tests. See runAXFRQueue.
type axfrPendingFunc func(ctx context.Context, scanID, afterRootDomain string, limit int) ([]store.AXFRTarget, error)

// axfrCommitFunc persists one domain's probe outcomes + selected zone and marks it done — the store's
// CommitAXFRDomain in production. Narrowed to a function value (rather than calling the store directly)
// so a test can inject a failing committer and prove the cancel-and-join shutdown path leaks no
// goroutines under -race.
type axfrCommitFunc func(ctx context.Context, scanID, domain string, probes []resolve.AXFROutcome, zone []model.DNSRecord, runID string, observedAt time.Time, errMsg string) error

// axfrDomainResult is one domain's complete probe pass, handed from a worker to the committer. zone is
// nil unless some endpoint opened.
type axfrDomainResult struct {
	domain     string
	probes     []resolve.AXFROutcome
	zone       []model.DNSRecord
	observedAt time.Time
}

// axfrLoadFunc runs the AXFR ClickHouse load pass for scanID — axfrLoad (a real ClickHouse connection)
// in production, a fake in tests that proves ordering/gating without a network dependency. See
// axfrCycle.
type axfrLoadFunc func(ctx context.Context, st *store.Store, scanID string) error

// axfrCycle runs the COMPLETE AXFR step for one scan-id: stage+probe every resolved domain
// (runAXFRPipeline, SQLite-only) and then the AXFR ClickHouse load pass (loadFn) that turns the staged
// probes into dns_axfr_latest/dns_axfr_state_changes rows. This is the ONE function both the
// orchestrator's AXFR phase (run.go's runAXFRPhase) and the standalone `scan --axfr` step (scan.go's
// runScan) call — there is no second AXFR code path, so the two entry points can never drift apart.
// Production always passes axfrLoad as loadFn; callers are expected to gate calling axfrCycle at all
// behind cfg.axfr themselves (mirroring runAXFRPipeline's own contract) so --axfr=false does zero AXFR
// work — no queue seeding, no prober, no ClickHouse connection.
func axfrCycle(ctx context.Context, st *store.Store, cfg scanConfig, loadFn axfrLoadFunc) error {
	if err := runAXFRPipeline(ctx, st, cfg); err != nil {
		return err
	}
	return loadFn(ctx, st, cfg.scanID)
}

// axfrLoad opens a fresh ClickHouse connection and runs one AXFR load pass (staged axfr_probes/
// axfr_changes → dns_axfr_latest/dns_axfr_state_changes) for scanID. This is axfrCycle's production
// loadFn.
func axfrLoad(ctx context.Context, st *store.Store, scanID string) error {
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	return load.LoadAXFR(ctx, conn, st, scanID, time.Now())
}

// runAXFRPipeline runs the standalone AXFR phase against the durable SQLite work queue (axfr_domains):
// SeedAXFRDomains (idempotent) stages every resolved domain that has a dialable NS endpoint set, then —
// unless the queue is already fully drained — a feeder streams PENDING rows to a bounded worker pool
// that probes each dialable, non-hyperscaler NS endpoint (ProbeServer) and returns one result per
// DOMAIN (every endpoint's outcome plus the first open zone found), and a single committer persists each
// result — probes, zone records, and the domain's done-status — in ONE transaction
// (store.CommitAXFRDomain). Staged first, checkpointed second: a crash between dispatch and commit
// leaves the domain 'pending' (the next run's PendingAXFRDomains returns it again), so a killed process
// can only REPEAT unfinished work, never SKIP it. This function itself stages to SQLite only — no
// ClickHouse I/O here; axfrCycle (above) is what pairs it with the ClickHouse load pass.
func runAXFRPipeline(ctx context.Context, st *store.Store, cfg scanConfig) error {
	if _, err := st.SeedAXFRDomains(ctx, cfg.scanID); err != nil {
		return fmt.Errorf("seed axfr queue: %w", err)
	}
	done, err := st.AXFRQueueComplete(ctx, cfg.scanID)
	if err != nil {
		return err
	}
	if done {
		log.Printf("scan_id=%s: AXFR queue already complete — skipping (no network requests)", cfg.scanID)
		return nil
	}

	workers := cfg.axfrWorkers
	if workers <= 0 {
		workers = 50
	}

	// AXFR scheduler lane + prober: one transfer per server IP at a time, aggregate inflight cap.
	axfrSched := scheduler.New(scheduler.Config{
		PerServerQPS:     cfg.axfrQPS,
		Burst:            max(1, int(cfg.axfrQPS)),
		MaxInFlight:      1,
		BreakerThreshold: cfg.breakerThreshold,
		BreakerCooldown:  cfg.breakerCooldown,
	})
	prober := resolve.NewAXFRProber(axfrSched, resolve.AXFRCaps{
		MaxRecords: cfg.axfrMaxRecords, MaxBytes: cfg.axfrMaxBytes, Deadline: cfg.axfrTimeout,
	}, cfg.axfrInflight)
	log.Printf("scan_id=%s: AXFR phase START (workers=%d inflight=%d timeout=%s)",
		cfg.scanID, workers, cfg.axfrInflight, cfg.axfrTimeout)

	if err := runAXFRQueue(ctx, axfrQueueDeps{
		scanID:        cfg.scanID,
		runID:         cfg.runID,
		workers:       workers,
		statsInterval: cfg.statsInterval,
		pending:       st.PendingAXFRDomains,
		prober:        prober,
		commit:        st.CommitAXFRDomain,
	}); err != nil {
		return err
	}
	log.Printf("scan_id=%s: AXFR phase complete", cfg.scanID)
	return nil
}

// axfrQueueDeps is runAXFRQueue's injectable seam: production wiring comes from runAXFRPipeline (a real
// *resolve.AXFRProber and store.Store methods); tests substitute a fake prober and/or a failing commit
// function to exercise the shutdown path without a network or a forced-failure store fixture.
type axfrQueueDeps struct {
	scanID, runID string
	workers       int
	statsInterval time.Duration
	pending       axfrPendingFunc
	prober        axfrProber
	commit        axfrCommitFunc
}

// runAXFRQueue is the feeder → worker pool → committer pipeline over the axfr_domains queue. Shutdown
// contract: on ANY commit error, the committer cancels ctx and keeps draining results (never blocking a
// worker mid-send) until the worker pool finishes and closes the channel; the feeder and workers both
// select on ctx.Done() so a cancel unblocks them promptly instead of running to natural completion. Every
// goroutine this function starts (feeder, workers, committer, stats reporter) is Wait()'d/joined before
// it returns, on every exit path — no leaks whether it returns cleanly, on a feed error, or on a commit
// error.
func runAXFRQueue(ctx context.Context, deps axfrQueueDeps) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := deps.workers
	if workers <= 0 {
		workers = 50
	}

	start := time.Now()
	var domainsProbed, domainsCommitted, nsProbed, nsOpen atomic.Int64

	// Periodic AXFR stats: processing speed + open-vs-tested running ratio. Stopped and joined on every
	// exit path via the deferred close(stopStats)/<-statsDone below.
	stopStats := make(chan struct{})
	statsDone := make(chan struct{})
	if deps.statsInterval > 0 {
		go func() {
			defer close(statsDone)
			ticker := time.NewTicker(deps.statsInterval)
			defer ticker.Stop()
			lastDomains, lastTick := int64(0), start
			for {
				select {
				case <-stopStats:
					return
				case now := <-ticker.C:
					domains := domainsProbed.Load()
					probed := nsProbed.Load()
					opened := nsOpen.Load()
					perSec := float64(domains-lastDomains) / now.Sub(lastTick).Seconds()
					avgPerSec := float64(domains) / now.Sub(start).Seconds()
					lastDomains, lastTick = domains, now
					openPct := 0.0
					if probed > 0 {
						openPct = 100 * float64(opened) / float64(probed)
					}
					log.Printf("axfr stats: elapsed=%s domains=%d (%.0f/s, avg %.0f/s) ns_tested=%d open=%d (%.3f%% of tested)",
						now.Sub(start).Round(time.Second), domains, perSec, avgPerSec, probed, opened, openPct)
				}
			}
		}()
	} else {
		close(statsDone)
	}
	defer func() {
		close(stopStats)
		<-statsDone
	}()

	work := make(chan store.AXFRTarget, workers*2)
	results := make(chan axfrDomainResult, workers*2)

	// Feeder: owns work's close. Streams PENDING domains in root_domain order; the cursor lives only in
	// this local variable — never persisted — so durable resume comes solely from axfr_domains.status.
	feederDone := make(chan struct{})
	var feedErr error
	go func() {
		defer close(feederDone)
		defer close(work)
		cursor := ""
		for {
			targets, err := deps.pending(ctx, deps.scanID, cursor, axfrDispatchBatch)
			if err != nil {
				feedErr = err
				cancel() // fatal: stop workers/committer promptly instead of draining to natural completion
				return
			}
			if len(targets) == 0 {
				return
			}
			for _, t := range targets {
				select {
				case work <- t:
				case <-ctx.Done():
					return
				}
			}
			cursor = targets[len(targets)-1].RootDomain
		}
	}()

	// Workers: probe every dialable, non-hyperscaler endpoint of each dispatched domain, then hand the
	// whole domain's outcome to the committer. The select-send lets a worker abandon a result the instant
	// ctx is cancelled instead of blocking forever on a channel nobody drains anymore.
	var workerWG sync.WaitGroup
	for range workers {
		workerWG.Go(func() {
			for t := range work {
				r := processAXFRTarget(ctx, deps.prober, t)
				probed, opened := 0, 0
				for _, p := range r.probes {
					probed++
					if p.IsOpen() {
						opened++
					}
				}
				nsProbed.Add(int64(probed))
				nsOpen.Add(int64(opened))
				domainsProbed.Add(1)
				select {
				case results <- r:
				case <-ctx.Done():
					return
				}
			}
		})
	}

	// Committer: the ONE goroutine that writes to SQLite. On its first commit error it records the error
	// and cancels ctx (unblocking the feeder's and workers' ctx.Done() selects), then keeps ranging over
	// results — draining, not committing — so no worker is ever left blocked mid-send; draining ends the
	// instant close(results) fires below, which happens once every worker has exited.
	committerDone := make(chan struct{})
	var commitErr error
	go func() {
		defer close(committerDone)
		for r := range results {
			if commitErr != nil {
				continue // already failed: drain only, never call the committer again
			}
			if err := deps.commit(ctx, deps.scanID, r.domain, r.probes, r.zone, deps.runID, r.observedAt, ""); err != nil {
				commitErr = fmt.Errorf("commit axfr domain %q: %w", r.domain, err)
				cancel()
				continue
			}
			domainsCommitted.Add(1)
		}
	}()

	<-feederDone
	workerWG.Wait()
	close(results)
	<-committerDone

	if feedErr != nil {
		return feedErr
	}
	if commitErr != nil {
		return commitErr
	}
	log.Printf("axfr queue drained: %d domains probed, %d committed, %d ns probed, %d open",
		domainsProbed.Load(), domainsCommitted.Load(), nsProbed.Load(), nsOpen.Load())
	return nil
}

// processAXFRTarget probes every dialable, non-hyperscaler NS endpoint of one domain and returns every
// endpoint's outcome plus the first zone that opened (nil if none did). It never touches storage — the
// committer persists the result — so it has no error return: an unreachable or refusing server is a
// normal, fully-formed AXFROutcome (Unknown/Closed verdict), not a Go error.
func processAXFRTarget(ctx context.Context, prober axfrProber, t store.AXFRTarget) axfrDomainResult {
	now := time.Now().UTC()
	var zone []model.DNSRecord
	var probes []resolve.AXFROutcome
	seenIP := map[string]bool{} // one IP can back several NS names (shared/anycast) — probe it once
	for _, ep := range t.Endpoints {
		if !ep.Dialable {
			continue // never dial a non-public endpoint (loopback/private/link-local/etc. — evidence only)
		}
		if scheduler.IsHyperscaler(ep.IP) {
			continue // skip this NS — hyperscalers never allow AXFR
		}
		if seenIP[ep.IP] {
			continue
		}
		seenIP[ep.IP] = true
		res := prober.ProbeServer(ctx, t.RootDomain, ep.Name, ep.IP)
		probes = append(probes, res)
		if res.IsOpen() && zone == nil {
			zone = res.Zone // capture the transferred zone once per domain
		}
	}
	return axfrDomainResult{domain: t.RootDomain, probes: probes, zone: zone, observedAt: now}
}

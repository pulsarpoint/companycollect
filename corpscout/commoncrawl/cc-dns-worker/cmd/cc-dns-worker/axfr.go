package main

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"
)

// runAXFRPipeline runs the standalone AXFR phase: a bounded worker pool that walks the resolved-domain
// cursor (AXFRTargetsAfter — reusing the ns_ips resolution already stored, so no NS re-discovery) and,
// for each domain, probes every NON-hyperscaler NS IP. It records a per-NS row in dns_axfr and appends
// any open zone's transferred records to scan_records (source='axfr'). This lives entirely off the
// resolution path — its own pool, run as a phase after scanning — so it can never throttle resolving.
// Resumable via axfr_state; idempotent (dns_axfr upsert; records appended after the domain committed).
func runAXFRPipeline(ctx context.Context, st *store.Store, cfg scanConfig) error {
	done, err := st.AXFRComplete(ctx, cfg.scanID)
	if err != nil {
		return err
	}
	if done {
		log.Printf("scan_id=%s: AXFR phase already complete — skipping", cfg.scanID)
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
	log.Printf("scan_id=%s: AXFR phase START (workers=%d qps=%.1f inflight=%d timeout=%s)",
		cfg.scanID, workers, cfg.axfrQPS, cfg.axfrInflight, cfg.axfrTimeout)

	jobs := make(chan store.AXFRTarget, workers*2)
	// Running counters, read by the stats goroutine (atomics, not a lock, since they're monotonic).
	var nsProbed, nsOpen, domainsProbed atomic.Int64
	var pool sync.WaitGroup
	for range workers {
		pool.Go(func() {
			for t := range jobs {
				probed, opened := processAXFRTarget(ctx, st, prober, cfg, t)
				nsProbed.Add(int64(probed))
				nsOpen.Add(int64(opened))
				domainsProbed.Add(1)
			}
		})
	}

	// Periodic AXFR stats: processing speed + open-vs-tested running ratio.
	start := time.Now()
	stopStats := make(chan struct{})
	var statsWG sync.WaitGroup
	if cfg.statsInterval > 0 {
		statsWG.Go(func() {
			ticker := time.NewTicker(cfg.statsInterval)
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
		})
	}

	// Feeder: walk the resolved-domain cursor, dispatch to the pool, advance the cursor per batch.
	const batch = 5000
	cursor, cerr := st.AXFRCursor(ctx, cfg.scanID)
	var feedErr error
	if cerr != nil {
		feedErr = cerr
	} else {
		feedErr = func() error {
			for {
				targets, err := st.AXFRTargetsAfter(ctx, cfg.scanID, cursor, batch)
				if err != nil {
					return err
				}
				if len(targets) == 0 {
					return nil
				}
				for _, t := range targets {
					select {
					case jobs <- t:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				cursor = targets[len(targets)-1].RootDomain
				if err := st.SetAXFRCursor(ctx, cfg.scanID, cursor); err != nil {
					return err
				}
			}
		}()
	}
	close(jobs)
	pool.Wait()
	close(stopStats)
	statsWG.Wait()
	if feedErr != nil {
		return feedErr
	}
	if err := st.MarkAXFRComplete(ctx, cfg.scanID); err != nil {
		return err
	}
	log.Printf("scan_id=%s: AXFR phase complete (%d domains, %d NS probed, %d open zones)",
		cfg.scanID, domainsProbed.Load(), nsProbed.Load(), nsOpen.Load())
	return nil
}

// processAXFRTarget probes each non-hyperscaler NS IP of one domain and records the per-NS outcomes
// plus any open zone's records. Returns (#NS probed, #NS that opened).
func processAXFRTarget(ctx context.Context, st *store.Store, prober *resolve.AXFRProber, cfg scanConfig, t store.AXFRTarget) (int, int) {
	var probes []model.AXFRProbe
	var zone []model.DNSRecord
	opened := 0
	for _, ip := range t.NSIPs {
		if scheduler.IsHyperscaler(ip) {
			continue // skip this NS — hyperscalers never allow AXFR
		}
		res := prober.ProbeServer(ctx, t.RootDomain, ip)
		probes = append(probes, model.AXFRProbe{Server: ip, Open: res.Open})
		if res.Open {
			opened++
			if zone == nil {
				zone = res.Zone // capture the transferred zone once per domain
			}
		}
	}
	if len(probes) == 0 {
		return 0, 0 // every NS was a hyperscaler — nothing probed
	}
	if err := st.RecordAXFR(ctx, cfg.scanID, t.RootDomain, probes, zone, cfg.runID, time.Now().UTC()); err != nil {
		log.Printf("scan_id=%s: AXFR record %s: %v", cfg.scanID, t.RootDomain, err)
	}
	return len(probes), opened
}

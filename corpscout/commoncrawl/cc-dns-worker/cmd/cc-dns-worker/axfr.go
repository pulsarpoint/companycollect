package main

import (
	"context"
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

// axfrCollector accumulates AXFR state-change rows from the worker pool (opens + closes only), flushed
// to ClickHouse's dns_axfr_observations log once the phase finishes.
type axfrCollector struct {
	mu      sync.Mutex
	changes []model.AXFRObservation
}

func (c *axfrCollector) add(o model.AXFRObservation) {
	c.mu.Lock()
	c.changes = append(c.changes, o)
	c.mu.Unlock()
}

// runAXFRPipeline runs the standalone AXFR phase: a bounded worker pool that walks the resolved-domain
// cursor (AXFRTargetsAfter — reusing the ns_ips resolution already stored, so no NS re-discovery) and,
// for each domain, probes every NON-hyperscaler NS IP. It loads the last known open/closed state per
// (domain, ns) from ClickHouse at the start, and emits a dns_axfr_observations row ONLY when the
// probed state differs from that (implicit-closed base: absent == closed) — so always-closed
// nameservers never produce a row and the log is already the collapsed open->close->open timeline.
// Open zones' records still flow to scan_records (source='axfr'). Entirely off the resolution path —
// its own pool, run as a phase after scanning. Resumable via axfr_state.
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

	// CH connection: load the previous per-NS state, then write the changes at the end.
	conn, err := chConn()
	if err != nil {
		return err
	}
	defer conn.Close()
	prev, err := load.LoadAXFRPrev(ctx, conn)
	if err != nil {
		return err
	}
	if err := st.ReplaceAXFRPrev(ctx, prev); err != nil { // mirror to SQLite for durability/inspection
		return err
	}
	log.Printf("scan_id=%s: AXFR phase START (workers=%d inflight=%d timeout=%s, %d known open-state NS)",
		cfg.scanID, workers, cfg.axfrInflight, cfg.axfrTimeout, len(prev))

	coll := &axfrCollector{}
	jobs := make(chan store.AXFRTarget, workers*2)
	var nsProbed, nsOpen, domainsProbed atomic.Int64
	var pool sync.WaitGroup
	for range workers {
		pool.Go(func() {
			for t := range jobs {
				probed, opened := processAXFRTarget(ctx, st, prober, cfg, t, prev, coll)
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
	// Flush the state changes to the ClickHouse observation log.
	if n, err := load.WriteAXFRObservations(ctx, conn, coll.changes); err != nil {
		return err
	} else if n > 0 {
		log.Printf("scan_id=%s: wrote %d AXFR state changes to %s", cfg.scanID, n, "dns_axfr_observations")
	}
	if err := st.MarkAXFRComplete(ctx, cfg.scanID); err != nil {
		return err
	}
	log.Printf("scan_id=%s: AXFR phase complete (%d domains, %d NS probed, %d open, %d state changes)",
		cfg.scanID, domainsProbed.Load(), nsProbed.Load(), nsOpen.Load(), len(coll.changes))
	return nil
}

// processAXFRTarget probes each non-hyperscaler NS IP of one domain, emits a state-change row when the
// probed open/closed value differs from prev (absent == closed), and appends any open zone's records
// to scan_records. Returns (#NS probed, #NS that opened). prev is read-only (built before the pool).
func processAXFRTarget(ctx context.Context, st *store.Store, prober *resolve.AXFRProber, cfg scanConfig, t store.AXFRTarget, prev map[[2]string]bool, coll *axfrCollector) (int, int) {
	var zone []model.DNSRecord
	probed, opened := 0, 0
	now := time.Now().UTC()
	for _, ip := range t.NSIPs {
		if scheduler.IsHyperscaler(ip) {
			continue // skip this NS — hyperscalers never allow AXFR
		}
		res := prober.ProbeServer(ctx, t.RootDomain, ip)
		probed++
		if res.Open {
			opened++
			if zone == nil {
				zone = res.Zone // capture the transferred zone once per domain
			}
		}
		// Emit only when the state changed vs the last known state (implicit-closed base).
		if res.Open != prev[[2]string{t.RootDomain, ip}] {
			coll.add(model.AXFRObservation{RootDomain: t.RootDomain, NameServer: ip, AXFROpen: res.Open, ObservedAt: now})
		}
	}
	if zone != nil {
		if err := st.RecordAXFRZone(ctx, cfg.scanID, t.RootDomain, zone, cfg.runID, now); err != nil {
			log.Printf("scan_id=%s: AXFR zone %s: %v", cfg.scanID, t.RootDomain, err)
		}
	}
	return probed, opened
}

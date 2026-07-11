package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"cc-dns-worker/internal/hostsource"
	"cc-dns-worker/internal/input"
	"cc-dns-worker/internal/load"
	"cc-dns-worker/internal/metrics"
	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
)

const idlePollInterval = 200 * time.Millisecond

// runBoundedCycle owns one complete keyset traversal. ClickHouse holds complete input, while SQLite
// never holds more active DNS or AXFR work than the configured capacities.
func runBoundedCycle(ctx context.Context, st *store.Store, cfg scanConfig) error {
	cfg = applyScanDefaults(cfg)
	startedAt := time.Now().UTC()
	if err := st.BeginCycle(ctx, cfg.scanID, startedAt); err != nil {
		return fmt.Errorf("begin cycle: %w", err)
	}
	if err := st.ResetRunning(ctx, cfg.scanID); err != nil {
		return fmt.Errorf("recover interrupted work: %w", err)
	}

	stats := &metrics.Stats{}
	discoveryScheduler := scheduler.New(scheduler.Config{
		PerServerQPS: cfg.discoveryQPS, Burst: max(1, int(cfg.discoveryQPS)),
		MaxInFlight: cfg.discoveryInflight, BreakerCooldown: cfg.breakerCooldown,
	})
	authoritativeScheduler := scheduler.New(scheduler.Config{
		PerServerQPS: cfg.qps, Burst: max(1, int(cfg.qps)), MaxInFlight: cfg.inflight,
		HyperscalerQPS: cfg.hyperscalerQPS, HyperscalerInFlight: max(cfg.inflight, 40),
		BreakerThreshold: cfg.breakerThreshold, BreakerCooldown: cfg.breakerCooldown,
	})
	discoverer := resolve.NewDiscoverer(
		resolve.NewExchangerWithStats(discoveryScheduler, cfg.timeout, stats), cfg.resolvers,
	)
	resolver := resolve.NewResolverWithStats(
		resolve.NewExchangerWithStats(authoritativeScheduler, cfg.timeout, stats), stats,
	)

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error { return domainSourceLoop(groupContext, st, cfg) })
	group.Go(func() error {
		return dnsWorkLoop(groupContext, st, cfg, discoverer, resolver, stats)
	})
	group.Go(func() error { return dnsFlushLoop(groupContext, st, cfg) })
	if cfg.axfr {
		group.Go(func() error { return boundedAXFRWorkLoop(groupContext, st, cfg, stats) })
		group.Go(func() error { return axfrFlushLoop(groupContext, st, cfg) })
	}
	if cfg.statsInterval > 0 {
		group.Go(func() error { return boundedStatsLoop(groupContext, st, cfg, stats, startedAt) })
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return st.CheckpointWAL(ctx)
}

func domainSourceLoop(ctx context.Context, st *store.Store, cfg scanConfig) error {
	conn, err := chConn()
	if err != nil {
		return fmt.Errorf("open domain source: %w", err)
	}
	defer conn.Close()
	for {
		state, err := st.SourceState(ctx, cfg.scanID)
		if err != nil {
			return fmt.Errorf("read source state: %w", err)
		}
		if state.SourceExhausted {
			return nil
		}
		active, err := st.DNSWorkCount(ctx, cfg.scanID)
		if err != nil {
			return fmt.Errorf("count DNS work: %w", err)
		}
		available := cfg.dnsCapacity - active
		if available <= 0 {
			if err := waitIdle(ctx); err != nil {
				return err
			}
			continue
		}
		pageSize := min(cfg.domainPageSize, available)
		if cfg.maxDomains > 0 {
			remaining := cfg.maxDomains - state.DomainsFetched
			if remaining <= 0 {
				_, err := st.AddDomainPage(ctx, cfg.scanID, nil, true, cfg.maxDomains)
				return err
			}
			pageSize = min(pageSize, remaining)
		}
		started := time.Now()
		page, err := input.FetchPage(ctx, conn, state.Cursor, pageSize)
		if err != nil {
			return fmt.Errorf("fetch domain page after %q: %w", state.Cursor, err)
		}
		exhausted := len(page) < pageSize
		added, err := st.AddDomainPage(ctx, cfg.scanID, page, exhausted, cfg.maxDomains)
		if err != nil {
			return fmt.Errorf("commit domain page: %w", err)
		}
		slog.Info("domain page committed", "scan_id", cfg.scanID, "rows", added,
			"cursor", lastString(page), "source_exhausted", exhausted, "latency", time.Since(started))
	}
}

func dnsWorkLoop(ctx context.Context, st *store.Store, cfg scanConfig, discoverer *resolve.Discoverer, resolver *resolve.Resolver, stats *metrics.Stats) error {
	var hostnameConn driver.Conn
	if cfg.hostEnrich {
		var err error
		hostnameConn, err = chConn()
		if err != nil {
			return fmt.Errorf("open hostname source: %w", err)
		}
		defer hostnameConn.Close()
	}
	for {
		claimLimit := cfg.dnsClaimBatch
		if cfg.axfr {
			activeAXFR, err := st.AXFRWorkCount(ctx, cfg.scanID)
			if err != nil {
				return fmt.Errorf("count AXFR work: %w", err)
			}
			if activeAXFR >= cfg.axfrCapacity {
				if err := waitIdle(ctx); err != nil {
					return err
				}
				continue
			}
			claimLimit = min(claimLimit, cfg.axfrCapacity-activeAXFR)
		}
		roots, err := st.ClaimDNS(ctx, cfg.scanID, claimLimit)
		if err != nil {
			return fmt.Errorf("claim DNS work: %w", err)
		}
		if len(roots) == 0 {
			done, err := dnsProductionDone(ctx, st, cfg.scanID)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			if err := waitIdle(ctx); err != nil {
				return err
			}
			continue
		}
		hosts := map[string][]model.HostLabel{}
		if cfg.hostEnrich {
			hosts, err = hostsource.Fetch(ctx, hostnameConn, roots, cfg.hostCap)
			if err != nil {
				_ = st.ReleaseDNS(context.WithoutCancel(ctx), cfg.scanID, roots)
				return fmt.Errorf("fetch hostname batch: %w", err)
			}
		}
		results := resolveDNSBatch(ctx, cfg, discoverer, resolver, stats, roots, hosts)
		for _, result := range results {
			if err := st.CommitDNS(ctx, result, cfg.axfr); err != nil {
				return fmt.Errorf("commit DNS result for %s: %w", result.RootDomain, err)
			}
		}
	}
}

func resolveDNSBatch(ctx context.Context, cfg scanConfig, discoverer *resolve.Discoverer, resolver *resolve.Resolver, stats *metrics.Stats, roots []string, hosts map[string][]model.HostLabel) []model.DomainResult {
	workers := min(cfg.workers, len(roots))
	jobs := make(chan string)
	results := make(chan model.DomainResult, len(roots))
	group, groupContext := errgroup.WithContext(ctx)
	for range workers {
		group.Go(func() error {
			for root := range jobs {
				result := resolveDomain(groupContext, discoverer, resolver, records.DefaultConfig(),
					root, cfg.scanID, cfg.runID, hosts[root])
				stats.Domains.Add(1)
				stats.Records.Add(int64(len(result.Records)))
				stats.DNSChecks.Add(int64(result.QueriesTotal))
				stats.DNSChecksOK.Add(int64(result.QueriesOK))
				if len(result.Records) == 0 {
					stats.DomainErrors.Add(1)
				}
				select {
				case results <- result:
				case <-groupContext.Done():
					return groupContext.Err()
				}
			}
			return nil
		})
	}
	group.Go(func() error {
		defer close(jobs)
		for _, root := range roots {
			select {
			case jobs <- root:
			case <-groupContext.Done():
				return groupContext.Err()
			}
		}
		return nil
	})
	go func() {
		_ = group.Wait()
		close(results)
	}()
	out := make([]model.DomainResult, 0, len(roots))
	for result := range results {
		out = append(out, result)
	}
	return out
}

func dnsFlushLoop(ctx context.Context, st *store.Store, cfg scanConfig) error {
	for {
		conn, err := chConn()
		if err == nil {
			loaded, flushErr := load.FlushDNS(ctx, conn, st, cfg.scanID, cfg.dnsFlushBatch)
			_ = conn.Close()
			if flushErr != nil {
				slog.Error("DNS flush failed", "scan_id", cfg.scanID, "error", flushErr)
			} else if loaded > 0 {
				slog.Info("DNS batch acknowledged", "scan_id", cfg.scanID, "domains", loaded)
				continue
			}
		} else {
			slog.Error("open DNS output connection", "scan_id", cfg.scanID, "error", err)
		}
		done, err := dnsDrainDone(ctx, st, cfg.scanID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := waitInterval(ctx, cfg.dnsFlushInterval); err != nil {
			return err
		}
	}
}

func boundedAXFRWorkLoop(ctx context.Context, st *store.Store, cfg scanConfig, stats *metrics.Stats) error {
	axfrScheduler := scheduler.New(scheduler.Config{
		PerServerQPS: cfg.axfrQPS, Burst: max(1, int(cfg.axfrQPS)), MaxInFlight: 1,
	})
	prober := resolve.NewAXFRProber(axfrScheduler, resolve.AXFRCaps{
		MaxRecords: cfg.axfrMaxRecords, MaxBytes: cfg.axfrMaxBytes, Deadline: cfg.axfrTimeout,
	}, cfg.axfrInflight)
	for {
		targets, err := st.ClaimAXFR(ctx, cfg.scanID, cfg.axfrClaimBatch)
		if err != nil {
			return fmt.Errorf("claim AXFR work: %w", err)
		}
		if len(targets) == 0 {
			done, err := axfrProductionDone(ctx, st, cfg.scanID)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			if err := waitIdle(ctx); err != nil {
				return err
			}
			continue
		}
		for _, result := range probeAXFRBatch(ctx, prober, stats, targets, cfg.axfrWorkers) {
			if err := st.CommitAXFR(ctx, cfg.scanID, result.domain, result.probes, result.zone, ""); err != nil {
				return fmt.Errorf("commit AXFR result for %s: %w", result.domain, err)
			}
		}
	}
}

func boundedStatsLoop(ctx context.Context, st *store.Store, cfg scanConfig, resolverStats *metrics.Stats, startedAt time.Time) error {
	previous := resolverStats.Snapshot(startedAt)
	recentErrors := metrics.NewErrorWindow(10 * time.Minute)
	for {
		if err := waitInterval(ctx, cfg.statsInterval); err != nil {
			return err
		}
		now := time.Now().UTC()
		current := resolverStats.Snapshot(now)
		recentErrors.Add(now, current.Domains-previous.Domains, current.DomainErrors-previous.DomainErrors)
		slog.Info(metrics.Line(current, startedAt, recentErrors.Percent()), "scan_id", cfg.scanID)
		previous = current
		stats, err := st.OperationalStats(ctx, cfg.scanID)
		if err != nil {
			return fmt.Errorf("read operational stats: %w", err)
		}
		slog.Debug("bounded SQLite status",
			"scan_id", cfg.scanID, "domain_cursor", stats.Source.Cursor,
			"input_domains_fetched", stats.Source.DomainsFetched,
			"source_exhausted", stats.Source.SourceExhausted,
			"dns_queue_pending", stats.DNS.Pending, "dns_queue_claimed", stats.DNS.Running,
			"dns_queue_ready", stats.DNS.Ready, "dns_outbox_records", stats.DNSRecords,
			"axfr_queue_pending", stats.AXFR.Pending, "axfr_queue_claimed", stats.AXFR.Running,
			"axfr_queue_ready", stats.AXFR.Ready, "axfr_outbox_probes", stats.AXFRProbes,
			"axfr_outbox_zone_records", stats.AXFRZoneRecords, "sqlite_pages", stats.PageCount,
			"sqlite_free_pages", stats.FreePages, "sqlite_wal_bytes", stats.WALBytes,
		)
		var done bool
		if cfg.axfr {
			done, err = axfrDrainDone(ctx, st, cfg.scanID)
		} else {
			done, err = dnsDrainDone(ctx, st, cfg.scanID)
		}
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func probeAXFRBatch(ctx context.Context, prober axfrProber, stats *metrics.Stats, targets []store.BoundedAXFRTarget, workerLimit int) []axfrDomainResult {
	workers := min(max(workerLimit, 1), len(targets))
	work := make(chan store.BoundedAXFRTarget)
	results := make(chan axfrDomainResult, len(targets))
	var group errgroup.Group
	for range workers {
		group.Go(func() error {
			for target := range work {
				results <- processAXFRTarget(ctx, prober, store.AXFRTarget{
					RootDomain: target.RootDomain, Endpoints: target.Endpoints,
				}, stats)
			}
			return nil
		})
	}
	go func() {
		for _, target := range targets {
			work <- target
		}
		close(work)
		_ = group.Wait()
		close(results)
	}()
	out := make([]axfrDomainResult, 0, len(targets))
	for result := range results {
		out = append(out, result)
	}
	return out
}

func axfrFlushLoop(ctx context.Context, st *store.Store, cfg scanConfig) error {
	for {
		conn, err := chConn()
		if err == nil {
			loaded, flushErr := load.FlushAXFR(ctx, conn, st, cfg.scanID, cfg.axfrFlushBatch)
			_ = conn.Close()
			if flushErr != nil {
				slog.Error("AXFR flush failed", "scan_id", cfg.scanID, "error", flushErr)
			} else if loaded > 0 {
				slog.Info("AXFR batch acknowledged", "scan_id", cfg.scanID, "domains", loaded)
				continue
			}
		} else {
			slog.Error("open AXFR output connection", "scan_id", cfg.scanID, "error", err)
		}
		done, err := axfrDrainDone(ctx, st, cfg.scanID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := waitInterval(ctx, cfg.axfrFlushInterval); err != nil {
			return err
		}
	}
}

func dnsProductionDone(ctx context.Context, st *store.Store, scanID string) (bool, error) {
	state, err := st.SourceState(ctx, scanID)
	if err != nil {
		return false, err
	}
	counts, err := st.DNSWorkCounts(ctx, scanID)
	return state.SourceExhausted && counts.Pending == 0 && counts.Running == 0, err
}

func dnsDrainDone(ctx context.Context, st *store.Store, scanID string) (bool, error) {
	state, err := st.SourceState(ctx, scanID)
	if err != nil {
		return false, err
	}
	count, err := st.DNSWorkCount(ctx, scanID)
	return state.SourceExhausted && count == 0, err
}

func axfrProductionDone(ctx context.Context, st *store.Store, scanID string) (bool, error) {
	dnsDone, err := dnsProductionDone(ctx, st, scanID)
	if err != nil || !dnsDone {
		return false, err
	}
	counts, err := st.AXFRWorkCounts(ctx, scanID)
	return counts.Pending == 0 && counts.Running == 0, err
}

func axfrDrainDone(ctx context.Context, st *store.Store, scanID string) (bool, error) {
	dnsDone, err := dnsDrainDone(ctx, st, scanID)
	if err != nil || !dnsDone {
		return false, err
	}
	counts, err := st.AXFRWorkCounts(ctx, scanID)
	return counts.Pending == 0 && counts.Running == 0 && counts.Ready == 0, err
}

func waitIdle(ctx context.Context) error { return waitInterval(ctx, idlePollInterval) }

func waitInterval(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func lastString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[len(values)-1]
}

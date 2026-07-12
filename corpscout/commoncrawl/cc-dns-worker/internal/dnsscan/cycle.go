package dnsscan

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"cc-dns-worker/internal/clickhouseconn"
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

const (
	idlePollInterval        = 200 * time.Millisecond
	hostnameEnrichmentBatch = 100
)

// RunCycle completes or resumes one bounded DNS traversal using dbPath as its private durable state.
func RunCycle(ctx context.Context, dbPath string, config Config) error {
	config = config.withDefaults()
	localStore, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("open DNS cycle store: %w", err)
	}
	defer localStore.Close()

	if err := localStore.BeginCycle(ctx, config.ScanID, time.Now().UTC()); err != nil {
		return fmt.Errorf("begin DNS cycle: %w", err)
	}
	if err := localStore.ResetRunning(ctx, config.ScanID); err != nil {
		return fmt.Errorf("recover interrupted DNS work: %w", err)
	}

	stats := &metrics.Stats{}
	discoveryScheduler := scheduler.New(scheduler.Config{
		PerServerQPS: config.DiscoveryQPS, Burst: max(1, int(config.DiscoveryQPS)),
		MaxInFlight: config.DiscoveryInflight, BreakerCooldown: config.BreakerCooldown,
	})
	authoritativeScheduler := scheduler.New(scheduler.Config{
		PerServerQPS: config.PerServerQPS, Burst: max(1, int(config.PerServerQPS)),
		MaxInFlight: config.PerServerInflight, HyperscalerQPS: config.HyperscalerQPS,
		HyperscalerInFlight: max(config.PerServerInflight, 40),
		BreakerThreshold:    config.BreakerThreshold, BreakerCooldown: config.BreakerCooldown,
	})
	discoverer := resolve.NewDiscoverer(
		resolve.NewExchangerWithStats(discoveryScheduler, config.QueryTimeout, stats), config.Resolvers,
	)
	resolver := resolve.NewResolverWithStats(
		resolve.NewExchangerWithStats(authoritativeScheduler, config.QueryTimeout, stats), stats,
	)

	group, groupContext := errgroup.WithContext(ctx)
	group.Go(func() error { return sourceLoop(groupContext, localStore, config) })
	group.Go(func() error { return workLoop(groupContext, localStore, config, discoverer, resolver, stats) })
	group.Go(func() error { return flushLoop(groupContext, localStore, config) })
	group.Go(func() error {
		return statsLoop(groupContext, localStore, config, stats, config.StatsInterval > 0)
	})
	if err := group.Wait(); err != nil {
		return err
	}
	return localStore.CheckpointWAL(ctx)
}

func sourceLoop(ctx context.Context, localStore *store.Store, config Config) error {
	connection, err := clickhouseconn.Open()
	if err != nil {
		return fmt.Errorf("open DNS domain source: %w", err)
	}
	defer connection.Close()
	for {
		state, err := localStore.SourceState(ctx, config.ScanID)
		if err != nil {
			return fmt.Errorf("read DNS source state: %w", err)
		}
		if state.SourceExhausted {
			return nil
		}
		active, err := localStore.DNSWorkCount(ctx, config.ScanID)
		if err != nil {
			return fmt.Errorf("count DNS work: %w", err)
		}
		remainingLimit := -1
		if config.MaxDomains > 0 {
			remainingLimit = config.MaxDomains - state.DomainsFetched
			if remainingLimit <= 0 {
				_, err := localStore.AddDomainPage(ctx, config.ScanID, nil, true, config.MaxDomains)
				return err
			}
		}
		pageSize := sourcePageSize(config, active, remainingLimit)
		if pageSize == 0 {
			if err := wait(ctx, idlePollInterval); err != nil {
				return err
			}
			continue
		}
		page, err := input.FetchPage(ctx, connection, state.Cursor, pageSize)
		if err != nil {
			return fmt.Errorf("fetch DNS domain page after %q: %w", state.Cursor, err)
		}
		exhausted := len(page) < pageSize
		if _, err := localStore.AddDomainPage(ctx, config.ScanID, page, exhausted, config.MaxDomains); err != nil {
			return fmt.Errorf("commit DNS domain page: %w", err)
		}
	}
}

func sourcePageSize(config Config, active, remainingLimit int) int {
	pageSize := min(config.DomainPageSize, config.WorkCapacity)
	if remainingLimit >= 0 {
		pageSize = min(pageSize, remainingLimit)
	}
	if pageSize <= 0 || config.WorkCapacity-active < pageSize {
		return 0
	}
	return pageSize
}

func workLoop(ctx context.Context, localStore *store.Store, config Config, discoverer *resolve.Discoverer, resolver *resolve.Resolver, stats *metrics.Stats) error {
	var hostnameConnection driver.Conn
	if config.HostnameEnrichment {
		var err error
		hostnameConnection, err = clickhouseconn.Open()
		if err != nil {
			return fmt.Errorf("open DNS hostname source: %w", err)
		}
		defer hostnameConnection.Close()
	}
	type task struct {
		rootDomain string
		hostnames  []model.HostLabel
	}
	poolContext, stopPool := context.WithCancel(ctx)
	defer stopPool()
	jobs := make(chan task, config.ClaimBatch)
	results := make(chan model.DomainResult, config.ClaimBatch)
	workerGroup, workerContext := errgroup.WithContext(poolContext)
	for range config.Workers {
		workerGroup.Go(func() error {
			for job := range jobs {
				result := resolveDomain(workerContext, discoverer, resolver, job.rootDomain,
					config.ScanID, config.RunID, job.hostnames)
				select {
				case results <- result:
				case <-workerContext.Done():
					return workerContext.Err()
				}
			}
			return nil
		})
	}
	workersDone := make(chan error, 1)
	go func() { workersDone <- workerGroup.Wait() }()

	inFlight := 0
	refillAt := max(1, config.ClaimBatch/2)
	for {
		if inFlight <= refillAt {
			claimLimit := config.ClaimBatch - inFlight
			if config.HostnameEnrichment {
				claimLimit = min(claimLimit, hostnameEnrichmentBatch)
			}
			roots, err := localStore.ClaimDNS(ctx, config.ScanID, claimLimit)
			if err != nil {
				close(jobs)
				return fmt.Errorf("claim DNS work: %w", err)
			}
			hostnames := map[string][]model.HostLabel{}
			if config.HostnameEnrichment && len(roots) > 0 {
				hostnames, err = hostsource.Fetch(ctx, hostnameConnection, roots, config.HostnameCap)
				if err != nil {
					_ = localStore.ReleaseDNS(context.WithoutCancel(ctx), config.ScanID, roots)
					close(jobs)
					return fmt.Errorf("fetch DNS hostname batch: %w", err)
				}
			}
			for _, rootDomain := range roots {
				select {
				case jobs <- task{rootDomain: rootDomain, hostnames: hostnames[rootDomain]}:
					inFlight++
				case <-ctx.Done():
					close(jobs)
					return ctx.Err()
				}
			}
			if len(roots) == 0 && inFlight == 0 {
				done, err := productionDone(ctx, localStore, config.ScanID)
				if err != nil {
					close(jobs)
					return err
				}
				if done {
					close(jobs)
					return <-workersDone
				}
				if err := wait(ctx, idlePollInterval); err != nil {
					close(jobs)
					return err
				}
				continue
			}
		}

		select {
		case result := <-results:
			if err := localStore.CommitDNS(ctx, result); err != nil {
				close(jobs)
				return fmt.Errorf("commit DNS result for %s: %w", result.RootDomain, err)
			}
			inFlight--
		case err := <-workersDone:
			close(jobs)
			if err == nil && inFlight > 0 {
				return fmt.Errorf("DNS resolver pool stopped with %d domains in flight", inFlight)
			}
			return err
		case <-ctx.Done():
			close(jobs)
			return ctx.Err()
		}
	}
}

func resolveDomain(ctx context.Context, discoverer *resolve.Discoverer, resolver *resolve.Resolver, domain, scanID, runID string, extra []model.HostLabel) model.DomainResult {
	now := time.Now().UTC()
	config := records.DefaultConfig()
	delegation, err := discoverer.DiscoverNS(ctx, domain)
	if err != nil || len(delegation.NSIPs) == 0 {
		message := "no authoritative NS IPs"
		if err != nil {
			message = err.Error()
		}
		status := model.DomainStatusError
		if errors.Is(err, resolve.ErrNoPublicNSEndpoints) {
			status = model.DomainStatusNoPublicNSEndpoints
		}
		return model.DomainResult{
			ScanID: scanID, RootDomain: domain, ETLD: delegation.ETLD,
			Nameservers: delegation.NS, NSIPs: delegation.NSIPs, Endpoints: delegation.Endpoints,
			DSPresent: delegation.DSOutcome == resolve.OutcomePresent, DSOutcome: delegation.DSOutcome,
			Status: status, Error: message, QueriesTotal: len(records.Plan(domain, config, extra)),
			SourceRunID: runID, ResolvedAt: now,
		}
	}
	return resolver.Resolve(ctx, domain, scanID, runID, delegation, config, now, extra)
}

func flushLoop(ctx context.Context, localStore *store.Store, config Config) error {
	for {
		connection, err := clickhouseconn.Open()
		if err == nil {
			loaded, flushErr := load.FlushDNS(ctx, connection, localStore, config.ScanID, config.FlushBatch)
			_ = connection.Close()
			if flushErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Error("DNS flush failed", "scan_id", config.ScanID, "error", flushErr)
			} else if loaded > 0 {
				continue
			}
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("open DNS output connection", "scan_id", config.ScanID, "error", err)
		}
		done, err := drainDone(ctx, localStore, config.ScanID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := wait(ctx, config.FlushInterval); err != nil {
			return err
		}
	}
}

func statsLoop(ctx context.Context, localStore *store.Store, config Config, stats *metrics.Stats, emit bool) error {
	previous := stats.Snapshot(time.Now().UTC())
	interval := config.StatsInterval
	if interval <= 0 {
		interval = time.Second
	}
	for {
		if err := wait(ctx, interval); err != nil {
			return err
		}
		now := time.Now().UTC()
		current := stats.Snapshot(now)
		if emit {
			slog.Info(metrics.Line(current, previous),
				"component", "dns", "scan_id", config.ScanID)
		}
		previous = current
		done, err := drainDone(ctx, localStore, config.ScanID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func productionDone(ctx context.Context, localStore *store.Store, scanID string) (bool, error) {
	state, err := localStore.SourceState(ctx, scanID)
	if err != nil {
		return false, err
	}
	counts, err := localStore.DNSWorkCounts(ctx, scanID)
	return state.SourceExhausted && counts.Pending == 0 && counts.Running == 0, err
}

func drainDone(ctx context.Context, localStore *store.Store, scanID string) (bool, error) {
	state, err := localStore.SourceState(ctx, scanID)
	if err != nil {
		return false, err
	}
	count, err := localStore.DNSWorkCount(ctx, scanID)
	return state.SourceExhausted && count == 0, err
}

func wait(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

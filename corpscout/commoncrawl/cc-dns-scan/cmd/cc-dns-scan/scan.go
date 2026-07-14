package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"cc-dns-scan/internal/dnsscan"
)

func dnsScannerFlags(flags *flag.FlagSet) func() (dnsscan.Config, error) {
	maxDomains := flags.Int("max-domains", 0, "cap domains fetched by each DNS cycle (0 = all)")
	statsInterval := flags.Duration("stats-interval", 5*time.Second, "periodic metrics interval (0 = off)")
	resolvers := flags.String("resolvers", "", "REQUIRED: comma-separated recursive resolvers")
	discoveryQPS := flags.Float64("discovery-qps", 50, "DNS discovery queries/sec per recursive resolver")
	discoveryInflight := flags.Int("discovery-inflight", 500, "DNS discovery queries in flight per resolver")
	perServerQPS := flags.Float64("per-server-qps", 10, "DNS queries/sec per authoritative NS IP")
	perServerInflight := flags.Int("per-server-inflight", 3, "DNS queries in flight per authoritative NS IP")
	hyperscalerQPS := flags.Float64("hyperscaler-qps", 200, "DNS QPS for known large providers")
	workers := flags.Int("workers", 4000, "domains resolved concurrently by DNS")
	queryTimeout := flags.Duration("query-timeout", 5*time.Second, "DNS per-query timeout")
	breakerThreshold := flags.Int("breaker-threshold", 5, "DNS transport failures before a circuit opens")
	breakerCooldown := flags.Duration("breaker-cooldown", 30*time.Second, "DNS circuit-breaker cooldown")
	hostEnrich := flags.Bool("host-enrich", true, "read ranked labels from the confirmed hostname view")
	hostCap := flags.Int("host-cap", 100, "maximum confirmed hostname labels queried per domain")
	domainPageSize := flags.Int("domain-page-size", 5000, "DNS root domains per ClickHouse page")
	dnsCapacity := flags.Int("dns-work-capacity", 20000, "active DNS domains retained in DNS SQLite")
	dnsClaimBatch := flags.Int("dns-claim-batch", 2000, "DNS domains claimed per batch")
	dnsFlushBatch := flags.Int("dns-flush-batch", 500, "ready DNS domains flushed per batch")
	dnsFlushInterval := flags.Duration("dns-flush-interval", 5*time.Second, "DNS output retry interval")

	return func() (dnsscan.Config, error) {
		resolverList := cleanResolvers(strings.Split(*resolvers, ","))
		if len(resolverList) == 0 {
			return dnsscan.Config{}, fmt.Errorf("--resolvers is required, for example 127.0.0.1:53")
		}
		return dnsscan.Config{
			MaxDomains: *maxDomains, Resolvers: resolverList,
			DiscoveryQPS: *discoveryQPS, DiscoveryInflight: *discoveryInflight,
			PerServerQPS: *perServerQPS, PerServerInflight: *perServerInflight,
			HyperscalerQPS: *hyperscalerQPS, Workers: *workers, QueryTimeout: *queryTimeout,
			BreakerThreshold: *breakerThreshold, BreakerCooldown: *breakerCooldown,
			StatsInterval: *statsInterval, HostnameEnrichment: *hostEnrich, HostnameCap: *hostCap,
			DomainPageSize: *domainPageSize, WorkCapacity: *dnsCapacity,
			ClaimBatch: *dnsClaimBatch, FlushBatch: *dnsFlushBatch, FlushInterval: *dnsFlushInterval,
		}, nil
	}
}

func runScan(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("scan", flag.ContinueOnError)
	scanID := flags.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan cycle id")
	runID := flags.String("run-id", "", "DNS source run id (defaults to scan-id)")
	dnsDB := flags.String("dns-db", "dns-scan.db", "DNS SQLite work/outbox path")
	build := dnsScannerFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	config, err := build()
	if err != nil {
		return err
	}
	config.ScanID, config.RunID = *scanID, *runID
	err = dnsscan.RunCycle(ctx, *dnsDB, config)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func cleanResolvers(raw []string) []string {
	cleaned := make([]string, 0, len(raw))
	for _, resolver := range raw {
		if value := strings.TrimSpace(resolver); value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}

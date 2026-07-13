package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/axfrscan"
	"cc-dns-worker/internal/dnsscan"
)

type scannerConfig struct {
	DNS     dnsscan.Config
	AXFR    axfrscan.Config
	RunDNS  bool
	RunAXFR bool
}

func scannerFlags(flags *flag.FlagSet) func() (scannerConfig, error) {
	maxDomains := flags.Int("max-domains", 0, "cap domains fetched by each scanner cycle (0 = all)")
	statsInterval := flags.Duration("stats-interval", 5*time.Second, "periodic metrics interval (0 = off)")
	runDNS := flags.Bool("dns", true, "run the independent DNS scanner")
	runAXFR := flags.Bool("axfr", true, "run the independent AXFR scanner")

	resolvers := flags.String("resolvers", "", "REQUIRED when DNS is enabled: comma-separated recursive resolvers")
	discoveryQPS := flags.Float64("discovery-qps", 50, "DNS discovery queries/sec per recursive resolver")
	discoveryInflight := flags.Int("discovery-inflight", 500, "DNS discovery queries in flight per resolver")
	perServerQPS := flags.Float64("per-server-qps", 10, "DNS queries/sec per authoritative NS IP")
	perServerInflight := flags.Int("per-server-inflight", 3, "DNS queries in flight per authoritative NS IP")
	hyperscalerQPS := flags.Float64("hyperscaler-qps", 200, "DNS QPS for known large providers")
	workers := flags.Int("workers", 4000, "domains resolved concurrently by DNS")
	queryTimeout := flags.Duration("query-timeout", 5*time.Second, "DNS per-query timeout")
	breakerThreshold := flags.Int("breaker-threshold", 5, "DNS transport failures before a circuit opens")
	breakerCooldown := flags.Duration("breaker-cooldown", 30*time.Second, "DNS circuit-breaker cooldown")
	hostEnrich := flags.Bool("host-enrich", true, "read ranked labels from the hostname registry")
	hostCap := flags.Int("host-cap", 100, "maximum registry labels queried per domain")
	domainPageSize := flags.Int("domain-page-size", 5000, "DNS root domains per ClickHouse page")
	dnsCapacity := flags.Int("dns-work-capacity", 20000, "active DNS domains retained in DNS SQLite")
	dnsClaimBatch := flags.Int("dns-claim-batch", 2000, "DNS domains claimed per batch")
	dnsFlushBatch := flags.Int("dns-flush-batch", 500, "ready DNS domains flushed per batch")
	dnsFlushInterval := flags.Duration("dns-flush-interval", 5*time.Second, "DNS output retry interval")

	axfrWorkers := flags.Int("axfr-workers", 50, "AXFR endpoints probed concurrently")
	axfrQPS := flags.Float64("axfr-qps", 5, "AXFR transfers/sec per NS IP")
	axfrMaxRecords := flags.Int("axfr-max-records", 50000, "records retained per AXFR transfer")
	axfrMaxBytes := flags.Int("axfr-max-bytes", 67108864, "bytes retained per AXFR transfer")
	axfrTimeout := flags.Duration("axfr-timeout", 20*time.Second, "whole-transfer AXFR timeout")
	axfrPageSize := flags.Int("axfr-domain-page-size", 1000, "AXFR root domains per ClickHouse page")
	axfrCapacity := flags.Int("axfr-work-capacity", 5000, "active AXFR domains retained in AXFR SQLite")
	axfrClaimBatch := flags.Int("axfr-claim-batch", 100, "AXFR endpoints claimed per batch")
	axfrFlushBatch := flags.Int("axfr-flush-batch", 100, "ready AXFR domains flushed per batch")
	axfrFlushInterval := flags.Duration("axfr-flush-interval", 5*time.Second, "AXFR output retry interval")

	return func() (scannerConfig, error) {
		resolverList := cleanResolvers(strings.Split(*resolvers, ","))
		if *runDNS && len(resolverList) == 0 {
			return scannerConfig{}, fmt.Errorf("--resolvers is required when --dns=true, for example 127.0.0.1:53")
		}
		if !*runDNS && !*runAXFR {
			return scannerConfig{}, fmt.Errorf("at least one of --dns or --axfr must be enabled")
		}
		return scannerConfig{
			RunDNS: *runDNS, RunAXFR: *runAXFR,
			DNS: dnsscan.Config{
				MaxDomains: *maxDomains, Resolvers: resolverList,
				DiscoveryQPS: *discoveryQPS, DiscoveryInflight: *discoveryInflight,
				PerServerQPS: *perServerQPS, PerServerInflight: *perServerInflight,
				HyperscalerQPS: *hyperscalerQPS, Workers: *workers, QueryTimeout: *queryTimeout,
				BreakerThreshold: *breakerThreshold, BreakerCooldown: *breakerCooldown,
				StatsInterval: *statsInterval, HostnameEnrichment: *hostEnrich, HostnameCap: *hostCap,
				DomainPageSize: *domainPageSize, WorkCapacity: *dnsCapacity,
				ClaimBatch: *dnsClaimBatch, FlushBatch: *dnsFlushBatch, FlushInterval: *dnsFlushInterval,
			},
			AXFR: axfrscan.Config{
				MaxDomains: *maxDomains, Workers: *axfrWorkers, PerServerQPS: *axfrQPS,
				MaxRecords: *axfrMaxRecords, MaxBytes: *axfrMaxBytes,
				Timeout: *axfrTimeout, StatsInterval: *statsInterval, DomainPageSize: *axfrPageSize,
				WorkCapacity: *axfrCapacity, ClaimBatch: *axfrClaimBatch,
				FlushBatch: *axfrFlushBatch, FlushInterval: *axfrFlushInterval,
			},
		}, nil
	}
}

func runScan(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("scan", flag.ContinueOnError)
	scanID := flags.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan cycle id")
	runID := flags.String("run-id", "", "DNS source run id (defaults to scan-id)")
	dnsDB := flags.String("dns-db", "dns-scan.db", "DNS SQLite work/outbox path")
	axfrDB := flags.String("axfr-db", "axfr-scan.db", "AXFR SQLite work/outbox path")
	build := scannerFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	config, err := build()
	if err != nil {
		return err
	}
	config.DNS.ScanID, config.DNS.RunID = *scanID, *runID
	config.AXFR.ScanID = *scanID

	var waitGroup sync.WaitGroup
	errorsByScanner := make(chan error, 2)
	if config.RunDNS {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errorsByScanner <- dnsscan.RunCycle(ctx, *dnsDB, config.DNS)
		}()
	}
	if config.RunAXFR {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errorsByScanner <- axfrscan.RunCycle(ctx, *axfrDB, config.AXFR)
		}()
	}
	waitGroup.Wait()
	close(errorsByScanner)
	var scannerErrors []error
	for scannerError := range errorsByScanner {
		if scannerError != nil && !errors.Is(scannerError, context.Canceled) {
			scannerErrors = append(scannerErrors, scannerError)
		}
	}
	return errors.Join(scannerErrors...)
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

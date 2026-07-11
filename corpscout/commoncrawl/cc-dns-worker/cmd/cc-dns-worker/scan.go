package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"
)

type scanConfig struct {
	scanID, runID     string
	maxDomains        int
	resolvers         []string
	discoveryQPS      float64
	discoveryInflight int
	qps               float64
	inflight          int
	hyperscalerQPS    float64
	workers           int
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
	domainPageSize    int
	dnsCapacity       int
	dnsClaimBatch     int
	dnsFlushBatch     int
	dnsFlushInterval  time.Duration
	axfrCapacity      int
	axfrClaimBatch    int
	axfrFlushBatch    int
	axfrFlushInterval time.Duration
}

func scanFlags(fs *flag.FlagSet) func() (scanConfig, error) {
	maxDomains := fs.Int("max-domains", 0, "cap domains durably fetched in this cycle (0 = all)")
	resolvers := fs.String("resolvers", "", "REQUIRED: comma-separated recursive resolvers for NS discovery")
	discoveryQPS := fs.Float64("discovery-qps", 50, "max queries/sec per recursive resolver")
	discoveryInflight := fs.Int("discovery-inflight", 500, "max in-flight queries per recursive resolver")
	qps := fs.Float64("per-server-qps", 10, "max queries/sec per authoritative NS IP")
	inflight := fs.Int("per-server-inflight", 3, "max in-flight queries per authoritative NS IP")
	hyperscalerQPS := fs.Float64("hyperscaler-qps", 200, "elevated QPS for known large DNS providers")
	workers := fs.Int("workers", 4000, "max domains resolved concurrently")
	timeout := fs.Duration("query-timeout", 5*time.Second, "per-query timeout")
	breakerThreshold := fs.Int("breaker-threshold", 5, "transport failures before an authoritative IP circuit opens")
	breakerCooldown := fs.Duration("breaker-cooldown", 30*time.Second, "authoritative IP circuit cooldown")
	statsInterval := fs.Duration("stats-interval", time.Second, "periodic metrics interval (0 = off)")
	axfr := fs.Bool("axfr", true, "run the concurrent AXFR lane")
	axfrWorkers := fs.Int("axfr-workers", 50, "max concurrent AXFR domain probers")
	axfrQPS := fs.Float64("axfr-qps", 5, "max AXFR transfers/sec per NS IP")
	axfrInflight := fs.Int("axfr-inflight", 50, "max total in-flight AXFR transfers")
	axfrMaxRecords := fs.Int("axfr-max-records", 50000, "stop collecting a zone past this record count")
	axfrMaxBytes := fs.Int("axfr-max-bytes", 67108864, "stop collecting a zone past this byte count")
	axfrTimeout := fs.Duration("axfr-timeout", 20*time.Second, "whole-transfer timeout per AXFR")
	hostEnrich := fs.Bool("host-enrich", true, "read ranked labels from the hostname registry")
	hostCap := fs.Int("host-cap", 100, "max registry labels queried per domain")
	domainPageSize := fs.Int("domain-page-size", 5000, "root domains fetched per ClickHouse keyset page")
	dnsCapacity := fs.Int("dns-work-capacity", 20000, "maximum active DNS jobs retained in SQLite")
	dnsClaimBatch := fs.Int("dns-claim-batch", 2000, "DNS jobs claimed with one hostname query")
	dnsFlushBatch := fs.Int("dns-flush-batch", 500, "ready DNS jobs acknowledged per flush")
	dnsFlushInterval := fs.Duration("dns-flush-interval", 5*time.Second, "DNS output retry interval")
	axfrCapacity := fs.Int("axfr-work-capacity", 5000, "maximum active AXFR jobs retained in SQLite")
	axfrClaimBatch := fs.Int("axfr-claim-batch", 100, "AXFR jobs claimed per batch")
	axfrFlushBatch := fs.Int("axfr-flush-batch", 100, "ready AXFR jobs acknowledged per flush")
	axfrFlushInterval := fs.Duration("axfr-flush-interval", 5*time.Second, "AXFR output retry interval")
	return func() (scanConfig, error) {
		resolverList := cleanResolvers(strings.Split(*resolvers, ","))
		if len(resolverList) == 0 {
			return scanConfig{}, fmt.Errorf("--resolvers is required, for example 127.0.0.1:53")
		}
		return scanConfig{
			maxDomains: *maxDomains, resolvers: resolverList,
			discoveryQPS: *discoveryQPS, discoveryInflight: *discoveryInflight,
			qps: *qps, inflight: *inflight, hyperscalerQPS: *hyperscalerQPS,
			workers: *workers, timeout: *timeout, breakerThreshold: *breakerThreshold,
			breakerCooldown: *breakerCooldown, statsInterval: *statsInterval,
			axfr: *axfr, axfrWorkers: *axfrWorkers, axfrQPS: *axfrQPS,
			axfrInflight: *axfrInflight, axfrMaxRecords: *axfrMaxRecords,
			axfrMaxBytes: *axfrMaxBytes, axfrTimeout: *axfrTimeout,
			hostEnrich: *hostEnrich, hostCap: *hostCap,
			domainPageSize: *domainPageSize, dnsCapacity: *dnsCapacity,
			dnsClaimBatch: *dnsClaimBatch, dnsFlushBatch: *dnsFlushBatch,
			dnsFlushInterval: *dnsFlushInterval, axfrCapacity: *axfrCapacity,
			axfrClaimBatch: *axfrClaimBatch, axfrFlushBatch: *axfrFlushBatch,
			axfrFlushInterval: *axfrFlushInterval,
		}, nil
	}
}

func runScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	scanID := fs.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan cycle id")
	runID := fs.String("run-id", "", "source run id (defaults to scan-id)")
	dbPath := fs.String("db", "scan.db", "SQLite work/outbox path")
	build := scanFlags(fs)
	_ = fs.Parse(args)
	cfg, err := build()
	if err != nil {
		return err
	}
	cfg.scanID, cfg.runID = *scanID, *runID
	if cfg.runID == "" {
		cfg.runID = cfg.scanID
	}
	localStore, err := store.Open(*dbPath)
	if err != nil {
		return err
	}
	defer localStore.Close()
	return runBoundedCycle(context.Background(), localStore, cfg)
}

func applyScanDefaults(cfg scanConfig) scanConfig {
	if cfg.workers <= 0 {
		cfg.workers = 1
	}
	if cfg.discoveryInflight <= 0 {
		cfg.discoveryInflight = 500
	}
	if cfg.domainPageSize <= 0 {
		cfg.domainPageSize = 5000
	}
	if cfg.dnsCapacity <= 0 {
		cfg.dnsCapacity = 20000
	}
	if cfg.dnsClaimBatch <= 0 {
		cfg.dnsClaimBatch = 2000
	}
	if cfg.dnsFlushBatch <= 0 {
		cfg.dnsFlushBatch = 500
	}
	if cfg.dnsFlushInterval <= 0 {
		cfg.dnsFlushInterval = 5 * time.Second
	}
	if cfg.axfrCapacity <= 0 {
		cfg.axfrCapacity = 5000
	}
	if cfg.axfrClaimBatch <= 0 {
		cfg.axfrClaimBatch = 100
	}
	if cfg.axfrFlushBatch <= 0 {
		cfg.axfrFlushBatch = 100
	}
	if cfg.axfrFlushInterval <= 0 {
		cfg.axfrFlushInterval = 5 * time.Second
	}
	return cfg
}

func resolveDomain(ctx context.Context, discoverer *resolve.Discoverer, resolver *resolve.Resolver, config records.Config, domain, scanID, runID string, extra []model.HostLabel) model.DomainResult {
	now := time.Now().UTC()
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

func cleanResolvers(raw []string) []string {
	cleaned := make([]string, 0, len(raw))
	for _, resolver := range raw {
		if value := strings.TrimSpace(resolver); value != "" {
			cleaned = append(cleaned, value)
		}
	}
	return cleaned
}

package main

import (
	"context"
	"flag"
	"time"

	"cc-dns-axfr/internal/axfrscan"
)

func axfrScannerFlags(flags *flag.FlagSet) func() axfrscan.Config {
	maxDomains := flags.Int("max-domains", 0, "cap domains fetched by this scan cycle (0 = all)")
	statsInterval := flags.Duration("stats-interval", 5*time.Second, "periodic metrics interval (0 = off)")
	workers := flags.Int("workers", 50, "AXFR endpoints probed concurrently")
	perServerQPS := flags.Float64("per-server-qps", 5, "AXFR transfers/sec per nameserver IP")
	maxRecords := flags.Int("max-records", 50000, "records retained per AXFR transfer")
	maxBytes := flags.Int("max-bytes", 64<<20, "bytes retained per AXFR transfer")
	timeout := flags.Duration("timeout", 20*time.Second, "AXFR per-connection timeout")
	domainPageSize := flags.Int("domain-page-size", 1000, "root domains fetched per ClickHouse page")
	workCapacity := flags.Int("work-capacity", 5000, "active domains retained in AXFR SQLite")
	claimBatch := flags.Int("claim-batch", 100, "AXFR endpoints claimed per batch")
	flushBatch := flags.Int("flush-batch", 100, "ready domains flushed per batch")
	flushInterval := flags.Duration("flush-interval", 5*time.Second, "AXFR output retry interval")

	return func() axfrscan.Config {
		return axfrscan.Config{
			MaxDomains:     *maxDomains,
			StatsInterval:  *statsInterval,
			Workers:        *workers,
			PerServerQPS:   *perServerQPS,
			MaxRecords:     *maxRecords,
			MaxBytes:       *maxBytes,
			Timeout:        *timeout,
			DomainPageSize: *domainPageSize,
			WorkCapacity:   *workCapacity,
			ClaimBatch:     *claimBatch,
			FlushBatch:     *flushBatch,
			FlushInterval:  *flushInterval,
		}
	}
}

func runScan(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("scan", flag.ContinueOnError)
	scanID := flags.String("scan-id", time.Now().UTC().Format("2006-01-02"), "scan cycle id")
	databasePath := flags.String("db", "axfr-scan.db", "AXFR SQLite work/outbox path")
	buildConfig := axfrScannerFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}

	config := buildConfig()
	config.ScanID = *scanID
	return axfrscan.RunCycle(ctx, *databasePath, config)
}

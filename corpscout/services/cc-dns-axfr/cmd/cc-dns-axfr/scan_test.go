package main

import (
	"flag"
	"testing"
	"time"
)

func TestAXFRScannerFlagDefaults(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	buildConfig := axfrScannerFlags(flags)
	if err := flags.Parse(nil); err != nil {
		t.Fatal(err)
	}

	config := buildConfig()
	if config.MaxDomains != 0 || config.StatsInterval != 5*time.Second {
		t.Fatalf("cycle defaults = max domains %d, stats %s", config.MaxDomains, config.StatsInterval)
	}
	if config.Workers != 50 || config.PerServerQPS != 5 {
		t.Fatalf("probe defaults = workers %d, per-server QPS %f", config.Workers, config.PerServerQPS)
	}
	if config.MaxRecords != 50000 || config.MaxBytes != 64<<20 || config.Timeout != 20*time.Second {
		t.Fatalf("transfer defaults = records %d, bytes %d, timeout %s", config.MaxRecords, config.MaxBytes, config.Timeout)
	}
	if config.DomainPageSize != 1000 || config.WorkCapacity != 5000 || config.ClaimBatch != 100 {
		t.Fatalf("queue defaults = page %d, capacity %d, claim %d", config.DomainPageSize, config.WorkCapacity, config.ClaimBatch)
	}
	if config.FlushBatch != 100 || config.FlushInterval != 5*time.Second {
		t.Fatalf("flush defaults = batch %d, interval %s", config.FlushBatch, config.FlushInterval)
	}
}

func TestAXFRScannerFlagsMapOperatorValues(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	buildConfig := axfrScannerFlags(flags)
	args := []string{
		"--max-domains", "12",
		"--stats-interval", "0",
		"--workers", "500",
		"--per-server-qps", "7.5",
		"--max-records", "1234",
		"--max-bytes", "5678",
		"--timeout", "45s",
		"--domain-page-size", "222",
		"--work-capacity", "333",
		"--claim-batch", "444",
		"--flush-batch", "555",
		"--flush-interval", "6s",
	}
	if err := flags.Parse(args); err != nil {
		t.Fatal(err)
	}

	config := buildConfig()
	if config.MaxDomains != 12 || config.StatsInterval != 0 || config.Workers != 500 || config.PerServerQPS != 7.5 {
		t.Fatalf("cycle/probe config = %+v", config)
	}
	if config.MaxRecords != 1234 || config.MaxBytes != 5678 || config.Timeout != 45*time.Second {
		t.Fatalf("transfer config = %+v", config)
	}
	if config.DomainPageSize != 222 || config.WorkCapacity != 333 || config.ClaimBatch != 444 {
		t.Fatalf("queue config = %+v", config)
	}
	if config.FlushBatch != 555 || config.FlushInterval != 6*time.Second {
		t.Fatalf("flush config = %+v", config)
	}
}

func TestAXFRScannerExposesOnlyUnprefixedFlags(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	axfrScannerFlags(flags)

	for _, name := range []string{
		"max-domains", "stats-interval", "workers", "per-server-qps", "max-records", "max-bytes",
		"timeout", "domain-page-size", "work-capacity", "claim-batch", "flush-batch", "flush-interval",
	} {
		if flags.Lookup(name) == nil {
			t.Errorf("missing --%s", name)
		}
	}
	if flags.Lookup("axfr-workers") != nil || flags.Lookup("axfr-inflight") != nil {
		t.Fatal("standalone AXFR command must not expose legacy prefixed or in-flight flags")
	}
}

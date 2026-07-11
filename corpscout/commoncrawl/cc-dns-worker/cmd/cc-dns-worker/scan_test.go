package main

import (
	"context"
	"flag"
	"strings"
	"testing"
)

func TestScannerFlagsEnableIndependentScannersByDefault(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	build := scannerFlags(flags)
	if err := flags.Parse([]string{"--resolvers", "127.0.0.1:53"}); err != nil {
		t.Fatal(err)
	}
	config, err := build()
	if err != nil {
		t.Fatal(err)
	}
	if !config.RunDNS || !config.RunAXFR || !config.DNS.HostnameEnrichment {
		t.Fatalf("defaults = DNS:%t AXFR:%t hostnames:%t", config.RunDNS, config.RunAXFR, config.DNS.HostnameEnrichment)
	}
}

func TestAXFRCanRunWithoutDNSResolvers(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	build := scannerFlags(flags)
	if err := flags.Parse([]string{"--dns=false"}); err != nil {
		t.Fatal(err)
	}
	config, err := build()
	if err != nil {
		t.Fatal(err)
	}
	if config.RunDNS || !config.RunAXFR {
		t.Fatalf("scanner selection = DNS:%t AXFR:%t", config.RunDNS, config.RunAXFR)
	}
}

func TestDNSRequiresExplicitResolver(t *testing.T) {
	err := runScan(context.Background(), []string{"--dns-db", t.TempDir() + "/dns.db", "--axfr=false"})
	if err == nil || !strings.Contains(err.Error(), "--resolvers is required") {
		t.Fatalf("error = %v, want explicit resolver error", err)
	}
}

func TestScannerFlagsRejectBothDisabled(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	build := scannerFlags(flags)
	if err := flags.Parse([]string{"--dns=false", "--axfr=false"}); err != nil {
		t.Fatal(err)
	}
	if _, err := build(); err == nil {
		t.Fatal("both scanners disabled: want error")
	}
}

func TestCleanResolvers(t *testing.T) {
	got := cleanResolvers([]string{" 1.1.1.1:53 ", "", "   ", "8.8.8.8:53", "\t"})
	want := []string{"1.1.1.1:53", "8.8.8.8:53"}
	if len(got) != len(want) {
		t.Fatalf("cleanResolvers = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("cleanResolvers = %v, want %v", got, want)
		}
	}
}

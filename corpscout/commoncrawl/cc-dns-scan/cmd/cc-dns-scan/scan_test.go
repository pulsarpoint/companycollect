package main

import (
	"context"
	"flag"
	"strings"
	"testing"
	"time"
)

func TestDNSScannerFlagsDefaults(t *testing.T) {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	build := dnsScannerFlags(flags)
	if err := flags.Parse([]string{"--resolvers", "127.0.0.1:53"}); err != nil {
		t.Fatal(err)
	}
	config, err := build()
	if err != nil {
		t.Fatal(err)
	}
	if !config.HostnameEnrichment {
		t.Fatal("hostname enrichment is disabled by default")
	}
	if config.StatsInterval != 5*time.Second {
		t.Fatalf("stats interval = %s, want 5s", config.StatsInterval)
	}
}

func TestDNSRequiresExplicitResolver(t *testing.T) {
	err := runScan(context.Background(), []string{"--dns-db", t.TempDir() + "/dns.db"})
	if err == nil || !strings.Contains(err.Error(), "--resolvers is required") {
		t.Fatalf("error = %v, want explicit resolver error", err)
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

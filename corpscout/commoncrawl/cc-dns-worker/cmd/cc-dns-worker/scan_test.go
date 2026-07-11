package main

import (
	"context"
	"flag"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"

	"github.com/miekg/dns"
)

func TestScanFlagsEnableAXFRAndHostEnrichmentByDefault(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	build := scanFlags(fs)
	if err := fs.Parse([]string{"--resolvers", "127.0.0.1:53"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := build()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.axfr || !cfg.hostEnrich {
		t.Fatalf("default features: axfr=%t hostEnrich=%t, want both enabled", cfg.axfr, cfg.hostEnrich)
	}
}

func TestScanFlagsAllowExplicitFeatureOptOut(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	build := scanFlags(fs)
	if err := fs.Parse([]string{
		"--resolvers", "127.0.0.1:53",
		"--axfr=false",
		"--host-enrich=false",
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := build()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.axfr || cfg.hostEnrich {
		t.Fatalf("explicit opt-out: axfr=%t hostEnrich=%t, want both disabled", cfg.axfr, cfg.hostEnrich)
	}
}

func TestCleanResolvers(t *testing.T) {
	got := cleanResolvers([]string{" 1.1.1.1:53 ", "", "   ", "8.8.8.8:53", "\t"})
	want := []string{"1.1.1.1:53", "8.8.8.8:53"}
	if len(got) != len(want) {
		t.Fatalf("cleanResolvers = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("cleanResolvers = %v, want %v", got, want)
		}
	}
}

func TestCleanResolversAllBlank(t *testing.T) {
	got := cleanResolvers([]string{"", "  ", " "})
	if len(got) != 0 {
		t.Fatalf("cleanResolvers(all blank) = %v, want empty", got)
	}
}

// TestRunScanRejectsEmptyResolvers proves --resolvers being empty/whitespace-only after the comma
// split fails fast with a clear error instead of silently making every domain's discovery fail one
// at a time later. This must return before any ClickHouse/SQLite I/O is attempted, so the test
// needs no external services.
func TestRunScanRejectsEmptyResolvers(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scan.db")
	err := runScan([]string{"-resolvers= , ,  ", "-db", dbPath})
	if err == nil {
		t.Fatal("runScan with blank --resolvers: want error, got nil")
	}
	if !strings.Contains(err.Error(), "--resolvers is required") {
		t.Fatalf("runScan error = %q, want to contain %q", err.Error(), "--resolvers is required")
	}
}

// TestRunScanRequiresResolversByDefault proves there is NO public-resolver default: omitting
// --resolvers entirely fails fast (NS discovery must run against an explicitly-configured, ideally
// local, recursive resolver).
func TestRunScanRequiresResolversByDefault(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "scan.db")
	err := runScan([]string{"-db", dbPath}) // no -resolvers at all
	if err == nil || !strings.Contains(err.Error(), "--resolvers is required") {
		t.Fatalf("runScan without --resolvers: want '--resolvers is required' error, got %v", err)
	}
}

// TestHostnamesForBatchEmpty proves that a scan explicitly run with --host-enrich=false can read an
// empty hostname set without erroring or panicking, so the feeder's per-batch bulk-load remains a
// safe no-op.
func TestHostnamesForBatchEmpty(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "s.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m, err := st.HostnamesForBatch(context.Background(), "s1", []string{"example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Fatalf("want empty map, got %+v", m)
	}
}

type privateDelegationExchanger struct{}

func (privateDelegationExchanger) Exchange(_ context.Context, request *dns.Msg, _ string) (*dns.Msg, error) {
	response := new(dns.Msg)
	response.SetReply(request)
	question := request.Question[0]
	switch question.Qtype {
	case dns.TypeNS:
		response.Answer = []dns.RR{&dns.NS{
			Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeNS, Class: dns.ClassINET, Ttl: 300},
			Ns:  "ns1.example.com.",
		}}
	case dns.TypeA:
		response.Answer = []dns.RR{&dns.A{
			Hdr: dns.RR_Header{Name: question.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 300},
			A:   []byte{10, 0, 0, 53},
		}}
	}
	return response, nil
}

func TestResolveDomainPersistsPrivateOnlyDelegationAsDistinctTerminalStatus(t *testing.T) {
	discoverer := resolve.NewDiscoverer(privateDelegationExchanger{}, []string{"operator-resolver"})
	result := resolveDomain(
		context.Background(), discoverer, nil, records.DefaultConfig(),
		"example.com", "scan-1", "run-1", nil,
	)

	if result.Status != model.DomainStatusNoPublicNSEndpoints {
		t.Fatalf("status = %q, want %q", result.Status, model.DomainStatusNoPublicNSEndpoints)
	}
	if len(result.Endpoints) != 1 || result.Endpoints[0].IP != "10.0.0.53" || result.Endpoints[0].Dialable {
		t.Fatalf("private delegation evidence was not preserved: %+v", result.Endpoints)
	}
	if result.ResolvedAt.IsZero() || time.Since(result.ResolvedAt) > time.Minute {
		t.Fatalf("unexpected resolution timestamp: %v", result.ResolvedAt)
	}
}

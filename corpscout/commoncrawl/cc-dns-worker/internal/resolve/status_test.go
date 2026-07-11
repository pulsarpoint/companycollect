package resolve

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cc-dns-worker/internal/metrics"
	"cc-dns-worker/internal/records"
	"cc-dns-worker/internal/scheduler"

	"github.com/miekg/dns"
)

// alwaysFailEx is an Exchanger that fails every single call (a stand-in for a domain whose
// authoritative servers never answer at all — every Tier-2 attempt exhausts as a transport failure).
type alwaysFailEx struct{}

func (alwaysFailEx) Exchange(_ context.Context, _ *dns.Msg, _ string) (*dns.Msg, error) {
	return nil, fmt.Errorf("simulated transport failure")
}

// TestResolveAllTier2QueriesFailIsNotDone covers bug (1): when every Tier-2 query exhausts without a
// definitive answer, Resolve must NOT report Status="done" (which would let a bad scan overwrite a
// domain's last-good summary) — see model.DomainResult's Status doc comment for the bar. Resolve never
// returns "error" (that status is reserved for a discovery failure, set by the caller), so the only
// other option is "partial" — which still must not silently look like success.
func TestResolveAllTier2QueriesFailIsNotDone(t *testing.T) {
	r := &Resolver{Ex: alwaysFailEx{}}
	del := Delegation{
		ETLD: "com", NS: []string{"ns1.example.com."},
		NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"},
		DSOutcome: OutcomePresent, // isolate the Tier-2 failure: pretend Tier-1 DS discovery succeeded
	}

	res := r.Resolve(context.Background(), "example.com", "sc", "run", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	if res.Status == "done" {
		t.Fatalf("Status = %q, want NOT done when every Tier-2 query exhausted", res.Status)
	}
	if res.Status != "partial" {
		t.Errorf("Status = %q, want %q (Resolve never returns \"error\")", res.Status, "partial")
	}
	if res.QueriesOK != 0 {
		t.Errorf("QueriesOK = %d, want 0 (every attempt failed)", res.QueriesOK)
	}
	if res.DNSKEYOutcome != OutcomeUnknown {
		t.Errorf("DNSKEYOutcome = %q, want %q", res.DNSKEYOutcome, OutcomeUnknown)
	}
}

// failOnTypeEx answers normally from a zone map for every query type except failType, which it always
// fails — used to isolate a single failing query (e.g. DNSKEY) from an otherwise-healthy scan.
type failOnTypeEx struct {
	z        map[string][]dns.RR
	failType uint16
}

func (f failOnTypeEx) Exchange(_ context.Context, m *dns.Msg, _ string) (*dns.Msg, error) {
	q := m.Question[0]
	if q.Qtype == f.failType {
		return nil, fmt.Errorf("simulated timeout on %s", dns.TypeToString[q.Qtype])
	}
	r := new(dns.Msg)
	r.SetReply(m)
	r.Answer = append(r.Answer, f.z[q.Name+"/"+dns.TypeToString[q.Qtype]]...)
	return r, nil
}

// TestResolveDNSKEYTimeoutMarksOutcomeUnknown covers bug (2): a DNSKEY query that never gets a
// definitive answer must land on DNSKEYOutcome="unknown", not be silently folded into
// DNSSECSigned=false as if the zone were definitively unsigned. Because DNSKEYOutcome is one of the
// fields the "done" bar depends on, the whole result must also downgrade to "partial" — which is
// exactly what keeps store.CommitBatch from ever writing this scan's false DNSSECSigned over a prior
// "done" scan's true value (see model.ScanRow / store.StagedDomains, gated on status='done').
func TestResolveDNSKEYTimeoutMarksOutcomeUnknown(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	}
	r := &Resolver{Ex: failOnTypeEx{z: z, failType: dns.TypeDNSKEY}}
	del := Delegation{
		ETLD: "com", NS: []string{"ns1.example.com."},
		NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"},
		DSOutcome: OutcomePresent,
	}

	res := r.Resolve(context.Background(), "example.com", "sc", "run", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	if res.DNSKEYOutcome != OutcomeUnknown {
		t.Fatalf("DNSKEYOutcome = %q, want %q", res.DNSKEYOutcome, OutcomeUnknown)
	}
	if res.DNSSECSigned {
		t.Errorf("DNSSECSigned = true, want false (this scan never proved anything about signing)")
	}
	if res.Status != "partial" {
		t.Errorf("Status = %q, want %q (DNSKEY outcome is unknown, so the done-bar is not met)", res.Status, "partial")
	}
}

// TestResolveDNSKEYPresentIsDoneWhenApexAnswered proves the flip side of the previous test: when the
// DNSKEY query DOES get a definitive answer (present, here) alongside a definitive apex A/AAAA and a
// known DS outcome, Resolve reports "done" — the bar is met, not artificially strict.
func TestResolveDNSKEYPresentIsDoneWhenApexAnswered(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./A":      {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
		"example.com./DNSKEY": {mustRR(t, "example.com. 300 IN DNSKEY 257 3 8 AwEAAaz/")},
	}
	r := &Resolver{Ex: stubEx{z: z}}
	del := Delegation{
		ETLD: "com", NS: []string{"ns1.example.com."},
		NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"},
		DSOutcome: OutcomePresent,
	}

	res := r.Resolve(context.Background(), "example.com", "sc", "run", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	if res.Status != "done" {
		t.Fatalf("Status = %q, want %q", res.Status, "done")
	}
	if res.DNSKEYOutcome != OutcomePresent || !res.DNSSECSigned {
		t.Errorf("DNSKEYOutcome/DNSSECSigned = %q/%v, want present/true", res.DNSKEYOutcome, res.DNSSECSigned)
	}
}

// TestResolveDSTimeoutIsNotDoneDistinctFromAbsent covers requirement (3)'s other half at the Resolve
// level: a Delegation whose DSOutcome is "unknown" (the Tier-1 DS query failed) must keep the result
// out of "done", distinguishably from a Delegation whose DSOutcome is definitively "absent" (a clean
// NODATA/NXDOMAIN), which — with everything else healthy — DOES reach "done".
func TestResolveDSTimeoutIsNotDoneDistinctFromAbsent(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	}
	newDel := func(dsOutcome string) Delegation {
		return Delegation{
			ETLD: "com", NS: []string{"ns1.example.com."},
			NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"},
			DSOutcome: dsOutcome,
		}
	}

	unknown := (&Resolver{Ex: stubEx{z: z}}).Resolve(context.Background(), "example.com", "sc", "run", newDel(OutcomeUnknown), records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)
	if unknown.Status == "done" {
		t.Errorf("Status = %q with DSOutcome=unknown, want NOT done", unknown.Status)
	}
	if unknown.DSPresent {
		t.Errorf("DSPresent = true with DSOutcome=unknown, want false (never forced true)")
	}

	absent := (&Resolver{Ex: stubEx{z: z}}).Resolve(context.Background(), "example.com", "sc", "run", newDel(OutcomeAbsent), records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)
	if absent.Status != "done" {
		t.Errorf("Status = %q with DSOutcome=absent (definitive), want %q", absent.Status, "done")
	}
	if absent.DSPresent {
		t.Errorf("DSPresent = true with DSOutcome=absent, want false")
	}
	if absent.DSOutcome != OutcomeAbsent {
		t.Errorf("DSOutcome = %q, want %q", absent.DSOutcome, OutcomeAbsent)
	}
}

// TestResolveFlagsPrivateAddressFromPublicServer covers requirement (4): a public authoritative server
// answering the apex A query with a private (RFC1918) address must have that value STORED verbatim
// (never dropped) and FLAGGED via DNSRecord.Finding, distinguishing it from an ordinary public answer.
// recordingEx additionally proves the private VALUE is never itself used as a dial target — Resolve
// only ever dials the server addresses in DialableNSIPs, never a record's rdata.
func TestResolveFlagsPrivateAddressFromPublicServer(t *testing.T) {
	z := map[string][]dns.RR{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 10.20.30.40")},
	}
	rec := &recordingEx{z: z}
	r := NewResolver(rec)
	del := Delegation{
		ETLD: "com", NS: []string{"ns1.example.com."},
		NSIPs: []string{"9.9.9.9"}, DialableNSIPs: []string{"9.9.9.9"}, // the SERVER is public
	}

	res := r.Resolve(context.Background(), "example.com", "sc", "run", del, records.DefaultConfig(), time.Unix(0, 0).UTC(), nil)

	var found bool
	for _, rr := range res.Records {
		if rr.RecordType == "A" && rr.Slot == "@" && rr.Value == "10.20.30.40" {
			found = true
			if rr.Finding != "public_dns_private_address" {
				t.Errorf("Finding = %q, want %q", rr.Finding, "public_dns_private_address")
			}
		}
	}
	if !found {
		t.Fatalf("private apex A record was not stored; records=%+v", res.Records)
	}

	rec.mu.Lock()
	dialed := append([]string(nil), rec.dialed...)
	rec.mu.Unlock()
	for _, d := range dialed {
		if d == "10.20.30.40" {
			t.Fatalf("the returned record VALUE must never itself be used as a dial target; dialed=%v", dialed)
		}
	}
}

// TestAddressFindingOnlyFlagsNonPublic proves addressFinding is a pure classification: a public
// address never gets flagged, and every non-public scope (not just private) does.
func TestAddressFindingOnlyFlagsNonPublic(t *testing.T) {
	cases := []struct {
		value string
		want  string
	}{
		{"93.184.216.34", ""},                         // public
		{"10.20.30.40", "public_dns_private_address"}, // private
		{"127.0.0.1", "public_dns_private_address"},   // loopback
		{"169.254.1.1", "public_dns_private_address"}, // link-local
		{"100.64.0.1", "public_dns_private_address"},  // CGNAT
		{"not-an-ip", ""},                             // not a parseable address: no finding
	}
	for _, c := range cases {
		if got := addressFinding(c.value); got != c.want {
			t.Errorf("addressFinding(%q) = %q, want %q", c.value, got, c.want)
		}
	}
}

// TestExchangeServfailCountsAsQueryError covers requirement (5) at the lowest level: a single SERVFAIL
// response is not a Go transport error, but exchange.go must still count it in Stats.QueryErrors,
// paired with the Stats.Queries increment for the same attempt.
func TestExchangeServfailCountsAsQueryError(t *testing.T) {
	addr, stop := startServfail(t)
	defer stop()

	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	var stats metrics.Stats
	ex := NewExchangerWithStats(sched, 2*time.Second, &stats)

	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	resp, err := ex.Exchange(context.Background(), m, addr)
	if err != nil {
		t.Fatalf("exchange: %v (a SERVFAIL response is not itself a Go error)", err)
	}
	if resp == nil || resp.Rcode != dns.RcodeServerFailure {
		t.Fatalf("resp = %+v, want a SERVFAIL response", resp)
	}
	if got := stats.Queries.Load(); got != 1 {
		t.Errorf("Queries = %d, want 1", got)
	}
	if got := stats.QueryErrors.Load(); got != 1 {
		t.Errorf("QueryErrors = %d, want 1 (SERVFAIL must count as a query error)", got)
	}
}

func TestExchangeValidNXDOMAINDoesNotCountAsQueryError(t *testing.T) {
	addr, stop := startRcode(t, dns.RcodeNameError)
	defer stop()
	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	var stats metrics.Stats
	ex := NewExchangerWithStats(sched, time.Second, &stats)
	m := new(dns.Msg)
	m.SetQuestion("missing.example.", dns.TypeA)
	if _, err := ex.Exchange(context.Background(), m, addr); err != nil {
		t.Fatal(err)
	}
	if stats.Queries.Load() != 1 || stats.QueryErrors.Load() != 0 {
		t.Fatalf("valid NXDOMAIN counters: queries=%d errors=%d", stats.Queries.Load(), stats.QueryErrors.Load())
	}
}

func TestExchangeRefusedCountsAsQueryError(t *testing.T) {
	addr, stop := startRcode(t, dns.RcodeRefused)
	defer stop()
	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	var stats metrics.Stats
	ex := NewExchangerWithStats(sched, time.Second, &stats)
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	if _, err := ex.Exchange(context.Background(), m, addr); err != nil {
		t.Fatal(err)
	}
	if stats.QueryErrors.Load() != 1 {
		t.Fatalf("REFUSED errors=%d, want 1", stats.QueryErrors.Load())
	}
}

func TestExchangeTimeoutCountsAsErrorAndTimeout(t *testing.T) {
	addr, stop := startNoReply(t)
	defer stop()
	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	var stats metrics.Stats
	ex := NewExchangerWithStats(sched, 50*time.Millisecond, &stats)
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	if _, err := ex.Exchange(context.Background(), m, addr); err == nil {
		t.Fatal("expected timeout")
	}
	if stats.Queries.Load() != 1 || stats.QueryErrors.Load() != 1 || stats.QueryTimeouts.Load() != 1 {
		t.Fatalf("timeout counters: queries=%d errors=%d timeouts=%d",
			stats.Queries.Load(), stats.QueryErrors.Load(), stats.QueryTimeouts.Load())
	}
}

// TestDiscoverNSExhaustedServfailCountsQueryErrorsConsistently exercises requirement (5) through a
// real exhausted retry sequence (Discoverer.query's rotate-and-retry loop, the same shared machinery
// Resolver.queryAuth uses): every attempt against an always-SERVFAIL resolver must count once in both
// Stats.Queries and Stats.QueryErrors — no double-count, no silent drop.
func TestDiscoverNSExhaustedServfailCountsQueryErrorsConsistently(t *testing.T) {
	addr, stop := startServfail(t)
	defer stop()

	sched := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	var stats metrics.Stats
	ex := NewExchangerWithStats(sched, 2*time.Second, &stats)
	d := NewDiscoverer(ex, []string{addr})

	if _, err := d.DiscoverNS(context.Background(), "example.com"); err == nil {
		t.Fatal("expected discovery to fail against an always-SERVFAIL resolver")
	}

	// One resolver, 2 attempts per Discoverer.query; DiscoverNS gives up on "no NS records" as soon as
	// the NS query itself comes back empty, so exactly one query() call (2 attempts) happens here.
	if got := stats.Queries.Load(); got != 2 {
		t.Errorf("Queries = %d, want 2", got)
	}
	if got := stats.QueryErrors.Load(); got != 2 {
		t.Errorf("QueryErrors = %d, want 2 — every exhausted SERVFAIL attempt must count, none silently dropped", got)
	}
}

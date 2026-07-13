package resolve

import (
	"context"
	"net"
	"testing"
	"time"

	"cc-dns-scan/internal/scheduler"

	"github.com/miekg/dns"
)

func newTestExchanger() Exchanger {
	s := scheduler.New(scheduler.Config{PerServerQPS: 1000, Burst: 1000, MaxInFlight: 10})
	return NewExchanger(s, 2*time.Second)
}

func TestExchangeRoundTrip(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	})
	defer stop()

	ex := newTestExchanger()
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	resp, err := ex.Exchange(context.Background(), m, addr)
	if err != nil {
		t.Fatalf("exchange: %v", err)
	}
	if len(resp.Answer) != 1 {
		t.Fatalf("answers = %d, want 1", len(resp.Answer))
	}
	if a, ok := resp.Answer[0].(*dns.A); !ok || a.A.String() != "93.184.216.34" {
		t.Errorf("bad answer %v", resp.Answer[0])
	}
}

// DiscoverNS against a stand-in recursive resolver: NS names, their IPs (including a cross-TLD NS
// the recursive resolver resolves for us), and the parent DS.
func TestDiscoverNS(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
			mustRR(t, "example.com. 300 IN NS ns2.example.net."), // cross-TLD, no glue needed
		},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 1.1.1.1")},
		"ns2.example.net./A": {mustRR(t, "ns2.example.net. 300 IN A 2.2.2.2")},
		"example.com./DS":    {mustRR(t, "example.com. 3600 IN DS 12345 13 2 E2D3C916F6DEEAC73294E8268FB5885044A833FC5459588F4A9184CFC41A5766")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if del.ETLD != "com" {
		t.Errorf("etld = %q, want com", del.ETLD)
	}
	if len(del.NS) != 2 {
		t.Errorf("NS = %v, want 2", del.NS)
	}
	hasIP := func(ip string) bool {
		for _, x := range del.NSIPs {
			if x == ip {
				return true
			}
		}
		return false
	}
	if !hasIP("1.1.1.1") || !hasIP("2.2.2.2") {
		t.Errorf("NSIPs = %v, want both 1.1.1.1 and 2.2.2.2", del.NSIPs)
	}
	if len(del.DS) == 0 {
		t.Errorf("expected DS captured")
	}
	if del.DSOutcome != OutcomePresent {
		t.Errorf("DSOutcome = %q, want %q", del.DSOutcome, OutcomePresent)
	}
}

// TestDiscoverNSDSAbsentIsDefinitive covers requirement (3): a domain whose parent zone has no DS
// record must get a DEFINITIVE "absent" outcome (a clean NOERROR/NODATA), not be indistinguishable
// from a DS query that simply failed.
func TestDiscoverNSDSAbsentIsDefinitive(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS":    {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 1.1.1.1")},
		// deliberately no example.com./DS entry: the stub answers NOERROR/NODATA for it.
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if del.DSOutcome != OutcomeAbsent {
		t.Errorf("DSOutcome = %q, want %q", del.DSOutcome, OutcomeAbsent)
	}
	if len(del.DS) != 0 {
		t.Errorf("DS = %v, want empty", del.DS)
	}
}

// startAuthDSFails behaves like startAuth (zone-driven answers) for every query EXCEPT dns.TypeDS,
// which it always answers SERVFAIL — used to exercise a Tier-1 DS query that fails while NS/A
// discovery succeeds normally, so DiscoverNS's DSOutcome must land on OutcomeUnknown rather than being
// silently indistinguishable from a definitive absence (TestDiscoverNSDSAbsentIsDefinitive).
func startAuthDSFails(t *testing.T, z zone) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		if q.Qtype == dns.TypeDS {
			m.Rcode = dns.RcodeServerFailure
		} else if rrs, ok := z[q.Name+"/"+dns.TypeToString[q.Qtype]]; ok {
			m.Answer = append(m.Answer, rrs...)
		}
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

// TestDiscoverNSDSOutcomeUnknownOnQueryFailure covers requirement (3)'s failure side: a DS query that
// exhausts (here, always SERVFAIL) while NS/A discovery succeeds normally must leave DSOutcome
// "unknown" and DS empty — never silently treated as a definitive absence.
func TestDiscoverNSDSOutcomeUnknownOnQueryFailure(t *testing.T) {
	addr, stop := startAuthDSFails(t, zone{
		"example.com./NS":    {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 5.5.5.5")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if del.DSOutcome != OutcomeUnknown {
		t.Errorf("DSOutcome = %q, want %q", del.DSOutcome, OutcomeUnknown)
	}
	if len(del.DS) != 0 {
		t.Errorf("DS = %v, want empty when outcome is unknown", del.DS)
	}
}

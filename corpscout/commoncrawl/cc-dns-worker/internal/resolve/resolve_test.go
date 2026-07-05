package resolve

import (
	"context"
	"testing"
	"time"

	"cc-dns-worker/internal/scheduler"

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
}

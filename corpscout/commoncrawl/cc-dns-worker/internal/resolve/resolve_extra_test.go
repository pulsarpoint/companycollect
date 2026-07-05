package resolve

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/miekg/dns"
)

// startAuthCapture is a variant of startAuth that additionally invokes onReq with every incoming
// request before the zone-driven reply is built, so tests can inspect what the client actually sent
// (e.g. EDNS0/DO) rather than just what came back.
func startAuthCapture(t *testing.T, z zone, onReq func(*dns.Msg)) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		if onReq != nil {
			onReq(r)
		}
		m := new(dns.Msg)
		m.SetReply(r)
		q := r.Question[0]
		if rrs, ok := z[q.Name+"/"+dns.TypeToString[q.Qtype]]; ok {
			m.Answer = append(m.Answer, rrs...)
		}
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

// startServfail starts an in-process server that answers every query with SERVFAIL, regardless of
// question name/type, to exercise the retryable-error/rotation path in Discoverer.query.
func startServfail(t *testing.T) (string, func()) {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	mux := dns.NewServeMux()
	mux.HandleFunc(".", func(w dns.ResponseWriter, r *dns.Msg) {
		m := new(dns.Msg)
		m.SetReply(r)
		m.Rcode = dns.RcodeServerFailure
		_ = w.WriteMsg(m)
	})
	srv := &dns.Server{PacketConn: pc, Handler: mux}
	go func() { _ = srv.ActivateAndServe() }()
	return pc.LocalAddr().String(), func() { _ = srv.Shutdown() }
}

// TestExchangeSetsEDNSAndDO verifies every outgoing query carries an EDNS0 OPT record with the DO
// (DNSSEC OK) bit set, per exchange.go's m.SetEdns0(1232, true) on every Exchange call.
func TestExchangeSetsEDNSAndDO(t *testing.T) {
	var (
		mu       sync.Mutex
		captured *dns.Msg
	)
	addr, stop := startAuthCapture(t, zone{
		"example.com./A": {mustRR(t, "example.com. 300 IN A 93.184.216.34")},
	}, func(r *dns.Msg) {
		mu.Lock()
		defer mu.Unlock()
		captured = r.Copy()
	})
	defer stop()

	ex := newTestExchanger()
	m := new(dns.Msg)
	m.SetQuestion("example.com.", dns.TypeA)
	if _, err := ex.Exchange(context.Background(), m, addr); err != nil {
		t.Fatalf("exchange: %v", err)
	}

	mu.Lock()
	req := captured
	mu.Unlock()
	if req == nil {
		t.Fatal("server never captured a request")
	}
	opt := req.IsEdns0()
	if opt == nil {
		t.Fatal("request has no EDNS0 OPT record")
	}
	if !opt.Do() {
		t.Error("EDNS0 OPT present but DO bit not set")
	}
}

// TestDiscoverNSNoNS covers the negative path where the resolver has no NS records at all for the
// domain: DiscoverNS must return a non-nil "no NS records" error and leave Delegation.NS empty.
func TestDiscoverNSNoNS(t *testing.T) {
	addr, stop := startAuth(t, zone{})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no NS records") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "no NS records")
	}
	if len(del.NS) != 0 {
		t.Errorf("NS = %v, want empty", del.NS)
	}
}

// TestDiscoverNSNoNSIPs covers the negative path where NS records resolve but none of the NS
// hostnames have any A/AAAA: DiscoverNS must return a non-nil "no NS IPs resolved" error, with NS
// populated but NSIPs empty.
func TestDiscoverNSNoNSIPs(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS": {
			mustRR(t, "example.com. 300 IN NS ns1.example.com."),
		},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "no NS IPs resolved") {
		t.Errorf("error = %q, want to contain %q", err.Error(), "no NS IPs resolved")
	}
	if len(del.NS) == 0 {
		t.Errorf("NS = %v, want populated", del.NS)
	}
	if len(del.NSIPs) != 0 {
		t.Errorf("NSIPs = %v, want empty", del.NSIPs)
	}
}

// TestDiscoverNSRotatesOnServfail proves Discoverer.query treats SERVFAIL as retryable: the first
// resolver always answers SERVFAIL, the second is a healthy zone, and discovery must still succeed
// by rotating past the failing resolver.
func TestDiscoverNSRotatesOnServfail(t *testing.T) {
	servfailAddr, stopServfail := startServfail(t)
	defer stopServfail()

	healthyAddr, stopHealthy := startAuth(t, zone{
		"example.com./NS":    {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./A": {mustRR(t, "ns1.example.com. 300 IN A 5.5.5.5")},
	})
	defer stopHealthy()

	d := NewDiscoverer(newTestExchanger(), []string{servfailAddr, healthyAddr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(del.NS) == 0 {
		t.Errorf("NS = %v, want populated", del.NS)
	}
	if len(del.NSIPs) == 0 {
		t.Errorf("NSIPs = %v, want populated", del.NSIPs)
	}
}

// TestDiscoverNSAAAA covers the *dns.AAAA branch in discover.go's NS-IP resolution loop: an NS
// hostname with only an AAAA record must still contribute its IPv6 address to Delegation.NSIPs.
func TestDiscoverNSAAAA(t *testing.T) {
	addr, stop := startAuth(t, zone{
		"example.com./NS":       {mustRR(t, "example.com. 300 IN NS ns1.example.com.")},
		"ns1.example.com./AAAA": {mustRR(t, "ns1.example.com. 300 IN AAAA 2001:db8::1")},
	})
	defer stop()

	d := NewDiscoverer(newTestExchanger(), []string{addr})
	del, err := d.DiscoverNS(context.Background(), "example.com")
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	hasIP := func(ip string) bool {
		for _, x := range del.NSIPs {
			if x == ip {
				return true
			}
		}
		return false
	}
	if !hasIP("2001:db8::1") {
		t.Errorf("NSIPs = %v, want to include 2001:db8::1", del.NSIPs)
	}
}
